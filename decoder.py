"""
V3 Polymarket market-channel codec (MAX COMPRESSION) + tolerant reinflater.

What's new vs earlier:
- **Stateful V3** stream with a tiny header and per-frame records.
- **Dictionary pooling** of strings across the session.
- **Timestamp delta encoding** per event; session base timestamp stored in header.
- **Single-asset mode**: asset_id lives in the header; omitted from events.
- **Raw DEFLATE** per line (no zlib header/trailer) -> base64-url.

We still:
- Drop 'market' and 'hash' (by design).
- Reconstruct original shapes: arrays of events, or a JSON string like "PONG".
- Provide a tolerant `reinflate_file` that can also read older files:
  * previous compact (zlib+base64 JSON arrays),
  * JSONL wrappers with {"m": ...} or {"c": "..."},
  * escaped JSON strings.

Public surface:
- class FrameCompressorV3(asset_id: str)
    .compress(raw_frame_str: str) -> str | [str, str]  # may include a header line
- function reinflate_file(input_path: str, output_path: str, ndjson: bool = True) -> None
    Reads a mixed/unknown log and writes NDJSON (or a single JSON array with --array).

CLI:
  python decoder.py --in updates.compact --out updates.ndjson
  python decoder.py --in updates.compact --out updates.json --array
"""
from __future__ import annotations

import argparse
import base64
import json
import zlib
from typing import Any, Dict, Iterable, List, Tuple, Union, Optional

# ---------- Varint helpers (LEB128) ----------

def _uvarint_encode(n: int, out: bytearray) -> None:
    """Unsigned LEB128."""
    if n < 0:
        raise ValueError("uvarint negative")
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n & 0x7F)

def _uvarint_decode(buf: bytes, i: int) -> Tuple[int, int]:
    """Return (value, new_index)."""
    shift = 0
    x = 0
    while True:
        if i >= len(buf):
            raise ValueError("uvarint truncated")
        b = buf[i]
        i += 1
        x |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return x, i
        shift += 7
        if shift > 70:
            raise ValueError("uvarint overflow")

# ---------- Tiny raw-DEFLATE base64 codecs ----------

def _deflate_raw_b64(data: bytes) -> str:
    co = zlib.compressobj(level=9, wbits=-15)  # raw DEFLATE
    comp = co.compress(data) + co.flush()
    return base64.urlsafe_b64encode(comp).decode("ascii")

def _inflate_raw_b64(token: str) -> bytes:
    data = base64.urlsafe_b64decode(token.encode("ascii"))
    return zlib.decompress(data, wbits=-15)

# ---------- Event + field coding ----------

_ET_CODE = {"book": 0, "price_change": 1, "tick_size_change": 2, "last_trade_price": 3}
_ET_FROM = {v: k for k, v in _ET_CODE.items()}

# type byte layout:
# bits 0..2 = event_type (0..7)
# bit 3      = optional present for LTP fee_rate_bps
# bit 5      = ts_is_absolute (0=delta, 1=absolute)
# bits 4,6,7 reserved
_TB_TS_ABS = 1 << 5
_TB_OPT0   = 1 << 3  # used by LTP fee_rate_bps

# record kinds
_REC_HEADER = 0x48  # 'H'
_REC_FRAME  = 0x46  # 'F'
_REC_RAW    = 0x58  # 'X'

# header flags
_H_SINGLE_ASSET = 1 << 0


class _StringPool:
    """Session string pool: assigns small IDs to first-seen strings."""
    __slots__ = ("_s2i", "_i2s", "_next")

    def __init__(self):
        self._s2i: Dict[str, int] = {}
        self._i2s: List[str] = ["<unused>"]  # 1-based
        self._next: int = 1

    def encode(self, s: str, out: bytearray) -> None:
        """
        Write either:
          - ref:   (id << 1) | 0
          - lit:   (len << 1) | 1  then raw bytes
        """
        i = self._s2i.get(s)
        if i is not None:
            _uvarint_encode((i << 1) | 0, out)
            return
        # new literal
        b = s.encode("utf-8")
        _uvarint_encode((len(b) << 1) | 1, out)
        out += b
        # add to pool
        self._s2i[s] = self._next
        self._i2s.append(s)
        self._next += 1

    def decode(self, buf: bytes, i: int) -> Tuple[str, int]:
        v, i = _uvarint_decode(buf, i)
        if (v & 1) == 0:
            idx = v >> 1
            if idx <= 0 or idx >= self._next:
                raise ValueError("bad string ref")
            return self._i2s[idx], i
        # literal
        ln = v >> 1
        if i + ln > len(buf):
            raise ValueError("literal overflow")
        s = buf[i:i+ln].decode("utf-8")
        i += ln
        self._s2i[s] = self._next
        self._i2s.append(s)
        self._next += 1
        return s, i

    def reset(self):
        self.__init__()


# ---------- V3 Compressor (stateful) ----------

class FrameCompressorV3:
    """
    Stateful V3 compressor:
      - writes a header on the first frame (asset_id + base_ts + flags)
      - delta-encodes timestamps
      - pools strings across frames
      - outputs raw-DEFLATE+base64 lines

    .compress(raw_frame_str) -> str | [str, str]
      May return [header_line, frame_line] the first time.
    """
    __slots__ = ("asset_id", "pool", "base_ts", "prev_ts", "wrote_header")

    def __init__(self, asset_id: str):
        self.asset_id = asset_id
        self.pool = _StringPool()
        self.base_ts: Optional[int] = None
        self.prev_ts: Optional[int] = None
        self.wrote_header = False

    # --- event minifiers into binary ---

    @staticmethod
    def _get_ts(ev: Dict[str, Any]) -> Optional[int]:
        ts = ev.get("timestamp")
        if ts is None:
            return None
        # timestamps are strings in Polymarket; keep exact numeric value
        try:
            return int(ts)
        except Exception:
            # non-numeric; fall back to None
            return None

    def _encode_levels(self, levels: Iterable[Dict[str, str]], out: bytearray) -> None:
        # levels are [{"price": "...", "size": "..."}, ...]
        # We write count then (price,size)* using string pool
        lv = list(levels)
        _uvarint_encode(len(lv), out)
        for lvx in lv:
            self.pool.encode(str(lvx.get("price", "")), out)
            self.pool.encode(str(lvx.get("size", "")), out)

    def _encode_book(self, ev: Dict[str, Any], out: bytearray) -> None:
        # Accept bids/asks or buys/sells
        if "bids" in ev:
            bids = ev.get("bids", [])
        else:
            bids = ev.get("buys", [])
        if "asks" in ev:
            asks = ev.get("asks", [])
        else:
            asks = ev.get("sells", [])
        self._encode_levels(bids, out)
        self._encode_levels(asks, out)

    def _encode_price_change(self, ev: Dict[str, Any], out: bytearray) -> None:
        chs = ev.get("changes", []) or []
        _uvarint_encode(len(chs), out)
        for ch in chs:
            side = str(ch.get("side", "")).upper()
            out.append(1 if side == "SELL" else 0)  # 1 byte
            self.pool.encode(str(ch.get("price", "")), out)
            self.pool.encode(str(ch.get("size", "")), out)

    def _encode_tick_size_change(self, ev: Dict[str, Any], out: bytearray) -> None:
        self.pool.encode(str(ev.get("old_tick_size", "")), out)
        self.pool.encode(str(ev.get("new_tick_size", "")), out)

    def _encode_last_trade_price(self, ev: Dict[str, Any], type_byte: int, out: bytearray) -> None:
        self.pool.encode(str(ev.get("price", "")), out)
        self.pool.encode(str(ev.get("size", "")), out)
        side = str(ev.get("side", "")).upper()
        out.append(1 if side == "SELL" else 0)
        if (type_byte & _TB_OPT0) != 0:
            self.pool.encode(str(ev.get("fee_rate_bps", "")), out)

    # --- frame builders ---

    def _ensure_header(self, first_ts: Optional[int]) -> Optional[str]:
        if self.wrote_header:
            return None
        # choose base timestamp as the first seen timestamp (or 0 if unknown)
        self.base_ts = int(first_ts) if first_ts is not None else 0
        self.prev_ts = self.base_ts
        flags = _H_SINGLE_ASSET
        # build header: 'H' | version=3 | flags | base_ts | asset_count=1 | asset_id
        out = bytearray()
        out.append(_REC_HEADER)
        _uvarint_encode(3, out)               # version
        _uvarint_encode(flags, out)           # flags
        _uvarint_encode(self.base_ts, out)    # base timestamp (absolute)
        _uvarint_encode(1, out)               # asset_count
        aid_b = self.asset_id.encode("utf-8")
        _uvarint_encode(len(aid_b), out)
        out += aid_b
        self.wrote_header = True
        # reset pool at header time (fresh session)
        self.pool.reset()
        return _deflate_raw_b64(bytes(out))

    def _encode_event(self, ev: Dict[str, Any], out: bytearray) -> None:
        et = str(ev.get("event_type", ""))
        et_code = _ET_CODE.get(et)
        if et_code is None:
            # Unknown dict: fall back to JSON literal payload (rare)
            # format: treat as raw frame with a JSON string; decoder will parse JSON
            payload = json.dumps(ev, separators=(",", ":"))
            # write as RAW record embedded inside a frame: et=7 reserved style
            # simpler: emit as a RAW top-level frame externally (one line)
            # But since we're inside the frame builder, encode as last_trade_price with no fields is messy.
            # Instead, raise and upstream will store raw JSON.
            raise ValueError(f"Unknown event_type: {et}")

        # timestamp delta
        ts = self._get_ts(ev)
        ts_abs = False
        if self.prev_ts is None or ts is None:
            # absolute if we don't know previous or timestamp missing
            ts_val = 0 if ts is None else ts
            ts_abs = True
            self.prev_ts = ts if ts is not None else self.prev_ts
        else:
            if ts >= self.prev_ts:
                ts_val = ts - self.prev_ts
                ts_abs = False
            else:
                ts_val = ts
                ts_abs = True
            self.prev_ts = ts

        # build type byte
        tb = et_code & 0x07
        if ts_abs:
            tb |= _TB_TS_ABS
        if et_code == _ET_CODE["last_trade_price"] and ("fee_rate_bps" in ev):
            tb |= _TB_OPT0

        out.append(tb)
        _uvarint_encode(ts_val, out)

        # payload
        if et_code == 0:
            self._encode_book(ev, out)
        elif et_code == 1:
            self._encode_price_change(ev, out)
        elif et_code == 2:
            self._encode_tick_size_change(ev, out)
        elif et_code == 3:
            self._encode_last_trade_price(ev, tb, out)

    def _encode_frame_events(self, events: List[Dict[str, Any]]) -> str:
        out = bytearray()
        out.append(_REC_FRAME)
        _uvarint_encode(len(events), out)
        for ev in events:
            self._encode_event(ev, out)
        return _deflate_raw_b64(bytes(out))

    # --- public entry point ---

    def compress(self, raw_frame_str: str) -> Union[str, List[str]]:
        """
        Encode a raw websocket frame (string). Returns either:
          - one base64 line (frame), or
          - [header_line, frame_line] the first time a timestamped event is seen.
        """
        # PONG / non-JSON:
        try:
            obj = json.loads(raw_frame_str)
        except Exception:
            # raw text
            # header still needed (pool/timestamps), but we can write header with base_ts=0
            header = self._ensure_header(first_ts=None)
            out = bytearray()
            out.append(_REC_RAW)
            # store the raw string via string pool as a literal
            self.pool.encode(str(raw_frame_str), out)
            line = _deflate_raw_b64(bytes(out))
            return [header, line] if header else line

        # JSON dict or list
        if isinstance(obj, dict):
            events = [obj]
        elif isinstance(obj, list):
            # The market channel typically sends a list of one dict
            events = obj
        else:
            header = self._ensure_header(first_ts=None)
            out = bytearray()
            out.append(_REC_RAW)
            self.pool.encode(json.dumps(obj, separators=(",", ":")), out)
            line = _deflate_raw_b64(bytes(out))
            return [header, line] if header else line

        # Choose base_ts from the first event that has a numeric timestamp
        first_ts = None
        for ev in events:
            ts = self._get_ts(ev)
            if ts is not None:
                first_ts = ts
                break

        header = self._ensure_header(first_ts=first_ts)
        # Drop 'market' and 'hash' to shrink payload
        for ev in events:
            if isinstance(ev, dict):
                ev.pop("market", None)
                ev.pop("hash", None)

        frame_line = self._encode_frame_events(events)
        return [header, frame_line] if header else frame_line


# ---------- V3 Decoder (stateful) ----------

class _V3State:
    __slots__ = ("pool", "base_ts", "prev_ts", "asset_ids", "flags", "have_header")

    def __init__(self):
        self.pool = _StringPool()
        self.base_ts: int = 0
        self.prev_ts: Optional[int] = None
        self.asset_ids: List[str] = []
        self.flags: int = 0
        self.have_header: bool = False

    def reset(self):
        self.__init__()


def _decode_header(buf: bytes, i: int, st: _V3State) -> int:
    # 'H' [version] [flags] [base_ts] [asset_count] assets...
    ver, i = _uvarint_decode(buf, i)
    if ver != 3:
        raise ValueError(f"Unsupported V3 version: {ver}")
    flags, i = _uvarint_decode(buf, i)
    base_ts, i = _uvarint_decode(buf, i)
    st.base_ts = base_ts
    st.prev_ts = base_ts
    st.flags = flags
    # reset pool for new session
    st.pool.reset()
    # asset dict
    ac, i = _uvarint_decode(buf, i)
    st.asset_ids = []
    for _ in range(ac):
        ln, i = _uvarint_decode(buf, i)
        if i + ln > len(buf):
            raise ValueError("asset id truncated")
        st.asset_ids.append(buf[i:i+ln].decode("utf-8"))
        i += ln
    st.have_header = True
    return i


def _dec_levels(buf: bytes, i: int, st: _V3State) -> Tuple[List[Dict[str, str]], int]:
    n, i = _uvarint_decode(buf, i)
    out = []
    for _ in range(n):
        p, i = st.pool.decode(buf, i)
        s, i = st.pool.decode(buf, i)
        out.append({"price": p, "size": s})
    return out, i


def _decode_event(buf: bytes, i: int, st: _V3State) -> Tuple[Dict[str, Any], int]:
    tb = buf[i]
    i += 1
    et_code = tb & 0x07
    ts_abs = (tb & _TB_TS_ABS) != 0
    et = _ET_FROM.get(et_code, None)
    if et is None:
        raise ValueError(f"Unknown event type code: {et_code}")
    tsv, i = _uvarint_decode(buf, i)
    if ts_abs or st.prev_ts is None:
        ts = tsv
    else:
        ts = st.prev_ts + tsv
    st.prev_ts = ts

    # build event
    obj: Dict[str, Any] = {
        "event_type": et,
        "asset_id": st.asset_ids[0] if st.asset_ids else "",  # single-asset mode
        "timestamp": str(ts),
    }

    if et == "book":
        bids, i = _dec_levels(buf, i, st)
        asks, i = _dec_levels(buf, i, st)
        obj["bids"] = bids
        obj["asks"] = asks
    elif et == "price_change":
        n, i = _uvarint_decode(buf, i)
        chs = []
        for _ in range(n):
            side = "SELL" if buf[i] == 1 else "BUY"
            i += 1
            price, i = st.pool.decode(buf, i)
            size, i = st.pool.decode(buf, i)
            chs.append({"side": side, "price": price, "size": size})
        obj["changes"] = chs
    elif et == "tick_size_change":
        old_tick, i = st.pool.decode(buf, i)
        new_tick, i = st.pool.decode(buf, i)
        obj["old_tick_size"] = old_tick
        obj["new_tick_size"] = new_tick
    elif et == "last_trade_price":
        price, i = st.pool.decode(buf, i)
        size, i = st.pool.decode(buf, i)
        side = "SELL" if buf[i] == 1 else "BUY"
        i += 1
        obj["price"] = price
        obj["size"] = size
        obj["side"] = side
        if (tb & _TB_OPT0) != 0:
            fee, i = st.pool.decode(buf, i)
            obj["fee_rate_bps"] = fee

    return obj, i


def _decode_frame(buf: bytes, i: int, st: _V3State) -> Tuple[str, int]:
    # Return a JSON string for the whole frame (array of events)
    cnt, i = _uvarint_decode(buf, i)
    arr = []
    for _ in range(cnt):
        ev, i = _decode_event(buf, i, st)
        arr.append(ev)
    return json.dumps(arr, separators=(",", ":")), i


def _decode_raw(buf: bytes, i: int, st: _V3State) -> Tuple[str, int]:
    s, i = st.pool.decode(buf, i)
    return json.dumps(s, separators=(",", ":")), i


def _try_decode_v3_line(token: str, st: _V3State) -> Optional[List[str]]:
    """
    Try to interpret 'token' as V3 base64 (raw DEFLATE). Returns a list of
    zero or more JSON strings produced by this line, or None if not V3.
    """
    try:
        buf = _inflate_raw_b64(token)
    except Exception:
        return None  # not V3
    if not buf:
        return None
    i = 0
    kind = buf[i]
    i += 1
    if kind == _REC_HEADER:
        _decode_header(buf, i, st)
        return []  # header produces no JSON output
    elif kind == _REC_FRAME:
        js, _ = _decode_frame(buf, i, st)
        return [js]
    elif kind == _REC_RAW:
        js, _ = _decode_raw(buf, i, st)
        return [js]
    else:
        # looks like raw-deflate but unknown record kind: treat as not V3
        return None

# ---------- Legacy/tolerant paths (for mixed logs) ----------

def _strip_keys(d: Union[Dict[str, Any], List[Any], Any]) -> Any:
    if isinstance(d, dict):
        return {k: _strip_keys(v) for k, v in d.items() if k not in {"market", "hash"}}
    if isinstance(d, list):
        return [_strip_keys(x) for x in d]
    return d

def _maybe_json_value(line: str) -> Optional[str]:
    s = line.strip()
    if not s:
        return None
    if s[0] in "{[":
        try:
            return json.dumps(_strip_keys(json.loads(s)), separators=(",", ":"))
        except Exception:
            return None
    if s[0] == '"':
        try:
            inner = json.loads(s)
            if isinstance(inner, str) and inner and inner[0] in "[{":
                try:
                    return json.dumps(_strip_keys(json.loads(inner)), separators=(",", ":"))
                except Exception:
                    return json.dumps(inner, separators=(",", ":"))
            return json.dumps(inner, separators=(",", ":"))
        except Exception:
            return None
    return None

def _iter_any_entries(path: str):
    # Try read as JSON array
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for it in data:
                yield it
            return
    except json.JSONDecodeError:
        pass
    # NDJSON / raw lines
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.rstrip("\n")
            if s:
                yield s

# ---------- Public file reinflater ----------

def reinflate_file(input_path: str, output_path: str, ndjson: bool = True) -> None:
    """
    Read a (possibly mixed) log file and write reconstructed JSON:
      - V3 lines: require the header; emit arrays or strings per frame.
      - Legacy lines: JSONL wrappers or base64 zlib JSON — all tolerated.
    """
    st = _V3State()

    def yield_json_values():
        for entry in _iter_any_entries(input_path):
            # 1) If entry is a dict (JSONL wrapper), normalize
            if isinstance(entry, dict):
                if "c" in entry:
                    maybe = _try_decode_v3_line(entry["c"], st)
                    if maybe is not None:
                        for x in maybe:
                            yield x
                        continue
                    # legacy compact (zlib+base64 JSON array)
                    try:
                        raw = base64.urlsafe_b64decode(entry["c"].encode("ascii"))
                        txt = zlib.decompress(raw).decode("utf-8")
                        yield json.dumps(_strip_keys(json.loads(txt)), separators=(",", ":"))
                        continue
                    except Exception:
                        pass
                if "compressed" in entry:
                    maybe = _try_decode_v3_line(entry["compressed"], st)
                    if maybe is not None:
                        for x in maybe:
                            yield x
                        continue
                if "m" in entry:
                    m = entry["m"]
                    if isinstance(m, dict) and "_raw" in m and isinstance(m["_raw"], str):
                        yield json.dumps(m["_raw"], separators=(",", ":"))
                    else:
                        yield json.dumps(_strip_keys(m), separators=(",", ":"))
                    continue
                # Unknown wrapper; pass through (pruned)
                yield json.dumps(_strip_keys(entry), separators=(",", ":"))
                continue

            # 2) Raw string line: try V3 first
            if isinstance(entry, str):
                maybe = _try_decode_v3_line(entry, st)
                if maybe is not None:
                    for x in maybe:
                        yield x
                    continue

                # Legacy: maybe base64 zlib of JSON
                try:
                    raw = base64.urlsafe_b64decode(entry.encode("ascii"))
                    txt = zlib.decompress(raw).decode("utf-8")
                    yield json.dumps(_strip_keys(json.loads(txt)), separators=(",", ":"))
                    continue
                except Exception:
                    pass

                # Maybe the line is JSON (or JSON string containing JSON)
                mjs = _maybe_json_value(entry)
                if mjs is not None:
                    yield mjs
                    continue

                # Fallback: plain string as JSON string
                yield json.dumps(entry, separators=(",", ":"))
                continue

            # 3) Other JSON values from array input
            yield json.dumps(_strip_keys(entry), separators=(",", ":"))

    if ndjson:
        with open(output_path, "w", encoding="utf-8") as out:
            for js in yield_json_values():
                out.write(js)
                out.write("\n")
    else:
        values = list(yield_json_values())
        with open(output_path, "w", encoding="utf-8") as out:
            out.write("[")
            out.write(",".join(values))
            out.write("]")


# ---------- CLI ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Reinflate compact Polymarket logs (V3 + legacy).")
    ap.add_argument("--in", dest="input_path", required=True, help="input file path")
    ap.add_argument("--out", dest="output_path", required=True, help="output file path")
    ap.add_argument("--array", action="store_true",
                    help="write a single JSON array instead of NDJSON")
    args = ap.parse_args()
    reinflate_file(args.input_path, args.output_path, ndjson=not args.array)
