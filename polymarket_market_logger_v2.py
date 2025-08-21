#!/usr/bin/env python3
"""
Polymarket market-channel logger (MAX COMPRESSION, V3)

- Connects to Polymarket CLOB 'market' websocket for a single asset_id.
- Emits:
    * One **header** line (on first frame after start) with session metadata and asset_id.
    * Then **one compressed base64-url line per server frame** (opaque).
- Format: custom V3 binary records with:
    * dictionary pooling for strings across the session
    * timestamp delta encoding
    * raw DEFLATE (no zlib header/trailer) + base64-url
    * single-asset mode: asset_id omitted from events

Usage:
  python polymarket_market_logger.py --asset <ASSET_ID> --out <PATH> [--verbose]
  # Optional: store JSONL wrappers instead of bare base64 (debug/testing)
  python polymarket_market_logger.py --asset <ASSET_ID> --out <PATH> --jsonl
  # Optional: store raw JSON (no compression); implies --jsonl
  python polymarket_market_logger.py --asset <ASSET_ID> --out <PATH> --raw --jsonl

Requires:
  pip install websocket-client
"""
import argparse
import json
import os
import random
import signal
import sys
import threading
import time
from typing import Optional, Callable, Union, List

from websocket import WebSocketApp  # pip install websocket-client

# Import the V3 compressor (stateful) from decoder.py (same directory)
try:
    from decoder import FrameCompressorV3  # noqa
except Exception:
    FrameCompressorV3 = None  # validated at runtime

WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
WS_PATH = "/ws/market"  # market channel path
PING_INTERVAL_SEC = 10
MAX_BACKOFF_SEC = 60


class DurableJsonlWriter:
    """
    Append-only writer that can write either JSONL (write_json) or raw lines (write_line).
    Flushes and fsyncs each record for durability.
    """
    def __init__(self, path: str):
        self.path = path
        self._fh = open(self.path, "a", buffering=1, encoding="utf-8")
        self._lock = threading.Lock()

    def write_json(self, obj):
        line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def write_line(self, line: str):
        if not isinstance(line, str):
            raise TypeError("write_line expects a str")
        if "\n" in line:
            line = line.replace("\n", "\\n")
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def close(self):
        try:
            with self._lock:
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()
        except Exception:
            pass


class MarketSubscriber:
    """
    Subscribes to the Polymarket 'market' websocket for a single asset_id.

    If compression_fn is provided and compact_records=True, each server frame is
    compressed to one or more base64 lines (opaque) for maximum compactness.
    The function may return either a single string or a list of strings (e.g., header + frame).

    If compression_fn is provided and compact_records=False, we write JSONL records
    with short keys: {"t": <epoch_ms>, "a": <asset_id>, "c": "<compressed>"}.
    The function may return a single string or a list of strings.

    If compression_fn is None, we store the parsed JSON payload (or raw string)
    under {"t": <epoch_ms>, "a": <asset_id>, "m": <payload>}.
    """
    def __init__(
        self,
        asset_id: str,
        out_path: str,
        verbose: bool = True,
        compression_fn: Optional[Callable[[str], Union[str, List[str]]]] = None,
        compact_records: bool = True,
    ):
        self.asset_id = asset_id
        self.out = DurableJsonlWriter(out_path)
        self.verbose = verbose
        self.ws: Optional[WebSocketApp] = None
        self._stop = threading.Event()
        self._ping_thread: Optional[threading.Thread] = None
        self.compression_fn = compression_fn
        self.compact_records = compact_records

    def _log(self, *args):
        if self.verbose:
            print(*args, file=sys.stderr, flush=True)

    def _on_open(self, ws):
        self._log("[open] connected, subscribing...")
        sub_msg = {"assets_ids": [self.asset_id], "type": "market"}
        ws.send(json.dumps(sub_msg))
        self._log("[open] subscribe payload sent:", sub_msg)
        # Start PING loop
        self._ping_thread = threading.Thread(target=self._ping_loop, args=(ws,), daemon=True)
        self._ping_thread.start()

    def _write_compact(self, payload: Union[str, List[str]]):
        if isinstance(payload, list):
            for item in payload:
                self.out.write_line(item)
        else:
            self.out.write_line(payload)

    def _write_jsonl(self, epoch_ms: int, payload: Union[str, List[str]]):
        if isinstance(payload, list):
            for item in payload:
                self.out.write_json({"t": epoch_ms, "a": self.asset_id, "c": item})
        else:
            self.out.write_json({"t": epoch_ms, "a": self.asset_id, "c": payload})

    def _on_message(self, ws, message: str):
        epoch_ms = int(time.time() * 1000)

        # Log event type if possible
        ev = None
        try:
            maybe = json.loads(message)
            if isinstance(maybe, dict):
                ev = maybe.get("event_type")
            elif isinstance(maybe, list) and maybe:
                head = maybe[0]
                if isinstance(head, dict):
                    ev = head.get("event_type")
                    if len(maybe) > 1:
                        ev = f"{ev}+{len(maybe)-1}"
        except Exception:
            pass

        if self.compression_fn:
            try:
                compressed = self.compression_fn(message)  # may return str or [str, ...]
            except Exception as e:
                # Fail-safe path: store parsed JSON to avoid data loss.
                self._log("[compress error]", e)
                try:
                    payload = json.loads(message)
                except Exception:
                    payload = {"_raw": message}
                self.out.write_json({"t": epoch_ms, "a": self.asset_id, "m": payload})
                return

            if self.compact_records:
                self._write_compact(compressed)
            else:
                self._write_jsonl(epoch_ms, compressed)
        else:
            # Raw JSON path (debug)
            try:
                payload = json.loads(message)
            except Exception:
                payload = {"_raw": message}
            self.out.write_json({"t": epoch_ms, "a": self.asset_id, "m": payload})

        if ev:
            self._log(f"[msg] {ev} @ {epoch_ms}")
        else:
            self._log(f"[msg] @ {epoch_ms}")

    def _on_error(self, ws, error):
        self._log("[error]", error)

    def _on_close(self, ws, code, reason):
        self._log(f"[close] code={code} reason={reason}")

    def _ping_loop(self, ws):
        while not self._stop.is_set():
            try:
                ws.send("PING")
            except Exception as e:
                self._log("[ping] error:", e)
                return
            time.sleep(PING_INTERVAL_SEC)

    def _make_ws(self):
        url = WS_BASE + WS_PATH
        return WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

    def run_forever(self):
        backoff = 1.0
        try:
            while not self._stop.is_set():
                self.ws = self._make_ws()
                self._log(f"[connect] {WS_BASE}{WS_PATH}")
                try:
                    self.ws.run_forever()
                except KeyboardInterrupt:
                    self._log("[interrupt] stopping...")
                    break
                finally:
                    self._stop.set()
                    if self._ping_thread and self._ping_thread.is_alive():
                        self._ping_thread.join(timeout=1.0)
                    self._ping_thread = None

                if self._stop.is_set():
                    break

                sleep_for = min(backoff, MAX_BACKOFF_SEC) + random.random()
                self._log(f"[reconnect] retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)
                backoff = min(backoff * 2, MAX_BACKOFF_SEC)
                self._stop.clear()
        finally:
            self.out.close()

    def stop(self):
        self._stop.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.out.close()


def main():

    asset = '60877716797186734067501445298560391388366404930163071113536019971288497460774'
    out = r'C:\Users\hsee\OneDrive - LMR Partners\Documents\compression\v2\updates_12am.json'
    verbose = True

    if FrameCompressorV3 is None:
        print("ERROR: decoder.FrameCompressorV3  not available. "
                "Ensure decoder.py is in the same directory.", file=sys.stderr)
    comp = FrameCompressorV3(asset_id=asset)
    compression_fn = comp.compress
    compact_records = True

    sub = MarketSubscriber(
        asset_id=asset,
        out_path=out,
        verbose=verbose,
        compression_fn=compression_fn,
        compact_records=compact_records,
    )

    def handle_sig(sig, frame):
        sub.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    sub.run_forever()

if __name__ == "__main__":
    main()
