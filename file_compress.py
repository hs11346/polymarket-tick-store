#!/usr/bin/env python3
"""
Lossless post-compression for Polymarket V3 logs (or any file).

Features (in priority order):
1) System 7-Zip CLI (7z/7za/7zr/7zz) with Ultra LZMA2, solid archive, large dictionary.
2) Pure-Python 7z via py7zr if CLI is unavailable.
3) Built-in .xz fallback (lzma) if neither 7z CLI nor py7zr is available.

CLI examples:
  # Compress with the best available method (prefers 7z Ultra)
  python archive_logs.py compress --in updates.v3
  # -> produces updates.v3.7z (or updates.v3.xz as a fallback)

  # Decompress back
  python archive_logs.py decompress --in updates.v3.7z --out-dir ./restored

  # Force method and set password (7z only; headers encrypted too)
  python archive_logs.py compress --in updates.v3 --method 7z --password secret

  # Use py7zr explicitly
  python archive_logs.py compress --in updates.v3 --method py7zr

  # Use .xz (built-in lzma)
  python archive_logs.py compress --in updates.v3 --method xz

All methods are strictly LOSSLESS.
"""

from __future__ import annotations

import argparse
import os
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Optional

# Optional: pure-Python 7z library
try:
    import py7zr  # type: ignore
    _HAS_PY7ZR = True
except Exception:
    _HAS_PY7ZR = False

import lzma  # built-in .xz fallback


# ------------------------------
# Utilities
# ------------------------------

def _find_7z_exe() -> Optional[str]:
    """Return a path to a 7-Zip CLI if available on PATH, else None."""
    for name in ("7z", "7za", "7zr", "7zz"):  # support common 7z binaries
        p = shutil.which(name)
        if p:
            return p
    return None


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _derive_archive_path(input_path: Path, method: str) -> Path:
    if method == "xz":
        return input_path.with_name(input_path.name + ".xz")
    # default to .7z
    return input_path.with_name(input_path.name + ".7z")


# ------------------------------
# 7z via system CLI (preferred)
# ------------------------------

def compress_with_7z_cli(
    input_path: str,
    archive_path: Optional[str] = None,
    *,
    dict_size_mb: int = 256,
    solid: bool = True,
    password: Optional[str] = None,
) -> Path:
    """
    Compress a single file using the system 7z binary with ULTRA settings.
    - LZMA2, solid blocks, maximum compression, large dictionary, multi-threaded.
    - If password is provided, headers are encrypted too (-mhe=on).

    Returns the Path to the created .7z file.
    """
    sevenzip = _find_7z_exe()
    if not sevenzip:
        raise RuntimeError("7z CLI not found on PATH")

    ip = Path(input_path).resolve()
    if not ip.is_file():
        raise FileNotFoundError(f"Input file not found: {ip}")

    ap = Path(archive_path).resolve() if archive_path else _derive_archive_path(ip, "7z")
    _ensure_parent_dir(ap)

    # Build 7z command (Ultra LZMA2)
    # -mx=9 : Ultra
    # -m0=lzma2 : use LZMA2
    # -md=256m : dictionary size
    # -mfb=273 : number of fast bytes (max)
    # -ms=on/off : solid archive
    # -mmt=on : multithread
    # -bd : disable progress
    # -y : assume yes on all queries
    cmd = [
        sevenzip, "a",
        "-mx=9",
        "-m0=lzma2",
        f"-md={int(dict_size_mb)}m",
        "-mfb=273",
        "-mmt=on",
        "-bd",
        "-y",
    ]
    cmd.append("-ms=on" if solid else "-ms=off")

    if password:
        cmd += [f"-p{password}", "-mhe=on"]  # encrypt headers (hide filenames)

    cmd += [str(ap), ip.name]

    # Run from the file's directory so only the basename is stored in the archive.
    print(f"[7z-cli] {' '.join(cmd)}  (cwd={ip.parent})", file=sys.stderr)
    subprocess.run(cmd, cwd=str(ip.parent), check=True)
    return ap


def decompress_with_7z_cli(
    archive_path: str,
    out_dir: str = ".",
    *,
    password: Optional[str] = None,
) -> Path:
    """
    Decompress a .7z archive using system 7z CLI into out_dir.
    Returns the output directory path.
    """
    sevenzip = _find_7z_exe()
    if not sevenzip:
        raise RuntimeError("7z CLI not found on PATH")

    ap = Path(archive_path).resolve()
    if not ap.exists():
        raise FileNotFoundError(f"Archive not found: {ap}")

    outp = Path(out_dir).resolve()
    outp.mkdir(parents=True, exist_ok=True)

    cmd = [sevenzip, "x", "-bd", "-y", f"-o{str(outp)}"]
    if password:
        cmd.append(f"-p{password}")
    cmd.append(str(ap))

    print(f"[7z-cli] {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True)
    return outp


# ------------------------------
# 7z via py7zr (fallback 1)
# ------------------------------

def compress_with_py7zr(
    input_path: str,
    archive_path: Optional[str] = None,
    *,
    dict_size_mb: int = 256,
    password: Optional[str] = None,
    solid: bool = True,  # py7zr writes solid archives by default
) -> Path:
    """
    Compress a single file using py7zr with LZMA2 Ultra-like settings.
    Returns the Path to the created .7z file.
    """
    if not _HAS_PY7ZR:
        raise RuntimeError("py7zr not installed (pip install py7zr)")

    ip = Path(input_path).resolve()
    if not ip.is_file():
        raise FileNotFoundError(f"Input file not found: {ip}")

    ap = Path(archive_path).resolve() if archive_path else _derive_archive_path(ip, "7z")
    _ensure_parent_dir(ap)

    # LZMA2 filter with large dictionary and 'preset' 9 (ultra-like).
    # Note: available keys may vary by py7zr version; the below works broadly.
    filters = [{
        "id": py7zr.FILTER_LZMA2,
        "preset": 9,
        "dict_size": int(dict_size_mb) * 1024 * 1024,
    }]

    # Header encryption (hide filenames) when password is set.
    header_encryption = bool(password)

    print(f"[py7zr] writing {ap}", file=sys.stderr)
    with py7zr.SevenZipFile(str(ap), mode="w",
                            filters=filters,
                            password=password,
                            header_encryption=header_encryption) as z:
        # Store only the basename inside the archive.
        z.write(str(ip), arcname=ip.name)
    return ap


def decompress_with_py7zr(
    archive_path: str,
    out_dir: str = ".",
    *,
    password: Optional[str] = None,
) -> Path:
    """
    Decompress a .7z archive using py7zr into out_dir.
    Returns the output directory path.
    """
    if not _HAS_PY7ZR:
        raise RuntimeError("py7zr not installed (pip install py7zr)")

    ap = Path(archive_path).resolve()
    if not ap.exists():
        raise FileNotFoundError(f"Archive not found: {ap}")

    outp = Path(out_dir).resolve()
    outp.mkdir(parents=True, exist_ok=True)

    print(f"[py7zr] extracting {ap} -> {outp}", file=sys.stderr)
    with py7zr.SevenZipFile(str(ap), mode="r", password=password) as z:
        z.extractall(path=str(outp))
    return outp


# ------------------------------
# .xz via built-in lzma (fallback 2)
# ------------------------------

def compress_with_xz(
    input_path: str,
    archive_path: Optional[str] = None,
    *,
    preset: int = 9,  # max
    block_size_mb: Optional[int] = None,  # None = default
) -> Path:
    """
    Compress a single file to .xz using built-in lzma (lossless).
    NOTE: .xz is a single-file stream format (not a container).
    """
    ip = Path(input_path).resolve()
    if not ip.is_file():
        raise FileNotFoundError(f"Input file not found: {ip}")

    ap = Path(archive_path).resolve() if archive_path else _derive_archive_path(ip, "xz")
    _ensure_parent_dir(ap)

    # Stream copy to avoid loading the whole file in memory.
    print(f"[xz] writing {ap} (preset={preset})", file=sys.stderr)
    filters = None
    if block_size_mb:
        # Optional: use a block size to improve ratio on huge files (platform dependent).
        filters = [{"id": lzma.FILTER_LZMA2, "preset": preset, "dict_size": block_size_mb * 1024 * 1024}]

    CHUNK = 8 * 1024 * 1024
    if filters is None:
        with open(ip, "rb") as src, lzma.open(ap, "wb", preset=preset, format=lzma.FORMAT_XZ) as dst:
            while True:
                b = src.read(CHUNK)
                if not b:
                    break
                dst.write(b)
    else:
        with open(ip, "rb") as src, lzma.LZMAFile(ap, "wb", format=lzma.FORMAT_XZ, filters=filters) as dst:
            while True:
                b = src.read(CHUNK)
                if not b:
                    break
                dst.write(b)
    return ap


def decompress_xz(
    archive_path: str,
    out_dir: str = ".",
) -> Path:
    """
    Decompress a .xz file to out_dir, restoring the original filename by stripping .xz.
    Returns the path to the decompressed file.
    """
    ap = Path(archive_path).resolve()
    if not ap.exists():
        raise FileNotFoundError(f"Archive not found: {ap}")

    outp = Path(out_dir).resolve()
    outp.mkdir(parents=True, exist_ok=True)

    out_file = outp / ap.name[:-3] if ap.name.lower().endswith(".xz") else outp / (ap.name + ".out")

    print(f"[xz] extracting {ap} -> {out_file}", file=sys.stderr)
    CHUNK = 8 * 1024 * 1024
    with lzma.open(ap, "rb") as src, open(out_file, "wb") as dst:
        while True:
            b = src.read(CHUNK)
            if not b:
                break
            dst.write(b)
    return out_file


# ------------------------------
# Public convenience wrappers
# ------------------------------

def compress_file(
    input_path: str,
    archive_path: Optional[str] = None,
    *,
    method: str = "auto",          # "auto" | "7z" | "py7zr" | "xz"
    password: Optional[str] = None,
    dict_size_mb: int = 256,
    solid: bool = True,
) -> Path:
    """
    Compress a file with the best available lossless method.

    - method="auto": prefer system 7z CLI -> py7zr -> xz
    - method="7z"  : require system 7z CLI
    - method="py7zr": require py7zr
    - method="xz"  : built-in lzma (.xz stream)

    Returns the Path to the created archive.
    """
    method = method.lower()
    if method not in {"auto", "7z", "py7zr", "xz"}:
        raise ValueError("method must be one of: auto, 7z, py7zr, xz")

    if method == "7z":
        return compress_with_7z_cli(input_path, archive_path, dict_size_mb=dict_size_mb, solid=solid, password=password)

    if method == "py7zr":
        return compress_with_py7zr(input_path, archive_path, dict_size_mb=dict_size_mb, password=password, solid=solid)

    if method == "xz":
        if password:
            raise ValueError(".xz does not support passwords")
        return compress_with_xz(input_path, archive_path, preset=9)

    # auto
    if _find_7z_exe():
        return compress_with_7z_cli(input_path, archive_path, dict_size_mb=dict_size_mb, solid=solid, password=password)
    if _HAS_PY7ZR:
        return compress_with_py7zr(input_path, archive_path, dict_size_mb=dict_size_mb, password=password, solid=solid)
    if password:
        raise RuntimeError("No 7z available and .xz does not support passwords")
    return compress_with_xz(input_path, archive_path, preset=9)


def decompress_file(
    archive_path: str,
    out_dir: str = ".",
    *,
    method: Optional[str] = None,   # infer from extension if None
    password: Optional[str] = None,
) -> Path:
    """
    Decompress an archive created by this script.

    - If method is None, infer from extension (.7z -> 7z, .xz -> xz).
    - For .7z: prefer system 7z CLI; fall back to py7zr if available.
    - For .xz: use built-in lzma.
    Returns the output path (dir for .7z, file for .xz).
    """
    ap = Path(archive_path)
    ext = ap.suffix.lower()
    if method is None:
        method = "7z" if ext == ".7z" else "xz" if ext == ".xz" else "auto"

    method = method.lower()
    if method == "7z" or (method == "auto" and ext == ".7z"):
        if _find_7z_exe():
            return decompress_with_7z_cli(archive_path, out_dir, password=password)
        if _HAS_PY7ZR:
            return decompress_with_py7zr(archive_path, out_dir, password=password)
        raise RuntimeError("Neither 7z CLI nor py7zr is available to extract .7z")

    if method == "xz" or (method == "auto" and ext == ".xz"):
        if password:
            raise ValueError(".xz does not support passwords")
        return decompress_xz(archive_path, out_dir)

    # If we get here, try everything
    if ext == ".7z":
        if _find_7z_exe():
            return decompress_with_7z_cli(archive_path, out_dir, password=password)
        if _HAS_PY7ZR:
            return decompress_with_py7zr(archive_path, out_dir, password=password)
        raise RuntimeError("Cannot extract .7z: no 7z tool/library available")
    if ext == ".xz":
        if password:
            raise ValueError(".xz does not support passwords")
        return decompress_xz(archive_path, out_dir)

    # Unknown extension; attempt 7z CLI then py7zr then xz
    if _find_7z_exe():
        try:
            return decompress_with_7z_cli(archive_path, out_dir, password=password)
        except Exception:
            pass
    if _HAS_PY7ZR:
        try:
            return decompress_with_py7zr(archive_path, out_dir, password=password)
        except Exception:
            pass
    # last resort: xz
    try:
        return decompress_xz(archive_path, out_dir)
    except Exception:
        raise RuntimeError(f"Unknown archive format or no tools available: {archive_path}")


# ------------------------------
# CLI
# ------------------------------

def _build_cli():
    p = argparse.ArgumentParser(description="Lossless compressor/decompressor for log files (prefers 7z Ultra).")
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("compress", help="Compress a file (prefers 7z Ultra)")
    pc.add_argument("--in", dest="inp", required=True, help="Input file path")
    pc.add_argument("--out", dest="outp", default=None, help="Output archive path (default: add .7z or .xz)")
    pc.add_argument("--method", choices=["auto", "7z", "py7zr", "xz"], default="auto",
                    help="Compression backend (default: auto)")
    pc.add_argument("--password", default=None, help="Archive password (7z only). Headers are encrypted too.")
    pc.add_argument("--dict-mb", type=int, default=256, help="Dictionary size for 7z (MiB). Default: 256")
    pc.add_argument("--no-solid", action="store_true", help="Disable solid archive mode (7z)")

    pd = sub.add_parser("decompress", help="Decompress an archive")
    pd.add_argument("--in", dest="arch", required=True, help="Input archive path (.7z or .xz)")
    pd.add_argument("--out-dir", dest="out_dir", default=".", help="Output directory (for .7z) or file's directory (.xz)")
    pd.add_argument("--method", choices=["auto", "7z", "py7zr", "xz"], default=None,
                    help="Decompression backend (default: infer from extension)")
    pd.add_argument("--password", default=None, help="Archive password (7z only)")

    return p


def main():
    cli = _build_cli()
    args = cli.parse_args()

    if args.cmd == "compress":
        solid = not args.no_solid
        try:
            out = compress_file(args.inp, args.outp, method=args.method,
                                password=args.password, dict_size_mb=args.dict_mb, solid=solid)
        except Exception as e:
            print(f"[compress error] {e}", file=sys.stderr)
            sys.exit(2)
        print(str(out))
        return

    if args.cmd == "decompress":
        try:
            out = decompress_file(args.arch, out_dir=args.out_dir, method=args.method, password=args.password)
        except Exception as e:
            print(f"[decompress error] {e}", file=sys.stderr)
            sys.exit(2)
        print(str(out))
        return


if __name__ == "__main__":
    main()
