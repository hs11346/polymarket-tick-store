# Polymarket Tick Store
To run the algorithm, provide the `asset` and `out` variables for the asset id and JSON path respectively via CLI:

```bash
python polymarket_market_logger.py --asset YOUR_ASSET_ID --out updates.json
```

To decode the algorithm, run the `decoder.py` file like this:

```bash
python "path/to/decoder.py" --in "path\to\compressed_updates.json" --out "path\to\save.json"
```
You can compress the output JSON file even further by using `file_compress.py` to compress the entire file.

```bash
python file_compress.py compress --in "path\to\compressed_updates.json" 
```

To decompress: 
```bash
python file_compress.py decompress --in "path\to\compressed_updates.json.xz" 
```
