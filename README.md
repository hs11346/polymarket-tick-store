# Polymarket Tick Store
To run the algorithm, provide the `asset` and `out` variables for the asset id and JSON path respectively via CLI:

```bash
python polymarket_market_logger.py --asset YOUR_ASSET_ID --out updates.json
```

To decode the algorithm, run the `decoder.py` file like this:

```bash
python "path/to/decoder.py" --in "path\to\compressed_updates.json" --out "path\to\save.json"
```
