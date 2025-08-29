# Infa Lineage (Flask + Flat Files)

Informatica PowerCenter 10.5.x lineage lookup with cross-workflow stitching and column-rename handling. Flask serves both the API and a minimal UI. Storage uses flat JSON files (DB-ready abstraction in `storage.py`).

## Features
- Parse Mapping XML → instances, ports, edges, expressions, physical SRC/TGT
- Multi-hop lineage traversal across mappings/workflows via shared physical objects
- Column rename handling across workflows (normalized + fuzzy match with confidence)
- Flat-file storage with clean repository interface (easy to swap to SQLite/Postgres)
- CSV export from the UI

## Project Structure
```
infa-lineage/
├─ app.py
├─ storage.py
├─ parser_infa.py
├─ lineage.py
├─ requirements.txt
├─ templates/
│  └─ index.html
├─ samples/
│  ├─ Mapping_Load_Claims_1057.xml
│  ├─ Mapping_Build_Mart_1057.xml
│  └─ params_DEV.par
└─ data/              # runtime JSON written here
```

## Quickstart
```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python app.py
# open http://localhost:8000
```

### Ingest samples
- In UI, paste the two sample XML paths separated by comma, or:
```bash
curl -s -X POST "http://localhost:8000/api/ingest" ^
  -H "Content-Type: application/json" ^
  -d "{\"xml_paths\": [\"samples/Mapping_Load_Claims_1057.xml\", \"samples/Mapping_Build_Mart_1057.xml\"]}"
```

### Lookup
Visit the UI and search for `NET_AMT` or `BALANCE`, or call the API:
```
GET /api/lookup?field=NET_AMT
```

## API
- `POST /api/ingest` — body: `{ "xml_paths": ["..."] }`
- `GET  /api/lookup?field=<name>` — returns lineage hops JSON
- `POST /api/reset` — clears JSON tables in `data/`

## Migrate to a DB later
Swap implementations in `storage.py` (`upsert`, `insert_if_missing`, `all_rows`, `where`, `by_id`) with SQL/ORM equivalents. No changes to the parser, lineage logic, or Flask routes needed.

## Notes
- Samples are synthetic but compatible with the parser.
- Cross-stitching uses physical table equality; column continuity across workflows uses a best-match heuristic with a confidence score.
