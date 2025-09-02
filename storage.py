
from pathlib import Path
import json

BASE = Path(__file__).resolve().parent / "data"
TABLES = ["mappings","instances","ports","edges","expressions","physical_objects","map_sources","map_targets"]

def _path(table): return BASE / f"{table}.json"

def reset_all():
    BASE.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        (_path(t)).write_text("[]", encoding="utf-8")

def _load(table):
    p = _path(table)
    if not p.exists(): return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

def _save(table, rows):
    _path(table).write_text(json.dumps(rows, ensure_ascii=False, indent=0), encoding="utf-8")

def all_rows(table): return _load(table)

def by_id(table, key_field):
    out = {}
    for r in _load(table):
        out[r.get(key_field)] = r
    return out

def where(table, **kwargs):
    rows = _load(table)
    return [r for r in rows if all(r.get(k)==v for k,v in kwargs.items())]

def upsert(table, rows, keys):
    existing = _load(table)
    index = {tuple(r.get(k) for k in keys): i for i, r in enumerate(existing)}
    for r in rows:
        k = tuple(r.get(k) for k in keys)
        if k in index:
            i = index[k]
            existing[i] = {**existing[i], **r}
        else:
            index[k] = len(existing)
            existing.append(r)
    _save(table, existing)

def insert_if_missing(table, rows, keys):
    existing = _load(table)
    seen = {tuple(r.get(k) for k in keys) for r in existing}
    for r in rows:
        k = tuple(r.get(k) for k in keys)
        if k not in seen:
            existing.append(r); seen.add(k)
    _save(table, existing)
