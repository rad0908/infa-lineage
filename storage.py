import json
from pathlib import Path
from typing import Dict, List, Iterable, Any, Tuple

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

TABLES = [
    "mappings",
    "instances",
    "ports",
    "edges",
    "expressions",
    "physical_objects",
    "map_sources",
    "map_targets",
    "crosslinks"
]

def _file(name: str) -> Path:
    return DATA_DIR / f"{name}.json"

def _read(name: str) -> List[Dict[str, Any]]:
    p = _file(name)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write(name: str, rows: List[Dict[str, Any]]):
    with _file(name).open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

def reset_all():
    for t in TABLES:
        _write(t, [])

def upsert(table: str, rows: Iterable[Dict[str, Any]], key_fields: Tuple[str, ...]):
    existing = _read(table)
    index = {tuple(str(r.get(k)) for k in key_fields): i for i, r in enumerate(existing)}
    for row in rows:
        k = tuple(str(row.get(kf)) for kf in key_fields)
        if k in index:
            existing[index[k]] = row
        else:
            existing.append(row)
    _write(table, existing)

def insert_if_missing(table: str, rows: Iterable[Dict[str, Any]], key_fields: Tuple[str, ...]):
    existing = _read(table)
    index = {tuple(str(r.get(k)) for k in key_fields): i for i, r in enumerate(existing)}
    changed = False
    for row in rows:
        k = tuple(str(row.get(kf)) for kf in key_fields)
        if k not in index:
            existing.append(row)
            changed = True
    if changed:
        _write(table, existing)

def all_rows(table: str) -> List[Dict[str, Any]]:
    return _read(table)

def where(table: str, **kwargs) -> List[Dict[str, Any]]:
    rows = _read(table)
    if not kwargs:
        return rows
    out = []
    for r in rows:
        ok = True
        for k, v in kwargs.items():
            if r.get(k) != v:
                ok = False
                break
        if ok:
            out.append(r)
    return out

def by_id(table: str, id_field: str) -> Dict[str, Dict[str, Any]]:
    return {r[id_field]: r for r in _read(table) if id_field in r}
