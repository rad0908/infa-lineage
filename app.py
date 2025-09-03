from flask import Flask, request, jsonify, render_template
from pathlib import Path
import os
import storage as st
from parser_infa import parse_mapping_xml
from lineage import upstream_lineage_multi


PRELOAD_ON_START   = os.getenv("PRELOAD_ON_START", "0") == "1"  # default: skip preload
AUTO_LOAD_IF_EMPTY = os.getenv("AUTO_LOAD_IF_EMPTY", "1") == "1"  # default: lazy-load on first lookup

# Resolve mappings directory: env var takes precedence, else ./samples next to this file
_env_dir = os.environ.get("MAPPINGS_DIR")
MAPPINGS_DIR = Path(_env_dir) if _env_dir and _env_dir.strip() else Path(__file__).resolve().parent / "samples"

app = Flask(__name__)

def _xml_files_in_dir(root: Path):
    if not root.exists():
        return []
    exts = {".xml", ".XML"}
    return sorted([p for p in root.rglob("*") if p.is_file() and p.suffix in exts])


def load_all_mappings_from_dir():
    """Reset the flat-file store and parse all XMLs from MAPPINGS_DIR."""
    st.reset_all()
    xmls = _xml_files_in_dir(MAPPINGS_DIR)
    loaded, errors = [], []
    for p in xmls:
        try:
            mid = parse_mapping_xml(str(p))
            loaded.append({"file": str(p), "mapping_id": mid})
        except Exception as e:
            errors.append({"file": str(p), "error": str(e)})
            print(f"[load] failed {p}: {e}")
    return {"dir": str(MAPPINGS_DIR.resolve()), "files": len(xmls), "loaded": loaded, "errors": errors}


# Load once on startup
if PRELOAD_ON_START:
    LOAD_INFO = load_all_mappings_from_dir()
else:
    LOAD_INFO = {
        "dir": str(MAPPINGS_DIR.resolve()),
        "files": 0,
        "loaded": [],
        "errors": [],
        "skipped": True
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def health():
    return {
        "ok": True,
        "mappings_dir": str(MAPPINGS_DIR.resolve()),
        "loaded_files": LOAD_INFO.get("files", 0),
        "preloaded": PRELOAD_ON_START,
        "skipped": LOAD_INFO.get("skipped", False),
        "errors": LOAD_INFO.get("errors", []),
    }


@app.route("/api/reset", methods=["POST"])
def reset():
    global LOAD_INFO
    LOAD_INFO = load_all_mappings_from_dir()
    return jsonify(LOAD_INFO)


@app.route("/api/lookup")
def lookup():
    field = request.args.get("field", "")
    # One-time lazy load when first lookup happens (if nothing is loaded yet)
    if AUTO_LOAD_IF_EMPTY and not st.all_rows("mappings"):
        global LOAD_INFO
        LOAD_INFO = load_all_mappings_from_dir()
    rows = upstream_lineage_multi(field)
    return jsonify(rows)


@app.route("/api/debug/mappings")
def debug_mappings():
    return jsonify(st.all_rows("mappings"))


@app.route("/api/debug/targets")
def debug_targets():
    like = (request.args.get("like", "") or "").lower().replace("_", "")
    rows = []
    ports = st.all_rows("ports")
    insts = {r["instance_id"]: r for r in st.all_rows("instances")}
    maps = {r["mapping_id"]: r for r in st.all_rows("mappings")}
    edges = st.all_rows("edges")
    has_out = {e["from_port_id"] for e in edges}
    for p in ports:
        inst = insts.get(p["instance_id"])
        if not inst:
            continue
        is_targetish = (inst.get("type") == "Target") or (
            p["direction"] == "INPUT" and p["port_id"] not in has_out
        )
        if not is_targetish:
            continue
        norm = p["name"].lower().replace("_", "")
        if like in norm if like else True:
            rows.append(
                {
                    "mapping": maps[inst["mapping_id"]]["name"],
                    "target": inst["name"],
                    "column": p["name"],
                    "port_id": p["port_id"],
                }
            )
    return jsonify(rows)


@app.route("/api/debug/edges")
def debug_edges():
    to_like = (request.args.get("to_like", "") or "").lower()
    fr_like = (request.args.get("from_like", "") or "").lower()
    edges = st.all_rows("edges")
    rows = []
    for e in edges:
        to_ok = to_like in e["to_port_id"].lower() if to_like else True
        fr_ok = fr_like in e["from_port_id"].lower() if fr_like else True
        if to_ok and fr_ok:
            rows.append(e)
    return {"count": len(rows), "sample": rows[:100]}


@app.route("/api/summary")
def summary():
    """
    Returns:
      - pair_rows: unique end-to-end pairs as DB.SCHEMA.TABLE.FIELD -> DB.SCHEMA.TABLE.FIELD (+ mapping)
      - summary_rows: per-mapping rollup (steps/exprs/joins)
    """
    from lineage import upstream_lineage_multi
    import storage as st
    from collections import defaultdict

    field = request.args.get("field", "")
    rows = upstream_lineage_multi(field)

    # ---- indices for physical resolution ----
    maps = st.all_rows("mappings")
    map_ids_by_name = {}
    for m in maps:
        map_ids_by_name.setdefault(m["name"], []).append(m["mapping_id"])

    insts = st.all_rows("instances")
    inst_exists = {i["instance_id"] for i in insts}

    # instance -> physical object
    inst_phys = st.all_rows("instance_phys")  # [{"instance_id","object_id","role"}]
    inst_to_obj = {r["instance_id"]: r["object_id"] for r in inst_phys}

    phys_idx = st.by_id("physical_objects", "object_id")  # {object_id: {..., full_name}}

    def resolve_instance_id(mapping_name: str, instance_name: str) -> str:
        """Find the concrete instance_id for (mapping, instance) or ''."""
        if not (mapping_name and instance_name):
            return ""
        for mid in map_ids_by_name.get(mapping_name, []):
            iid = f"{mid}:{instance_name}"
            if iid in inst_exists:
                return iid
        return ""

    def fqf(mapping_name: str, instance_name: str, field_name: str) -> str:
        """Return DB.SCHEMA.TABLE.FIELD if resolvable, else just FIELD."""
        if not field_name:
            return ""
        iid = resolve_instance_id(mapping_name, instance_name)
        obj_id = inst_to_obj.get(iid, "")
        obj = phys_idx.get(obj_id, {}) if obj_id else {}
        full = obj.get("full_name", "")
        return f"{full}.{field_name}" if full else field_name

    # ---- group rows by chain ----
    rows_by_chain = defaultdict(list)
    for r in rows:
        rows_by_chain[r.get("chain_id", 1)].append(r)

    pair_set = set()
    pair_rows = []

    def is_real_inst(name: str) -> bool:
        return bool(name) and not str(name).startswith("(")  # exclude "(SOURCE)"/"(TARGET)" markers

    for cid, crs in rows_by_chain.items():
        # primary (type-based) leaf/tail detection
        leaves = [r for r in crs
                  if (r.get("from_type","").lower() == "source") and is_real_inst(r.get("from_instance",""))]
        tails  = [r for r in crs
                  if (r.get("to_type","").lower() == "target") and is_real_inst(r.get("to_instance",""))]

        # fallback: set-difference heuristic if types are missing
        if not leaves or not tails:
            to_keys   = set(f"{r.get('to_instance')}::{r.get('to_port')}"   for r in crs)
            from_keys = set(f"{r.get('from_instance')}::{r.get('from_port')}" for r in crs)
            if not leaves:
                leaves = [r for r in crs if f"{r.get('from_instance')}::{r.get('from_port')}" not in to_keys and is_real_inst(r.get("from_instance",""))]
            if not tails:
                tails  = [r for r in crs if f"{r.get('to_instance')}::{r.get('to_port')}" not in from_keys and is_real_inst(r.get("to_instance",""))]

        # build pairs
        for leaf in leaves:
            for tail in tails:
                src_fq = fqf(leaf.get("mapping",""), leaf.get("from_instance",""), leaf.get("from_port",""))
                tgt_fq = fqf(tail.get("mapping",""), tail.get("to_instance",""),  tail.get("to_port",""))
                if not src_fq or not tgt_fq:
                    continue
                key = (tail.get("mapping",""), src_fq, tgt_fq)
                if key in pair_set:
                    continue
                pair_set.add(key)
                pair_rows.append({
                    "mapping": tail.get("mapping",""),
                    "source":  src_fq,
                    "target":  tgt_fq,
                })

    # nice stable ordering
    pair_rows.sort(key=lambda r: (r["mapping"], r["target"], r["source"]))

    # ---- per-mapping rollup (unchanged) ----
    agg = {}
    for r in rows:
        k = r.get("mapping", "")
        g = agg.setdefault(k, {"mapping": k, "steps": 0, "exprs": set(), "joins": set()})
        g["steps"] += 1
        if r.get("expression"):
            g["exprs"].add(r["expression"])
        if r.get("join_condition"):
            g["joins"].add(r["join_condition"])

    summary_rows = []
    for g in sorted(agg.values(), key=lambda x: x["mapping"]):
        summary_rows.append({
            "mapping": g["mapping"],
            "steps": g["steps"],
            "expr_count": len(g["exprs"]),
            "join_count": len(g["joins"]),
            "expr_examples": list(g["exprs"])[:2],
            "join_examples": list(g["joins"])[:1],
        })

    return jsonify({
        "total_rows": len(rows),
        "pair_rows": pair_rows,
        "summary_rows": summary_rows,
    })



if __name__ == "__main__":
    app.run(debug=True, port=8001)