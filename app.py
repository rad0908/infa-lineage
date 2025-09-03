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
    Build a deterministic summary from upstream_lineage_multi:
    One row per (chain_id, mapping) in chain order.
    """
    from lineage import upstream_lineage_multi

    field = request.args.get("field", "")
    rows = upstream_lineage_multi(field)

    # Group by (chain_id, mapping) and keep first step order
    chains = {}
    for r in sorted(rows, key=lambda x: (x.get("chain_id", 1), x.get("step_no", 0))):
        cid = r.get("chain_id", 1)
        mp  = r.get("mapping", "")
        ch  = chains.setdefault(cid, {})
        itm = ch.setdefault(mp, {
            "chain_id": cid,
            "mapping": mp,
            "first_step": r.get("step_no", 0),
            "steps": 0,
            "exprs": set(),
            "joins": set(),
        })
        itm["steps"] += 1
        if r.get("expression"):
            itm["exprs"].add(r["expression"])
        if r.get("join_condition"):
            itm["joins"].add(r["join_condition"])

    # Flatten to table rows in chain order
    summary_rows = []
    for cid, maps in chains.items():
        items = list(maps.values())
        items.sort(key=lambda x: x["first_step"])
        seq = 0
        for it in items:
            seq += 1
            summary_rows.append({
                "chain_id": cid,
                "seq": seq,                            # mapping order within the chain
                "mapping": it["mapping"],
                "steps": it["steps"],
                "expr_count": len(it["exprs"]),
                "join_count": len(it["joins"]),
                "expr_examples": list(it["exprs"])[:2],   # show a couple inline; expand in UI if needed
                "join_examples": list(it["joins"])[:1],
            })

    return jsonify({
        "total_rows": len(rows),
        "summary_rows": summary_rows
    })


if __name__ == "__main__":
    app.run(debug=True, port=8001)