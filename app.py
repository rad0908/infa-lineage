from flask import Flask, request, jsonify, render_template
from pathlib import Path
import os
import storage as st
from parser_infa import parse_mapping_xml
from lineage import upstream_lineage_multi

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
LOAD_INFO = load_all_mappings_from_dir()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def health():
    return {
        "ok": True,
        "mappings_dir": str(MAPPINGS_DIR.resolve()),
        "loaded_files": LOAD_INFO.get("files", 0),
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


if __name__ == "__main__":
    app.run(debug=True, port=8001)