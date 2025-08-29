from flask import Flask, request, jsonify, render_template
from pathlib import Path
import os
import storage as st
from parser_infa import parse_mapping_xml
from lineage import upstream_lineage_multi, build_crosslinks_deterministic

# Read all XML mappings from this folder at startup
MAPPINGS_DIR = Path(os.environ.get("MAPPINGS_DIR", "samples"))

app = Flask(__name__)

def _xml_files_in_dir(root: Path):
    """Return all XML files under root (recursive), case-insensitive extension."""
    if not root.exists():
        return []
    exts = {".xml", ".XML"}
    return sorted([p for p in root.rglob("*") if p.is_file() and p.suffix in exts])

def load_all_mappings_from_dir():
    """Reset flat-file tables and (re)load every .xml in MAPPINGS_DIR (recursively)."""
    st.reset_all()
    xmls = _xml_files_in_dir(MAPPINGS_DIR)
    loaded = []
    errors = []
    for p in xmls:
        try:
            mid = parse_mapping_xml(str(p))
            loaded.append({"file": str(p), "mapping_id": mid})
        except Exception as e:
            errors.append({"file": str(p), "error": str(e)})
            print(f"[load] failed {p}: {e}")
    build_crosslinks_deterministic()
    return {"dir": str(MAPPINGS_DIR.resolve()), "files": len(xmls), "loaded": loaded, "errors": errors}

# Load once on startup
LOAD_INFO = load_all_mappings_from_dir()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/health")
def health():
    return {"ok": True, "mappings_dir": str(MAPPINGS_DIR.resolve()), "loaded_files": LOAD_INFO.get("files", 0), "errors": LOAD_INFO.get("errors", [])}

# No /api/ingest endpoint — mappings come from folder
@app.route("/api/reset", methods=["POST"])
def reset():
    """Force a reload from the configured folder."""
    global LOAD_INFO
    LOAD_INFO = load_all_mappings_from_dir()
    return jsonify(LOAD_INFO)

@app.route("/api/lookup")
def lookup():
    field = request.args.get("field", "")
    rows = upstream_lineage_multi(field)
    return jsonify(rows)

# === Debug helpers to verify what's loaded ===
@app.route("/api/debug/mappings")
def debug_mappings():
    return jsonify(st.all_rows("mappings"))

@app.route("/api/debug/targets")
def debug_targets():
    like = (request.args.get("like", "") or "").lower().replace("_", "")
    rows = []
    ports = st.all_rows("ports")
    insts = {r["instance_id"]: r for r in st.all_rows("instances")}
    maps  = {r["mapping_id"]: r for r in st.all_rows("mappings")}
    for p in ports:
        inst = insts.get(p["instance_id"])
        if not inst or inst["type"] != "Target":
            continue
        norm = p["name"].lower().replace("_", "")
        if like in norm if like else True:
            rows.append({
                "mapping": maps[inst["mapping_id"]]["name"],
                "target": inst["name"],
                "column": p["name"],
                "port_id": p["port_id"]
            })
    return jsonify(rows)

if __name__ == "__main__":
    app.run(debug=True, port=8000)
