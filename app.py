
from flask import Flask, request, jsonify, render_template
from pathlib import Path
import os
import storage as st
from parser_infa import parse_mapping_xml
from lineage import upstream_lineage_multi

_env_dir = os.environ.get("MAPPINGS_DIR")
if _env_dir and _env_dir.strip():
    MAPPINGS_DIR = Path(_env_dir)
else:
    MAPPINGS_DIR = Path(__file__).resolve().parent / "samples"

app = Flask(__name__)

def _xml_files_in_dir(root: Path):
    if not root.exists():
        return []
    exts = {".xml", ".XML"}
    return sorted([p for p in root.rglob("*") if p.is_file() and p.suffix in exts])

def load_all_mappings_from_dir():
    st.reset_all()
    xmls = _xml_files_in_dir(MAPPINGS_DIR)
    loaded, errors = [], []\n
    for p in xmls:\n
        try:\n
            mid = parse_mapping_xml(str(p))\n
            loaded.append({\"file\": str(p), \"mapping_id\": mid})\n
        except Exception as e:\n
            errors.append({\"file\": str(p), \"error\": str(e)})\n
            print(f\"[load] failed {p}: {e}\")\n
    return {\"dir\": str(MAPPINGS_DIR.resolve()), \"files\": len(xmls), \"loaded\": loaded, \"errors\": errors}\n
\n
LOAD_INFO = load_all_mappings_from_dir()\n
\n
@app.route(\"/\")\n
def index():\n
    return render_template(\"index.html\")\n
\n
@app.route(\"/api/health\")\n
def health():\n
    return {\"ok\": True, \"mappings_dir\": str(MAPPINGS_DIR.resolve()), \"loaded_files\": LOAD_INFO.get(\"files\", 0), \"errors\": LOAD_INFO.get(\"errors\", [])}\n
\n
@app.route(\"/api/reset\", methods=[\"POST\"]) \n
def reset():\n
    global LOAD_INFO\n
    LOAD_INFO = load_all_mappings_from_dir()\n
    return jsonify(LOAD_INFO)\n
\n
@app.route(\"/api/lookup\")\n
def lookup():\n
    field = request.args.get(\"field\", \"\")\n
    rows = upstream_lineage_multi(field)\n
    return jsonify(rows)\n
\n
@app.route(\"/api/debug/mappings\")\n
def debug_mappings():\n
    return jsonify(st.all_rows(\"mappings\"))\n
\n
@app.route(\"/api/debug/targets\")\n
def debug_targets():\n
    like = (request.args.get(\"like\", \"\") or \"\").lower().replace(\"_\", \"\")\n
    rows = []\n
    ports = st.all_rows(\"ports\")\n
    insts = {r[\"instance_id\"]: r for r in st.all_rows(\"instances\")}\n
    maps  = {r[\"mapping_id\"]: r for r in st.all_rows(\"mappings\")}\n
    edges = st.all_rows(\"edges\")\n
    has_out = {e[\"from_port_id\"] for e in edges}\n
    for p in ports:\n
        inst = insts.get(p[\"instance_id\"]) \n
        if not inst: continue\n
        is_targetish = (inst.get(\"type\") == \"Target\") or (p[\"direction\"] == \"INPUT\" and p[\"port_id\"] not in has_out)\n
        if not is_targetish: continue\n
        norm = p[\"name\"].lower().replace(\"_\", \"\")\n
        if like in norm if like else True:\n
            rows.append({\n
                \"mapping\": maps[inst[\"mapping_id\"]][\"name\"],\n
                \"target\": inst[\"name\"],\n
                \"column\": p[\"name\"],\n
                \"port_id\": p[\"port_id\"]\n
            })\n
    return jsonify(rows)\n
\n
@app.route(\"/api/debug/edges\")\n
def debug_edges():\n
    to_like = (request.args.get(\"to_like\",\"\") or \"\").lower()\n
    fr_like = (request.args.get(\"from_like\",\"\") or \"\").lower()\n
    edges = st.all_rows(\"edges\")\n
    rows = []\n
    for e in edges:\n
        to_ok = to_like in e[\"to_port_id\"].lower() if to_like else True\n
        fr_ok = fr_like in e[\"from_port_id\"].lower() if fr_like else True\n
        if to_ok and fr_ok:\n
            rows.append(e)\n
    return {\"count\": len(rows), \"sample\": rows[:100]}\n
\n
if __name__ == \"__main__\":\n
    app.run(debug=True, port=8000)\n
