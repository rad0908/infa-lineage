from lxml import etree as ET
from typing import List, Dict
import re
import storage as st


def _id(*parts) -> str:
    return ":".join(parts)


def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def parse_mapping_xml(xml_path: str) -> str:
    """
    Parse a PowerCenter XML where <SOURCE>/<TARGET> may be defined at the FOLDER level
    (outside the MAPPING) and referenced via <INSTANCE> inside the MAPPING.

    - Creates instances and ports for Transformations, Sources, Targets.
    - Adds intra-transformation edges (inputs -> outputs) based on expressions or fallback.
    - Adds inter-instance edges from <CONNECTOR>.
    - Persists physical objects and mapping<->object links.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    folder_el = root.find(".//FOLDER")
    if folder_el is None:
        raise ValueError(f"FOLDER not found in {xml_path}")

    mapping = folder_el.find("./MAPPING")
    if mapping is None:
        # Some exports put MAPPING directly under root
        mapping = root.find(".//MAPPING")
    if mapping is None:
        raise ValueError(f"MAPPING not found in {xml_path}")

    mapping_name = mapping.get("NAME")
    folder = folder_el.get("NAME") or "UNKNOWN"
    mapping_id = _id(folder, mapping_name)

    # ------------------------------------------------------------
    # Collect folder-level SOURCE/TARGET definitions
    # ------------------------------------------------------------
    def _full(db, schema, name):
        db = (db or "").upper(); schema = (schema or "").upper()
        return f"{db}.{schema}.{name}" if db and schema else name

    folder_sources: Dict[str, Dict] = {}
    folder_targets: Dict[str, Dict] = {}

    for s in folder_el.findall("./SOURCE"):
        sname = (s.get("NAME") or "").strip()
        if not sname:
            continue
        fields = [(f.get("NAME"), f.get("DATATYPE") or "") for f in s.findall("./FIELD")]
        meta = {
            "fields": fields,
            "db": (s.get("DBDNAME") or "").upper(),
            "schema": (s.get("OWNERNAME") or "").upper(),
            "name": sname,
            "full": _full(s.get("DBDNAME"), s.get("OWNERNAME"), sname),
        }
        folder_sources[sname.upper()] = meta

    for t in folder_el.findall("./TARGET"):
        tname = (t.get("NAME") or "").strip()
        if not tname:
            continue
        fields = [(f.get("NAME"), f.get("DATATYPE") or "") for f in t.findall("./FIELD")]
        meta = {
            "fields": fields,
            "db": (t.get("DBDNAME") or "").upper(),
            "schema": (t.get("OWNERNAME") or "").upper(),
            "name": tname,
            "full": _full(t.get("DBDNAME"), t.get("OWNERNAME"), tname),
        }
        folder_targets[tname.upper()] = meta

    # ------------------------------------------------------------
    # Buckets to persist
    # ------------------------------------------------------------
    instances: List[Dict] = []
    ports: List[Dict] = []
    edges: List[Dict] = []
    exprs: List[Dict] = []
    phys_src: List[Dict] = []
    phys_tgt: List[Dict] = []
    map_src: List[Dict] = []
    map_tgt: List[Dict] = []

    def _add_instance(inst_name: str, inst_type: str) -> str:
        inst_id = _id(mapping_id, inst_name)
        instances.append({"instance_id": inst_id, "mapping_id": mapping_id, "type": inst_type, "name": inst_name})
        return inst_id

    def _add_ports_for_source_instance(inst_id: str, src_key: str):
        meta = folder_sources.get(src_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            ports.append({"port_id": _id(inst_id, fname), "instance_id": inst_id,
                          "name": fname, "dtype": dtype, "direction": "OUTPUT"})
        obj_id = _id("SRC", meta["full"])
        phys_src.append({"object_id": obj_id, "kind": "SOURCE", "db": meta["db"], "schema": meta["schema"],
                         "name": meta["name"], "full_name": meta["full"]})
        map_src.append({"mapping_id": mapping_id, "object_id": obj_id})

    def _add_ports_for_target_instance(inst_id: str, tgt_key: str):
        meta = folder_targets.get(tgt_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            ports.append({"port_id": _id(inst_id, fname), "instance_id": inst_id,
                          "name": fname, "dtype": dtype, "direction": "INPUT"})
        obj_id = _id("TGT", meta["full"])
        phys_tgt.append({"object_id": obj_id, "kind": "TARGET", "db": meta["db"], "schema": meta["schema"],
                         "name": meta["name"], "full_name": meta["full"]})
        map_tgt.append({"mapping_id": mapping_id, "object_id": obj_id})

    # ------------------------------------------------------------
    # TRANSFORMATIONS (in-mapping) + intra-transform edges
    # ------------------------------------------------------------
    tx_names = set()
    for t in mapping.findall("./TRANSFORMATION"):
        t_name = t.get("NAME")
        t_type = t.get("TYPE")
        tx_names.add(t_name)
        inst_id = _add_instance(t_name, t_type)

        input_names: List[str] = []
        output_defs: List[Dict] = []  # [{name, expr_text}]

        for pf in t.findall("./TRANSFORMFIELD"):
            pname = pf.get("NAME")
            pdir = (pf.get("PORTTYPE") or "").upper()  # INPUT / OUTPUT / VARIABLE
            dtype = pf.get("DATATYPE") or ""
            pid = _id(inst_id, pname)

            direction = pdir if pdir in ("INPUT", "OUTPUT") else "VARIABLE"
            ports.append({"port_id": pid, "instance_id": inst_id, "name": pname, "dtype": dtype, "direction": direction})

            expr_text = pf.get("EXPRESSION") or pf.get("EXPR") or ""
            if expr_text:
                exprs.append({"port_id": pid, "kind": "expr", "raw": expr_text, "meta": None})

            if direction in ("INPUT", "VARIABLE"):
                input_names.append(pname)
            if direction == "OUTPUT":
                output_defs.append({"name": pname, "expr_text": expr_text})

        # Conditions from TABLEATTRIBUTE (join/filter/lookup/group)
        for ta in t.findall("./TABLEATTRIBUTE"):
            aname = (ta.get("NAME") or "").lower()
            aval = ta.get("VALUE") or ""
            if not aval:
                continue
            kind = None
            if "join" in aname:
                kind = "join"
            elif "filter" in aname:
                kind = "filter"
            elif "group" in aname:
                kind = "groupby"
            elif "lookup" in aname:
                kind = "lookup"
            if kind:
                pid = _id(inst_id, f"__{kind}__")
                ports.append({"port_id": pid, "instance_id": inst_id, "name": f"__{kind}__", "dtype": "", "direction": "OUTPUT"})
                exprs.append({"port_id": pid, "kind": kind, "raw": aval, "meta": aname})

        # intra-transform edges (inputs ➜ outputs)
        for od in output_defs:
            out_name = od["name"]
            out_pid = _id(inst_id, out_name)
            refs = set()
            if od["expr_text"]:
                tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", od["expr_text"])
                for tok in tokens:
                    if tok in input_names:
                        refs.add(tok)
            if not refs:
                refs = set(input_names)
            for in_name in refs:
                in_pid = _id(inst_id, in_name)
                if in_pid != out_pid:
                    edges.append({"from_port_id": in_pid, "to_port_id": out_pid})

    # ------------------------------------------------------------
    # MAPPING/INSTANCE: bind instance names to folder-level Source/Target
    # ------------------------------------------------------------
    instance_type_by_name: Dict[str, str] = {}  # name -> "Source"/"Target"/"Transformation"
    instance_refname_by_name: Dict[str, str] = {}  # instance name -> folder object name

    for inst in mapping.findall("./INSTANCE"):
        iname = inst.get("NAME") or inst.get("INSTANCE_NAME")
        rawt = (inst.get("TYPE") or inst.get("TRANSFORMATION_TYPE") or inst.get("TRANSFORMATIONTYPE") or "").lower()
        refname = (inst.get("TRANSFORMATION_NAME") or inst.get("REFOBJECTNAME") or inst.get("REF_OBJECT_NAME") or iname or "")
        if not iname:
            continue
        refU = (refname or "").upper()
        inameU = iname.upper()

        if ("target" in rawt) or ("target definition" in rawt) or (refU in folder_targets) or (inameU in folder_targets):
            instance_type_by_name[iname] = "Target"
            instance_refname_by_name[iname] = refname or iname
        elif ("source" in rawt) or ("source definition" in rawt) or (refU in folder_sources) or (inameU in folder_sources):
            instance_type_by_name[iname] = "Source"
            instance_refname_by_name[iname] = refname or iname
        else:
            instance_type_by_name[iname] = "Transformation"

    # Create Source/Target instances from INSTANCE bindings (skip if same name was a Transformation)
    for iname, itype in instance_type_by_name.items():
        if iname in tx_names:
            continue
        if itype == "Source":
            inst_id = _add_instance(iname, "Source")
            key = (instance_refname_by_name.get(iname, iname) or iname).upper()
            _add_ports_for_source_instance(inst_id, key)
        elif itype == "Target":
            inst_id = _add_instance(iname, "Target")
            key = (instance_refname_by_name.get(iname, iname) or iname).upper()
            _add_ports_for_target_instance(inst_id, key)

    # Fallback: connectors mention instances not declared in <INSTANCE>
    connector_insts = set()
    for c in mapping.findall("./CONNECTOR"):
        if c.get("FROMINSTANCE"):
            connector_insts.add(c.get("FROMINSTANCE"))
        if c.get("TOINSTANCE"):
            connector_insts.add(c.get("TOINSTANCE"))

    existing_insts = {r["name"] for r in instances}
    for iname in connector_insts - existing_insts:
        inameU = iname.upper()
        if inameU in folder_sources:
            inst_id = _add_instance(iname, "Source")
            _add_ports_for_source_instance(inst_id, inameU)
        elif inameU in folder_targets:
            inst_id = _add_instance(iname, "Target")
            _add_ports_for_target_instance(inst_id, inameU)
        # else: assume it was a transformation already created

    # Safety net: backfill ports for any Source/Target instance that ended up empty
    have_ports_by_inst = {p["instance_id"] for p in ports}
    for inst in list(instances):
        if inst["instance_id"] in have_ports_by_inst:
            continue
        if inst["type"] == "Target":
            key = (instance_refname_by_name.get(inst["name"], inst["name"]) or inst["name"]).upper()
            _add_ports_for_target_instance(inst["instance_id"], key)
        elif inst["type"] == "Source":
            key = (instance_refname_by_name.get(inst["name"], inst["name"]) or inst["name"]).upper()
            _add_ports_for_source_instance(inst["instance_id"], key)

    # ------------------------------------------------------------
    # CONNECTORs (between instances)
    # ------------------------------------------------------------
    for c in mapping.findall("./CONNECTOR"):
        fi = c.get("FROMINSTANCE"); fp = c.get("FROMPORT")
        ti = c.get("TOINSTANCE");   tp = c.get("TOPORT")
        if fi and fp and ti and tp:
            from_pid = _id(_id(mapping_id, fi), fp)
            to_pid   = _id(_id(mapping_id, ti), tp)
            edges.append({"from_port_id": from_pid, "to_port_id": to_pid})

    # ------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------
    st.upsert("mappings", [{"mapping_id": mapping_id, "name": mapping_name, "folder": folder}], ("mapping_id",))
    st.insert_if_missing("instances", instances, ("instance_id",))
    st.insert_if_missing("ports", ports, ("port_id",))
    st.insert_if_missing("edges", edges, ("from_port_id", "to_port_id"))
    st.insert_if_missing("expressions", exprs, ("port_id", "kind"))
    st.insert_if_missing("physical_objects", phys_src + phys_tgt, ("object_id",))
    st.insert_if_missing("map_sources", map_src, ("mapping_id", "object_id"))
    st.insert_if_missing("map_targets", map_tgt, ("mapping_id", "object_id"))

    return mapping_id