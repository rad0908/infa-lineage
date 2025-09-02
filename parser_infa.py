
from lxml import etree as ET
from typing import List, Dict
import re
import storage as st

def _id(*parts) -> str:
    return ":".join(parts)

def _collect_fields(def_el, tags):
    out = []
    for tag in tags:
        for f in def_el.findall(f"./{tag}"):
            name = f.get("NAME") or f.get("FIELDNAME") or f.get("COLUMN_NAME")
            if not name:
                continue
            dtype = f.get("DATATYPE") or f.get("TYPE") or ""
            if not dtype:
                prec = f.get("PRECISION"); scale = f.get("SCALE")
                if prec and scale:
                    dtype = f"DECIMAL({prec},{scale})"
            out.append((name, dtype))
    return out

def parse_mapping_xml(xml_path: str) -> str:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    folder_el = root.find(".//FOLDER")
    if folder_el is None:
        raise ValueError(f"FOLDER not found in {xml_path}")

    mapping = folder_el.find("./MAPPING") or root.find(".//MAPPING")
    if mapping is None:
        raise ValueError(f"MAPPING not found in {xml_path}")

    mapping_name = mapping.get("NAME")
    folder = folder_el.get("NAME") or "UNKNOWN"
    mapping_id = _id(folder, mapping_name)

    def _full(db, schema, name):
        db = (db or "").upper(); schema = (schema or "").upper()
        return f"{db}.{schema}.{name}" if db and schema else name

    folder_sources: Dict[str, Dict] = {}
    folder_targets: Dict[str, Dict] = {}

    for s in folder_el.findall("./SOURCE"):
        sname = (s.get("NAME") or "").strip()
        if not sname:
            continue
        fields = _collect_fields(s, ["SOURCEFIELD", "FIELD"])
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
        fields = _collect_fields(t, ["TARGETFIELD", "FIELD"])
        meta = {
            "fields": fields,
            "db": (t.get("DBDNAME") or "").upper(),
            "schema": (t.get("OWNERNAME") or "").upper(),
            "name": tname,
            "full": _full(t.get("DBDNAME"), t.get("OWNERNAME"), tname),
        }
        folder_targets[tname.upper()] = meta

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

    # Transformations & intra-transform edges
    tx_names = set()
    for t in mapping.findall("./TRANSFORMATION"):
        t_name = t.get("NAME"); t_type = t.get("TYPE")
        tx_names.add(t_name)
        inst_id = _add_instance(t_name, t_type)

        input_names: List[str] = []
        output_defs: List[Dict] = []

        for pf in t.findall("./TRANSFORMFIELD"):
            pname = pf.get("NAME"); pdir = (pf.get("PORTTYPE") or "").upper()
            dtype = pf.get("DATATYPE") or ""; pid = _id(inst_id, pname)
            direction = pdir if pdir in ("INPUT", "OUTPUT") else "VARIABLE"
            ports.append({"port_id": pid, "instance_id": inst_id, "name": pname, "dtype": dtype, "direction": direction})

            expr_text = pf.get("EXPRESSION") or pf.get("EXPR") or ""
            if expr_text:
                exprs.append({"port_id": pid, "kind": "expr", "raw": expr_text, "meta": None})

            if direction in ("INPUT", "VARIABLE"):
                input_names.append(pname)
            if direction == "OUTPUT":
                output_defs.append({"name": pname, "expr_text": expr_text})

        for ta in t.findall("./TABLEATTRIBUTE"):
            aname = (ta.get("NAME") or "").lower(); aval = ta.get("VALUE") or ""
            if not aval:
                continue
            kind = None
            if "join" in aname: kind = "join"
            elif "filter" in aname: kind = "filter"
            elif "group" in aname: kind = "groupby"
            elif "lookup" in aname: kind = "lookup"
            if kind:
                pid = _id(inst_id, f"__{kind}__")
                ports.append({"port_id": pid, "instance_id": inst_id, "name": f"__{kind}__", "dtype": "", "direction": "OUTPUT"})
                exprs.append({"port_id": pid, "kind": kind, "raw": aval, "meta": aname})

        for od in output_defs:
            out_name = od["name"]; out_pid = _id(inst_id, out_name)
            refs = set()
            if od["expr_text"]:
                tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", od["expr_text"])
                refs.update({tok for tok in tokens if tok in input_names})
            if not refs:
                refs = set(input_names)
            for in_name in refs:
                in_pid = _id(inst_id, in_name)
                if in_pid != out_pid:
                    edges.append({"from_port_id": in_pid, "to_port_id": out_pid})

    # INSTANCE binding w/ robust attribute handling
    def _aget(el, *names):
        for n in names:
            v = el.get(n)
            if v is not None:
                return v
        return None

    instance_type_by_name: Dict[str, str] = {}
    instance_refname_by_name: Dict[str, str] = {}

    for inst in mapping.findall(".//INSTANCE"):
        iname = _aget(inst, "NAME", "INSTANCE_NAME")
        rawt  = (_aget(inst, "TYPE", "TRANSFORMATION_TYPE", "TRANSFORMATIONTYPE") or "").strip().lower()
        refname = _aget(inst, "TRANSFORMATION_NAME", "REFOBJECTNAME", "REF_OBJECT_NAME", "REFOBJECT_NAME", "TRANFIRMATION_NAME") or iname or ""
        if not iname:
            continue
        refU = (refname or "").upper(); inameU = iname.upper()

        if ("target" in rawt) or (rawt == "target") or (refU in folder_targets) or (inameU in folder_targets):
            instance_type_by_name[iname] = "Target"; instance_refname_by_name[iname] = refname or iname
        elif ("source" in rawt) or (rawt == "source") or (refU in folder_sources) or (inameU in folder_sources):
            instance_type_by_name[iname] = "Source"; instance_refname_by_name[iname] = refname or iname
        else:
            instance_type_by_name[iname] = "Transformation"

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

    # CONNECTORs and backfill endpoints
    connector_insts = set()
    for c in mapping.findall(".//CONNECTOR"):
        if c.get("FROMINSTANCE"): connector_insts.add(c.get("FROMINSTANCE"))
        if c.get("TOINSTANCE"):   connector_insts.add(c.get("TOINSTANCE"))

    existing_insts = {r["name"] for r in instances}
    for iname in connector_insts - existing_insts:
        inameU = iname.upper()
        if inameU in folder_sources:
            inst_id = _add_instance(iname, "Source"); _add_ports_for_source_instance(inst_id, inameU)
        elif inameU in folder_targets:
            inst_id = _add_instance(iname, "Target"); _add_ports_for_target_instance(inst_id, inameU)
        else:
            _add_instance(iname, "Transformation")

    for c in mapping.findall(".//CONNECTOR"):
        fi = c.get("FROMINSTANCE"); fp = c.get("FROMPORT")
        ti = c.get("TOINSTANCE");   tp = c.get("TOPORT")
        if fi and fp and ti and tp:
            from_pid = _id(_id(mapping_id, fi), fp)
            to_pid   = _id(_id(mapping_id, ti), tp)
            edges.append({"from_port_id": from_pid, "to_port_id": to_pid})

    def _ensure_port(port_id: str, direction: str):
        parts = port_id.split(":")
        if len(parts) < 3:
            return
        inst_name = parts[-2]; port_name = parts[-1]
        mapping_id_local = ":".join(parts[:-2])
        inst_id = f"{mapping_id_local}:{inst_name}"
        if not any(i["instance_id"] == inst_id for i in instances):
            instances.append({"instance_id": inst_id, "mapping_id": mapping_id_local, "type": "Transformation", "name": inst_name})
        if not any(p["port_id"] == port_id for p in ports):
            ports.append({"port_id": port_id, "instance_id": inst_id, "name": port_name, "dtype": "", "direction": direction})

    for e in edges:
        _ensure_port(e["to_port_id"],   "INPUT")
        _ensure_port(e["from_port_id"], "OUTPUT")

    st.upsert("mappings", [{"mapping_id": mapping_id, "name": mapping_name, "folder": folder}], ("mapping_id",))
    st.insert_if_missing("instances", instances, ("instance_id",))
    st.insert_if_missing("ports", ports, ("port_id",))
    st.insert_if_missing("edges", edges, ("from_port_id", "to_port_id"))
    st.insert_if_missing("expressions", exprs, ("port_id", "kind"))
    st.insert_if_missing("physical_objects", phys_src + phys_tgt, ("object_id",))
    st.insert_if_missing("map_sources", map_src, ("mapping_id", "object_id"))
    st.insert_if_missing("map_targets", map_tgt, ("mapping_id", "object_id"))

    return mapping_id
