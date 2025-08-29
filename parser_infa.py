from lxml import etree as ET
from typing import List, Dict
import re
import storage as st

def _id(*parts) -> str:
    return ":".join(parts)

def parse_mapping_xml(xml_path: str) -> str:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    mapping = root.find(".//MAPPING")
    if mapping is None:
        raise ValueError(f"MAPPING not found in {xml_path}")

    mapping_name = mapping.get("NAME")
    folder_el = root.find(".//FOLDER")
    folder = folder_el.get("NAME") if folder_el is not None else "UNKNOWN"
    mapping_id = _id(folder, mapping_name)

    instances: List[Dict] = []
    ports: List[Dict] = []
    edges: List[Dict] = []
    exprs: List[Dict] = []
    phys_src: List[Dict] = []
    phys_tgt: List[Dict] = []
    map_src: List[Dict] = []
    map_tgt: List[Dict] = []

    # TRANSFORMATIONS + INTRA EDGES
    for t in mapping.findall("./TRANSFORMATION"):
        t_name = t.get("NAME")
        t_type = t.get("TYPE")
        inst_id = _id(mapping_id, t_name)
        instances.append({"instance_id": inst_id, "mapping_id": mapping_id, "type": t_type, "name": t_name})

        input_names: List[str] = []
        output_defs: List[Dict] = []  # [{name, expr_text}]

        for pf in t.findall("./TRANSFORMFIELD"):
            pname = pf.get("NAME")
            pdir  = (pf.get("PORTTYPE") or "").upper()
            dtype = pf.get("DATATYPE") or ""
            pid = _id(inst_id, pname)

            if pdir not in ("INPUT", "OUTPUT"):
                direction = "VARIABLE"
            else:
                direction = pdir

            ports.append({"port_id": pid, "instance_id": inst_id, "name": pname, "dtype": dtype, "direction": direction})

            expr_text = pf.get("EXPRESSION") or pf.get("EXPR") or ""
            if expr_text:
                exprs.append({"port_id": pid, "kind": "expr", "raw": expr_text, "meta": None})

            if direction in ("INPUT", "VARIABLE"):
                input_names.append(pname)
            if direction == "OUTPUT":
                output_defs.append({"name": pname, "expr_text": expr_text})

        for ta in t.findall("./TABLEATTRIBUTE"):
            aname = (ta.get("NAME") or "").lower()
            aval  = ta.get("VALUE") or ""
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

        for od in output_defs:
            out_name = od["name"]
            out_pid  = _id(inst_id, out_name)
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

    # SOURCE / TARGET blocks
    for s in mapping.findall("./SOURCE"):
        sname = s.get("NAME")
        inst_id = _id(mapping_id, sname)
        instances.append({"instance_id": inst_id, "mapping_id": mapping_id, "type": "Source", "name": sname})
        for f in s.findall("./FIELD"):
            pname = f.get("NAME"); dtype = f.get("DATATYPE") or ""
            ports.append({"port_id": _id(inst_id, pname), "instance_id": inst_id, "name": pname, "dtype": dtype, "direction": "OUTPUT"})
        db = (s.get("DBDNAME") or "").upper(); schema = (s.get("OWNERNAME") or "").upper()
        full = f"{db}.{schema}.{sname}" if db and schema else sname
        obj_id = _id("SRC", full)
        phys_src.append({"object_id": obj_id, "kind":"SOURCE","db":db,"schema":schema,"name":sname,"full_name":full})
        map_src.append({"mapping_id": mapping_id, "object_id": obj_id})

    for t in mapping.findall("./TARGET"):
        tname = t.get("NAME")
        inst_id = _id(mapping_id, tname)
        instances.append({"instance_id": inst_id, "mapping_id": mapping_id, "type": "Target", "name": tname})
        for f in t.findall("./FIELD"):
            pname = f.get("NAME"); dtype = f.get("DATATYPE") or ""
            ports.append({"port_id": _id(inst_id, pname), "instance_id": inst_id, "name": pname, "dtype": dtype, "direction": "INPUT"})
        db = (t.get("DBDNAME") or "").upper(); schema = (t.get("OWNERNAME") or "").upper()
        full = f"{db}.{schema}.{tname}" if db and schema else tname
        obj_id = _id("TGT", full)
        phys_tgt.append({"object_id": obj_id, "kind":"TARGET","db":db,"schema":schema,"name":tname,"full_name":full})
        map_tgt.append({"mapping_id": mapping_id, "object_id": obj_id})

    for c in mapping.findall("./CONNECTOR"):
        fi = c.get("FROMINSTANCE"); fp = c.get("FROMPORT")
        ti = c.get("TOINSTANCE");   tp = c.get("TOPORT")
        if fi and fp and ti and tp:
            from_pid = _id(_id(mapping_id, fi), fp)
            to_pid   = _id(_id(mapping_id, ti), tp)
            edges.append({"from_port_id": from_pid, "to_port_id": to_pid})

    st.upsert("mappings", [{"mapping_id": mapping_id, "name": mapping_name, "folder": folder}], ("mapping_id",))
    st.insert_if_missing("instances", instances, ("instance_id",))
    st.insert_if_missing("ports", ports, ("port_id",))
    st.insert_if_missing("edges", edges, ("from_port_id","to_port_id"))
    st.insert_if_missing("expressions", exprs, ("port_id","kind"))
    st.insert_if_missing("physical_objects", phys_src + phys_tgt, ("object_id",))
    st.insert_if_missing("map_sources", map_src, ("mapping_id","object_id"))
    st.insert_if_missing("map_targets", map_tgt, ("mapping_id","object_id"))

    return mapping_id
