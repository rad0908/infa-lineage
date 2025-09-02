from lxml import etree as ET
from typing import List, Dict, Tuple
import re
import storage as st

# ---------------------------
# Helpers
# ---------------------------

def _id(*parts: str) -> str:
    return ":".join(p for p in parts if p is not None)


def _aget(el, *names: str):
    for n in names:
        v = el.get(n)
        if v is not None:
            return v
    return None


def _collect_fields(def_el, tags: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
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


# ---------------------------
# Main parser (STRICT)
# ---------------------------

def parse_mapping_xml(xml_path: str) -> str:
    """
    Strict Informatica PowerCenter XML parser.

    Assumptions/guarantees:
    - SOURCE/TARGET live under <FOLDER>. MAPPING references them via <INSTANCE>.
    - INSTANCE typing is based strictly on TYPE/TRANSFORMATION_TYPE (or TRANSFORMATIONTYPE)
      and the referenced folder object name (TRANSFORMATION_NAME / REFOBJECTNAME...).
    - TRANSFORMATION edges are added ONLY from explicit EXPRESSION token references.
      If an OUTPUT has no EXPRESSION, we DO NOT infer any edges.
    - CONNECTOR edges are created ONLY from the XML using FROM*/TO* attributes
      (supports FROMPORT/TOFIELD variants). No synthetic/backfill edges.
    - No fuzzy/substring name logic anywhere.

    Note: This reads the FIRST <MAPPING> in the file.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    folder_el = root.find(".//FOLDER")
    if folder_el is None:
        raise ValueError(f"FOLDER not found in {xml_path}")

    mapping = folder_el.find("./MAPPING") or root.find(".//MAPPING")
    if mapping is None:
        raise ValueError(f"MAPPING not found in {xml_path}")

    mapping_name = mapping.get("NAME") or "(unnamed)"
    folder = folder_el.get("NAME") or "UNKNOWN"
    mapping_id = _id(folder, mapping_name)

    # ---- Folder-level physical defs
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
        folder_sources[sname.upper()] = {
            "fields": fields,
            "db": (s.get("DBDNAME") or "").upper(),
            "schema": (s.get("OWNERNAME") or "").upper(),
            "name": sname,
            "full": _full(s.get("DBDNAME"), s.get("OWNERNAME"), sname),
        }

    for t in folder_el.findall("./TARGET"):
        tname = (t.get("NAME") or "").strip()
        if not tname:
            continue
        fields = _collect_fields(t, ["TARGETFIELD", "FIELD"])
        folder_targets[tname.upper()] = {
            "fields": fields,
            "db": (t.get("DBDNAME") or "").upper(),
            "schema": (t.get("OWNERNAME") or "").upper(),
            "name": tname,
            "full": _full(t.get("DBDNAME"), t.get("OWNERNAME"), tname),
        }

    # ---- Buckets
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
        instances.append({
            "instance_id": inst_id,
            "mapping_id": mapping_id,
            "type": inst_type,
            "name": inst_name,
        })
        return inst_id

    def _add_ports_for_source_instance(inst_id: str, src_key: str):
        meta = folder_sources.get(src_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            ports.append({
                "port_id": _id(inst_id, fname),
                "instance_id": inst_id,
                "name": fname,
                "dtype": dtype,
                "direction": "OUTPUT",
            })
        obj_id = _id("SRC", meta["full"])
        phys_src.append({
            "object_id": obj_id,
            "kind": "SOURCE",
            "db": meta["db"],
            "schema": meta["schema"],
            "name": meta["name"],
            "full_name": meta["full"],
        })
        map_src.append({"mapping_id": mapping_id, "object_id": obj_id})

    def _add_ports_for_target_instance(inst_id: str, tgt_key: str):
        meta = folder_targets.get(tgt_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            ports.append({
                "port_id": _id(inst_id, fname),
                "instance_id": inst_id,
                "name": fname,
                "dtype": dtype,
                "direction": "INPUT",
            })
        obj_id = _id("TGT", meta["full"])
        phys_tgt.append({
            "object_id": obj_id,
            "kind": "TARGET",
            "db": meta["db"],
            "schema": meta["schema"],
            "name": meta["name"],
            "full_name": meta["full"],
        })
        map_tgt.append({"mapping_id": mapping_id, "object_id": obj_id})

    # ---- Transformations (instances + ports + intra-transform edges from EXPRESSION only)
    tx_names = set()
    for t in mapping.findall("./TRANSFORMATION"):
        t_name = t.get("NAME")
        t_type = t.get("TYPE")
        tx_names.add(t_name)
        inst_id = _add_instance(t_name, t_type or "Transformation")

        input_names: List[str] = []
        var_names:   List[str] = []
        outputs:     List[Dict] = []

        for pf in t.findall("./TRANSFORMFIELD"):
            pname = pf.get("NAME")
            if not pname:
                continue
            pdir  = (pf.get("PORTTYPE") or "").upper()
            dtype = pf.get("DATATYPE") or ""
            pid   = _id(inst_id, pname)

            direction = pdir if pdir in ("INPUT", "OUTPUT", "VARIABLE") else "VARIABLE"
            ports.append({
                "port_id": pid,
                "instance_id": inst_id,
                "name": pname,
                "dtype": dtype,
                "direction": direction,
            })

            # Capture actual formula from EXPRESSION (attribute or child tag)
            expr_text = (pf.get("EXPRESSION")
                         or pf.findtext("./EXPRESSION")
                         or pf.findtext("./EXPR")
                         or "")
            expr_name = (pf.get("EXPRESSIONNAME") or pf.get("EXPRESSION_NAME") or "")

            if expr_text and direction == "OUTPUT":
                exprs.append({
                    "port_id": pid,   # attach to OUTPUT port
                    "kind":   "expr",
                    "raw":    expr_text,
                    "meta":   expr_name,
                })

            if direction == "INPUT":
                input_names.append(pname)
            elif direction == "VARIABLE":
                var_names.append(pname)
            elif direction == "OUTPUT":
                outputs.append({"name": pname, "expr_text": expr_text})

        # Record join/filter text for display (metadata only; no edges)
        for ta in t.findall("./TABLEATTRIBUTE"):
            aname = (ta.get("NAME") or "").lower()
            aval  = ta.get("VALUE") or ""
            if not aval:
                continue
            if "join" in aname or aname in ("join condition", "joiner condition"):
                exprs.append({
                    "port_id": _id(inst_id, "__join__"),
                    "kind": "join",
                    "raw": aval,
                    "meta": aname,
                })
            # If needed you can also record filters similarly.

        inputs_ci = {n.lower(): n for n in input_names}
        vars_ci   = {n.lower(): n for n in var_names}

        # STRICT: wire only explicit references found in EXPRESSION
        for od in outputs:
            out_name = od["name"]
            out_pid  = _id(inst_id, out_name)
            expr_txt = od["expr_text"] or ""
            if not expr_txt:
                continue  # do not infer when expression is blank
            tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr_txt)
            refs: List[str] = []
            for tok in tokens:
                tci = tok.lower()
                if tci in inputs_ci:
                    refs.append(inputs_ci[tci])
                elif tci in vars_ci:
                    refs.append(vars_ci[tci])
            for in_name in refs:
                in_pid = _id(inst_id, in_name)
                if in_pid != out_pid:
                    edges.append({"from_port_id": in_pid, "to_port_id": out_pid})

    # ---- INSTANCE binding (STRICT; no name heuristics)
    instance_type_by_name: Dict[str, str] = {}
    instance_refname_by_name: Dict[str, str] = {}

    for inst in mapping.findall(".//INSTANCE"):
        iname   = _aget(inst, "NAME", "INSTANCE_NAME")
        if not iname:
            continue
        rawt    = (_aget(inst, "TYPE", "TRANSFORMATION_TYPE", "TRANSFORMATIONTYPE") or "").strip().lower()
        refname = _aget(inst, "TRANSFORMATION_NAME", "REFOBJECTNAME", "REF_OBJECT_NAME", "REFOBJECT_NAME", "TRANFIRMATION_NAME") or ""
        refU    = refname.upper()

        is_target = (rawt in ("target", "target definition")) or (refU in folder_targets)
        is_source = (rawt in ("source", "source definition")) or (refU in folder_sources)

        if is_target:
            instance_type_by_name[iname] = "Target"
        elif is_source:
            instance_type_by_name[iname] = "Source"
        else:
            instance_type_by_name[iname] = "Transformation"
        instance_refname_by_name[iname] = refname

        # Record Source Qualifier association (metadata only)
        if rawt == "source qualifier":
            assoc = _aget(inst, "ASSOCIATED_SOURCE_INSTANCE", "ASSOCIATEDSOURCEINSTANCE")
            if assoc:
                exprs.append({
                    "port_id": _id(_id(mapping_id, iname), "__associated_source__"),
                    "kind": "assoc_source",
                    "raw": assoc,
                    "meta": "ASSOCIATED_SOURCE_INSTANCE",
                })

    # materialize non-TRANSFORMATION instances and their ports for Source/Target
    for iname, itype in instance_type_by_name.items():
        if iname in tx_names:
            continue
        inst_id = _add_instance(iname, itype)
        if itype == "Source":
            key = (instance_refname_by_name.get(iname, "") or "").upper()
            if key:
                _add_ports_for_source_instance(inst_id, key)
        elif itype == "Target":
            key = (instance_refname_by_name.get(iname, "") or "").upper()
            if key:
                _add_ports_for_target_instance(inst_id, key)

    # ---- CONNECTOR edges (STRICT; accept FROMFIELD/TOFIELD variants)
    for c in mapping.findall(".//CONNECTOR"):
        fi = _aget(c, "FROMINSTANCE", "FROM_INSTANCE", "FROMINSTANCENAME")
        ti = _aget(c, "TOINSTANCE",   "TO_INSTANCE",   "TOINSTANCENAME")
        fp = _aget(c, "FROMPORT", "FROM_FIELD", "FROMFIELD", "FROMPORTNAME", "FROMFIELDNAME")
        tp = _aget(c, "TOPORT",  "TO_FIELD",   "TOFIELD",   "TOPORTNAME",  "TOFIELDNAME")
        if not (fi and fp and ti and tp):
            continue
        from_pid = _id(_id(mapping_id, fi), fp)
        to_pid   = _id(_id(mapping_id, ti), tp)
        edges.append({"from_port_id": from_pid, "to_port_id": to_pid})

    # ---- Persist
    st.upsert("mappings", [{"mapping_id": mapping_id, "name": mapping_name, "folder": folder}], ("mapping_id",))
    st.insert_if_missing("instances", instances, ("instance_id",))
    st.insert_if_missing("ports", ports, ("port_id",))
    st.insert_if_missing("edges", edges, ("from_port_id", "to_port_id"))
    st.insert_if_missing("expressions", exprs, ("port_id", "kind", "raw"))
    st.insert_if_missing("physical_objects", phys_src + phys_tgt, ("object_id",))
    st.insert_if_missing("map_sources", map_src, ("mapping_id", "object_id"))
    st.insert_if_missing("map_targets", map_tgt, ("mapping_id", "object_id"))

    return mapping_id