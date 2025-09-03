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

        # --- Folder-level MAPPLets ---
    folder_mapplets: Dict[str, Dict] = {}  # KEY: MAPPLET NAME (UPPER) -> def

    def _parse_mapplet_def(mp_el) -> Dict:
        """Extract a reusable mapplet definition from <MAPPLET>."""
        mp_name = (mp_el.get("NAME") or "").strip()
        trans = []       # [{name,type, fields:[{name,dir,dtype,expr}], is_boundary:bool}]
        edges_intra = [] # [{'from': (inst,port), 'to': (inst,port)}]
        exprs_local = [] # [{'inst':..., 'port':..., 'raw':...}]

        # Identify boundary transforms by TYPE
        boundary_in_names = set()
        boundary_out_names = set()

        for t in mp_el.findall("./TRANSFORMATION"):
            t_name = t.get("NAME") or ""
            t_type = (t.get("TYPE") or "").strip()
            is_in  = t_type.lower() == "mapplet input"
            is_out = t_type.lower() == "mapplet output"
            if is_in:  boundary_in_names.add(t_name)
            if is_out: boundary_out_names.add(t_name)

            fields = []
            inputs_ci, vars_ci, outputs = {}, {}, []
            for pf in t.findall("./TRANSFORMFIELD"):
                pname = pf.get("NAME")
                if not pname: 
                    continue
                pdir  = (pf.get("PORTTYPE") or "").upper()
                dtype = pf.get("DATATYPE") or ""
                fields.append({"name": pname, "dir": pdir, "dtype": dtype})

                # capture expressions for OUTPUT and VARIABLE
                expr_text = (
                    pf.get("EXPRESSION") or
                    (pf.findtext("./EXPRESSION") or pf.findtext("./EXPR")) or
                    pf.get("EXPRESSIONVALUE") or pf.get("EXPRVALUE") or pf.get("VALUE") or
                    ""
                )
                if expr_text and pdir in ("OUTPUT", "VARIABLE"):
                    exprs_local.append({"inst": t_name, "port": pname, "raw": expr_text})

                if pdir == "INPUT":
                    inputs_ci[pname.lower()] = pname
                elif pdir == "VARIABLE":
                    vars_ci[pname.lower()] = pname
                elif pdir == "OUTPUT":
                    outputs.append({"name": pname, "expr_text": expr_text})

            # Strict intra-transform edges from OUTPUT expr tokens
            import re as _re
            for od in outputs:
                if not od["expr_text"]:
                    continue
                tokens = _re.findall(r"[A-Za-z_][A-Za-z0-9_]*", od["expr_text"])
                for tok in tokens:
                    tci = tok.lower()
                    if tci in inputs_ci:
                        edges_intra.append({"from": (t_name, inputs_ci[tci]), "to": (t_name, od["name"])})
                    elif tci in vars_ci:
                        edges_intra.append({"from": (t_name, vars_ci[tci]), "to": (t_name, od["name"])})

            trans.append({"name": t_name, "type": t_type, "fields": fields, "is_boundary": is_in or is_out})

        # Mapplet-level connectors
        for c in mp_el.findall(".//CONNECTOR"):
            fi = c.get("FROMINSTANCE") or c.get("FROM_INSTANCE") or c.get("FROMINSTANCENAME")
            ti = c.get("TOINSTANCE")   or c.get("TO_INSTANCE")   or c.get("TOINSTANCENAME")
            fp = c.get("FROMPORT") or c.get("FROM_FIELD") or c.get("FROMFIELD") or c.get("FROMPORTNAME") or c.get("FROMFIELDNAME")
            tp = c.get("TOPORT")  or c.get("TO_FIELD")   or c.get("TOFIELD")   or c.get("TOPORTNAME")  or c.get("TOFIELDNAME")
            if fi and ti and fp and tp:
                edges_intra.append({"from": (fi, fp), "to": (ti, tp)})

        # external port names = fields of boundary transforms
        in_ports  = sorted({ f["name"] for t in trans if t["name"] in boundary_in_names  for f in t["fields"] if f["name"] })
        out_ports = sorted({ f["name"] for t in trans if t["name"] in boundary_out_names for f in t["fields"] if f["name"] })

        return {
            "name": mp_name,
            "transforms": trans,
            "edges": edges_intra,
            "exprs": exprs_local,
            "in_ports": in_ports,
            "out_ports": out_ports,
            "boundary_in": list(boundary_in_names),
            "boundary_out": list(boundary_out_names),
        }

    # collect all mapplets under the folder
    for mp in folder_el.findall("./MAPPLET"):
        nameU = (mp.get("NAME") or "").strip().upper()
        if not nameU: 
            continue
        folder_mapplets[nameU] = _parse_mapplet_def(mp)



    # ---- Buckets
    instances: List[Dict] = []
    ports: List[Dict] = []
    edges: List[Dict] = []
    exprs: List[Dict] = []
    phys_src: List[Dict] = []
    phys_tgt: List[Dict] = []
    map_src: List[Dict] = []
    map_tgt: List[Dict] = []
    inst_phys: List[Dict] = []     # maps an instance to its physical object
    sq_assoc:  List[Dict] = []     # maps an SQ instance to one or more associated source instance names


    def _add_instance(inst_name: str, inst_type: str) -> str:
        inst_id = _id(mapping_id, inst_name)
        instances.append({
            "instance_id": inst_id,
            "mapping_id": mapping_id,
            "type": inst_type,
            "name": inst_name,
        })
        return inst_id
    
    def _add_port(inst_id: str, name: str, direction: str, dtype: str = ""):
        ports.append({
            "port_id": _id(inst_id, name),
            "instance_id": inst_id,
            "name": name,
            "dtype": dtype,
            "direction": direction,
        })

    def _expand_mapplet_instance(inst_id: str, inst_name: str, mapplet_ref_upper: str):
        """
        Inline a mapplet definition into the current mapping namespace.
        - Create internal instances as <mapping_id>:<inst_name>.<inner_name>
        - Add inner ports/edges/expressions
        - Add external ports on the mapplet instance (INPUT/OUTPUT) so mapping-level connectors can attach
        - Bridge edges between external mapplet ports and boundary transforms inside
        """
        mpdef = folder_mapplets.get(mapplet_ref_upper)
        if not mpdef:
            return

        # 1) materialize inner transforms
        inner_name_to_inst_id = {}
        for t in mpdef["transforms"]:
            inner_name = t["name"]
            inner_type = t["type"] or "Transformation"
            prefixed_name = f"{inst_name}.{inner_name}"
            prefixed_inst_id = _add_instance(prefixed_name, inner_type)
            inner_name_to_inst_id[inner_name] = prefixed_inst_id

            # ports
            for f in t["fields"]:
                # Mapplet Input/Output keep their field directions as exported
                direction = f.get("dir") or "VARIABLE"
                _add_port(prefixed_inst_id, f["name"], direction, f.get("dtype",""))

        # 2) expressions on inner ports
        for ex in mpdef["exprs"]:
            pref_inst_id = inner_name_to_inst_id.get(ex["inst"])
            if not pref_inst_id:
                continue
            exprs.append({
                "port_id": _id(pref_inst_id, ex["port"]),
                "kind": "expr",
                "raw": ex["raw"],
                "meta": None,
            })

        # 3) inner edges (expressions + connectors)
        for ed in mpdef["edges"]:
            fi, fp = ed["from"]; ti, tp = ed["to"]
            fi_id = inner_name_to_inst_id.get(fi); ti_id = inner_name_to_inst_id.get(ti)
            if not (fi_id and ti_id):
                continue
            edges.append({"from_port_id": _id(fi_id, fp), "to_port_id": _id(ti_id, tp)})

        # 4) external ports on the mapplet instance (so mapping connectors can attach to them)
        #    INPUTS are external inputs; OUTPUTS are external outputs
        for p in mpdef["in_ports"]:
            _add_port(inst_id, p, "INPUT", "")
        for p in mpdef["out_ports"]:
            _add_port(inst_id, p, "OUTPUT", "")

        # 5) bridge: external INPUT  -> boundary IN (inside)
        #            boundary OUT (inside) -> external OUTPUT
        # use the first boundary transform of each kind (common case: single in/out transform)
        in_boundaries  = mpdef["boundary_in"]
        out_boundaries = mpdef["boundary_out"]
        in_b_inst  = inner_name_to_inst_id.get(in_boundaries[0])  if in_boundaries else None
        out_b_inst = inner_name_to_inst_id.get(out_boundaries[0]) if out_boundaries else None

        if in_b_inst:
            for p in mpdef["in_ports"]:
                edges.append({
                    "from_port_id": _id(inst_id, p),       # external -> internal
                    "to_port_id":   _id(in_b_inst, p),
                })
        if out_b_inst:
            for p in mpdef["out_ports"]:
                edges.append({
                    "from_port_id": _id(out_b_inst, p),    # internal -> external
                    "to_port_id":   _id(inst_id, p),
                })


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
        inst_phys.append({"instance_id": inst_id, "object_id": obj_id, "role": "Source"})


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
        inst_phys.append({"instance_id": inst_id, "object_id": obj_id, "role": "Target"})


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

            if expr_text and direction in ("OUTPUT", "VARIABLE"):
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
        # If this is a Source Qualifier, collect associated sources (attr or child elements)
        if (rawt or "").strip().lower() == "source qualifier":
            # attribute form
            assoc_attr = _aget(inst, "ASSOCIATED_SOURCE_INSTANCE", "ASSOCIATEDSOURCEINSTANCE")
            if assoc_attr:
                sq_assoc.append({
                    "mapping_id": mapping_id,
                    "sq_instance_id": _id(mapping_id, iname),
                    "source_instance_name": assoc_attr.strip()
                })
            # child element(s) form
            for child in inst.findall("./ASSOCIATED_SOURCE_INSTANCE"):
                nm = (child.get("NAME") or child.get("INSTANCE") or (child.text or "")).strip()
                if nm:
                    sq_assoc.append({
                        "mapping_id": mapping_id,
                        "sq_instance_id": _id(mapping_id, iname),
                        "source_instance_name": nm
                    })


        is_target = (rawt in ("target", "target definition")) or (refU in folder_targets)
        is_source = (rawt in ("source", "source definition")) or (refU in folder_sources)
        is_mapplet = (rawt == "mapplet") or ((refname or "").upper() in folder_mapplets)

        if is_target:
            instance_type_by_name[iname] = "Target"
        elif is_source:
            instance_type_by_name[iname] = "Source"
        elif is_mapplet:
            instance_type_by_name[iname] = "Transformation"
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
        # Expand mapplet internals (if this instance references a folder-level MAPPLET)
        refU = (instance_refname_by_name.get(iname, "") or "").upper()
        rawt_lower = (rawt or "").lower()
        if (rawt_lower == "mapplet") or (refU in folder_mapplets):
            _expand_mapplet_instance(inst_id, iname, refU)

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
    st.insert_if_missing("instance_phys", inst_phys, ("instance_id", "object_id"))
    st.insert_if_missing("sq_assoc", sq_assoc, ("mapping_id", "sq_instance_id", "source_instance_name"))


    return mapping_id