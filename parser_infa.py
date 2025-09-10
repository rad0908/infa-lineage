# parser_infa.py
from lxml import etree as ET
from typing import List, Dict, Tuple
import re
import storage as st

# ---- Optional SQL parser (sqlglot). Script works without it; edges from SQL are skipped if missing.
try:
    import sqlglot as _sg
    from sqlglot import exp as _sge
    _HAS_SQLGLOT = True
except Exception:  # pragma: no cover
    _HAS_SQLGLOT = False

# ==========================
# Small helpers
# ==========================

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


def _expr_text_from_port(pf) -> str:
    val = pf.get("EXPRESSION")
    if val:
        return val
    node = pf.find("./EXPRESSION") or pf.find("./EXPR")
    if node is not None:
        if (node.text or "").strip():
            return node.text
        v = node.get("VALUE")
        if v:
            return v
    for attr in ("EXPRESSIONVALUE", "EXPRVALUE", "VALUE"):
        v = pf.get(attr)
        if v:
            return v
    return ""

# ==========================
# Mapplet support
# ==========================

def _parse_mapplet_def(mp_el) -> Dict:
    """Extract a reusable mapplet definition from <MAPPLET>."""
    trans = []        # [{name,type, fields:[{name,dir,dtype}], is_boundary:bool}]
    edges_intra = []  # [{'from': (inst,port), 'to': (inst,port)}]
    exprs_local = []  # [{'inst':..., 'port':..., 'raw':...}]

    boundary_in_names:  set = set()
    boundary_out_names: set = set()

    for t in mp_el.findall("./TRANSFORMATION"):
        t_name = t.get("NAME") or ""
        t_type = (t.get("TYPE") or "").strip().lower()
        is_in  = ("mapplet" in t_type) and ("input" in t_type)
        is_out = ("mapplet" in t_type) and ("output" in t_type)
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

            expr_text = _expr_text_from_port(pf)
            if expr_text and pdir in ("OUTPUT", "VARIABLE"):
                exprs_local.append({"inst": t_name, "port": pname, "raw": expr_text})

            if pdir == "INPUT":
                inputs_ci[pname.lower()] = pname
            elif pdir == "VARIABLE":
                vars_ci[pname.lower()] = pname
            elif pdir == "OUTPUT":
                outputs.append({"name": pname, "expr_text": expr_text})

        # STRICT intra-transform edges from OUTPUT expr tokens
        for od in outputs:
            if not od["expr_text"]:
                continue
            tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", od["expr_text"]) 
            for tok in tokens:
                tci = tok.lower()
                if tci in inputs_ci:
                    edges_intra.append({"from": (t_name, inputs_ci[tci]), "to": (t_name, od["name"])})
                elif tci in vars_ci:
                    edges_intra.append({"from": (t_name, vars_ci[tci]), "to": (t_name, od["name"])})

        trans.append({"name": t_name, "type": t.get("TYPE") or "Transformation",
                      "fields": fields, "is_boundary": is_in or is_out})

    # mapplet-level connectors
    for c in mp_el.findall(".//CONNECTOR"):
        fi = _aget(c, "FROMINSTANCE", "FROM_INSTANCE", "FROMINSTANCENAME")
        ti = _aget(c, "TOINSTANCE",   "TO_INSTANCE",   "TOINSTANCENAME")
        fp = _aget(c, "FROMPORT", "FROM_FIELD", "FROMFIELD", "FROMPORTNAME", "FROMFIELDNAME")
        tp = _aget(c, "TOPORT",  "TO_FIELD",   "TOFIELD",   "TOPORTNAME",  "TOFIELDNAME")
        if fi and ti and fp and tp:
            edges_intra.append({"from": (fi, fp), "to": (ti, tp)})

    # external ports and their owning boundary transforms
    in_ports,  out_ports = set(), set()
    in_port_owners:  Dict[str, List[str]] = {}
    out_port_owners: Dict[str, List[str]] = {}

    for t in trans:
        tname = t["name"]
        if tname in boundary_in_names:
            for f in t["fields"]:
                p = f["name"]; 
                if not p: continue
                in_ports.add(p)
                in_port_owners.setdefault(p, []).append(tname)
        if tname in boundary_out_names:
            for f in t["fields"]:
                p = f["name"]; 
                if not p: continue
                out_ports.add(p)
                out_port_owners.setdefault(p, []).append(tname)

    return {
        "transforms": trans,
        "edges": edges_intra,
        "exprs": exprs_local,
        "in_ports":  sorted(in_ports),
        "out_ports": sorted(out_ports),
        "in_port_owners":  in_port_owners,
        "out_port_owners": out_port_owners,
    }

# ==========================
# SQL override support
# ==========================

def _extract_sql_override_from_attrs(t) -> str:
    for ta in t.findall("./TABLEATTRIBUTE"):
        name = (ta.get("NAME") or "").lower()
        val  = ta.get("VALUE") or ta.text or ""
        if not val:
            continue
        if any(k in name for k in ("sql", "override", "query")):
            return val
    return ""


def _parse_sql_dependencies(sql: str):
    """
    Return (tables, selects, joins_text)
      tables: dict alias->full (e.g., 'C'->'DB.SCH.TABLE')
      selects: dict out_name -> list of (alias, column)
      joins_text: string with ON conditions
    """
    if not _HAS_SQLGLOT:
        return {}, {}, ""
    try:
        root = _sg.parse_one(sql, read="ansi")
    except Exception:
        return {}, {}, ""

    tables = {}
    for t in root.find_all(_sge.Table):
        alias = (t.alias_or_name or t.name)
        parts = [p for p in (t.db, t.catalog, t.this) if p]
        full = ".".join([p.upper() for p in parts]) if parts else (t.name or alias)
        tables[alias] = full

    selects = {}
    select_expr = getattr(root, "select", None)
    if select_expr:
        for proj in select_expr.expressions:
            out = proj.alias_or_name
            if not out and isinstance(proj, _sge.Column):
                out = proj.name
            out = out or ""
            refs = []
            for col in proj.find_all(_sge.Column):
                alias = (col.table or "")
                name  = col.name or ""
                if name:
                    refs.append((alias, name))
            if out:
                seen = set()
                for r in refs:
                    if r not in seen:
                        selects.setdefault(out, []).append(r)
                        seen.add(r)

    join_parts = []
    for j in root.find_all(_sge.Join):
        on = j.args.get("on")
        if on is not None:
            join_parts.append(on.sql())
    joins_text = " AND ".join(join_parts)

    return tables, selects, joins_text

# ==========================
# Main per-mapping parser (element-based)
# ==========================

def parse_mapping_element(mapping, folder: str,
                          folder_sources: Dict[str, Dict],
                          folder_targets: Dict[str, Dict],
                          folder_mapplets: Dict[str, Dict]) -> str:
    mapping_name = mapping.get("NAME") or "(unnamed)"
    mapping_id = _id(folder, mapping_name)

    # Buckets
    instances: List[Dict] = []
    ports:     List[Dict] = []
    edges:     List[Dict] = []
    exprs:     List[Dict] = []
    physical_objects: List[Dict] = []
    map_sources: List[Dict] = []
    map_targets: List[Dict] = []
    instance_phys: List[Dict] = []
    sq_assoc:  List[Dict] = []

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

    def _add_ports_for_source_instance(inst_id: str, src_key: str):
        meta = folder_sources.get(src_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            _add_port(inst_id, fname, "OUTPUT", dtype)
        obj_id = _id("SRC", meta["full"])
        physical_objects.append({
            "object_id": obj_id,
            "kind": "SOURCE",
            "db": meta["db"],
            "schema": meta["schema"],
            "name": meta["name"],
            "full_name": meta["full"],
        })
        map_sources.append({"mapping_id": mapping_id, "object_id": obj_id})
        instance_phys.append({"instance_id": inst_id, "object_id": obj_id, "role": "Source"})

    def _add_ports_for_target_instance(inst_id: str, tgt_key: str):
        meta = folder_targets.get(tgt_key)
        if not meta:
            return
        for fname, dtype in meta["fields"]:
            _add_port(inst_id, fname, "INPUT", dtype)
        obj_id = _id("TGT", meta["full"])
        physical_objects.append({
            "object_id": obj_id,
            "kind": "TARGET",
            "db": meta["db"],
            "schema": meta["schema"],
            "name": meta["name"],
            "full_name": meta["full"],
        })
        map_targets.append({"mapping_id": mapping_id, "object_id": obj_id})
        instance_phys.append({"instance_id": inst_id, "object_id": obj_id, "role": "Target"})

    # --- Transformations (instances + ports + strict edges from expressions)
    tx_names = set()
    for t in mapping.findall("./TRANSFORMATION"):
        t_name = t.get("NAME")
        t_type = t.get("TYPE") or "Transformation"
        tx_names.add(t_name)
        inst_id = _add_instance(t_name, t_type)

        input_names: List[str] = []
        var_names:   List[str] = []
        outputs:     List[Dict] = []
        var_exprs:   Dict[str, str] = {}

        for pf in t.findall("./TRANSFORMFIELD"):
            pname = pf.get("NAME")
            if not pname:
                continue
            pdir  = (pf.get("PORTTYPE") or "").upper()
            dtype = pf.get("DATATYPE") or ""
            pid   = _id(inst_id, pname)

            direction = pdir if pdir in ("INPUT", "OUTPUT", "VARIABLE") else "VARIABLE"
            _add_port(inst_id, pname, direction, dtype)

            expr_text = _expr_text_from_port(pf)
            expr_name = (pf.get("EXPRESSIONNAME") or pf.get("EXPRESSION_NAME") or "")

            if expr_text and direction in ("OUTPUT", "VARIABLE"):
                exprs.append({
                    "port_id": pid,
                    "kind":   "expr",
                    "raw":    expr_text,
                    "meta":   expr_name,
                })

            if direction == "INPUT":
                input_names.append(pname)
            elif direction == "VARIABLE":
                var_names.append(pname)
                if expr_text:
                    var_exprs[pname] = expr_text
            elif direction == "OUTPUT":
                outputs.append({"name": pname, "expr_text": expr_text})

        # record join/filter text (metadata only)
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

        inputs_ci = {n.lower(): n for n in input_names}
        vars_ci   = {n.lower(): n for n in var_names}

        # STRICT wiring for VARIABLEs: tokens in a var's expression feed the var
        for vname, vexpr in var_exprs.items():
            tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", vexpr)
            v_pid  = _id(inst_id, vname)
            for tok in tokens:
                tci = tok.lower()
                if tci in inputs_ci:
                    in_pid = _id(inst_id, inputs_ci[tci])
                    if in_pid != v_pid:
                        edges.append({"from_port_id": in_pid, "to_port_id": v_pid})
                elif tci in vars_ci and vars_ci[tci] != vname:
                    in_pid = _id(inst_id, vars_ci[tci])
                    edges.append({"from_port_id": in_pid, "to_port_id": v_pid})

        # STRICT wiring for OUTPUTs: tokens in OUTPUT expr feed the output
        for od in outputs:
            out_name = od["name"]
            out_pid  = _id(inst_id, out_name)
            expr_txt = od["expr_text"] or ""
            if not expr_txt:
                continue
            tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr_txt)
            for tok in tokens:
                tci = tok.lower()
                if tci in inputs_ci:
                    in_pid = _id(inst_id, inputs_ci[tci])
                    if in_pid != out_pid:
                        edges.append({"from_port_id": in_pid, "to_port_id": out_pid})
                elif tci in vars_ci:
                    in_pid = _id(inst_id, vars_ci[tci])
                    if in_pid != out_pid:
                        edges.append({"from_port_id": in_pid, "to_port_id": out_pid})

        # SQL overrides (SQ/Lookup)
        t_type_low = (t.get("TYPE") or "").strip().lower()
        is_sq     = "source qualifier" in t_type_low
        is_lookup = (t_type_low in ("lookup", "lookup procedure")) or ("lookup" in t_type_low)
        if is_sq or is_lookup:
            sql_text = _extract_sql_override_from_attrs(t)
            if sql_text:
                _apply_sql_override(mapping_id, inst_id, inst_name=t.get("NAME"), sql_text=sql_text, is_lookup=is_lookup,
                                    instances=instances, ports=ports, edges=edges, exprs=exprs,
                                    physical_objects=physical_objects, map_sources=map_sources, instance_phys=instance_phys)

    # --- INSTANCE binding (Source/Target/Mapplet classification)
    instance_type_by_name: Dict[str, str] = {}
    instance_refname_by_name: Dict[str, str] = {}
    instance_is_mapplet: Dict[str, bool] = {}

    for inst in mapping.findall(".//INSTANCE"):
        iname   = _aget(inst, "NAME", "INSTANCE_NAME")
        if not iname:
            continue
        rawt    = (_aget(inst, "TYPE", "TRANSFORMATION_TYPE", "TRANSFORMATIONTYPE") or "").strip().lower()
        refname = _aget(inst, "TRANSFORMATION_NAME", "REFOBJECTNAME", "REF_OBJECT_NAME", "REFOBJECT_NAME", "TRANFIRMATION_NAME") or ""
        refU    = (refname or "").upper()

        is_target = (rawt in ("target", "target definition")) or (refU in folder_targets)
        is_source = (rawt in ("source", "source definition")) or (refU in folder_sources)
        is_mapplet = (rawt == "mapplet") or (refU in folder_mapplets)

        if is_target:
            instance_type_by_name[iname] = "Target"
        elif is_source:
            instance_type_by_name[iname] = "Source"
        else:
            instance_type_by_name[iname] = "Transformation"
        instance_refname_by_name[iname] = refname
        if is_mapplet:
            instance_is_mapplet[iname] = True

        # Source Qualifier association(s)
        if rawt == "source qualifier":
            assoc_attr = _aget(inst, "ASSOCIATED_SOURCE_INSTANCE", "ASSOCIATEDSOURCEINSTANCE")
            if assoc_attr:
                sq_assoc.append({
                    "mapping_id": mapping_id,
                    "sq_instance_id": _id(mapping_id, iname),
                    "source_instance_name": assoc_attr.strip(),
                })
            for child in inst.findall("./ASSOCIATED_SOURCE_INSTANCE"):
                nm = (child.get("NAME") or child.get("INSTANCE") or (child.text or "")).strip()
                if nm:
                    sq_assoc.append({
                        "mapping_id": mapping_id,
                        "sq_instance_id": _id(mapping_id, iname),
                        "source_instance_name": nm,
                    })

    # materialize non-TRANSFORMATION instances
    for iname, itype in instance_type_by_name.items():
        if iname in tx_names:
            continue
        inst_id = _add_instance(iname, itype)

        refU = (instance_refname_by_name.get(iname, "") or "").upper()
        if instance_is_mapplet.get(iname) or (refU in folder_mapplets):
            _expand_mapplet_instance(mapping_id, inst_id, iname, folder_mapplets[refU], instances, ports, edges, exprs)
            continue
        if itype == "Source":
            key = (instance_refname_by_name.get(iname, "") or "").upper()
            if key:
                _add_ports_for_source_instance(inst_id, key)
        elif itype == "Target":
            key = (instance_refname_by_name.get(iname, "") or "").upper()
            if key:
                _add_ports_for_target_instance(inst_id, key)

    # --- CONNECTOR edges (strict)
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

    # Persist
    st.upsert("mappings", [{"mapping_id": mapping_id, "name": mapping_name, "folder": folder}], ("mapping_id",))
    st.insert_if_missing("instances", instances, ("instance_id",))
    st.insert_if_missing("ports", ports, ("port_id",))
    st.insert_if_missing("edges", edges, ("from_port_id", "to_port_id"))
    st.insert_if_missing("expressions", exprs, ("port_id", "kind", "raw"))
    st.insert_if_missing("physical_objects", physical_objects, ("object_id",))
    st.insert_if_missing("map_sources", map_sources, ("mapping_id", "object_id"))
    st.insert_if_missing("map_targets", map_targets, ("mapping_id", "object_id"))
    st.insert_if_missing("instance_phys", instance_phys, ("instance_id", "object_id"))
    st.insert_if_missing("sq_assoc", sq_assoc, ("mapping_id", "sq_instance_id", "source_instance_name"))

    return mapping_id

# Mapplet expansion (inliner)

def _expand_mapplet_instance(mapping_id: str, inst_id: str, inst_name: str, mpdef: Dict,
                             instances: List[Dict], ports: List[Dict], edges: List[Dict], exprs: List[Dict]):
    inner_name_to_inst_id: Dict[str, str] = {}

    def _add_instance_local(inner_name: str, inner_type: str) -> str:
        prefixed_name = f"{inst_name}.{inner_name}"
        iid = _id(mapping_id, prefixed_name)
        instances.append({
            "instance_id": iid,
            "mapping_id": mapping_id,
            "type": inner_type or "Transformation",
            "name": prefixed_name,
        })
        return iid

    def _add_port_local(iid: str, name: str, direction: str, dtype: str = ""):
        ports.append({
            "port_id": _id(iid, name),
            "instance_id": iid,
            "name": name,
            "dtype": dtype,
            "direction": direction,
        })

    # 1) inner transforms and ports
    for t in mpdef["transforms"]:
        inner_name = t["name"]
        inner_type = t["type"]
        pref_id = _add_instance_local(inner_name, inner_type)
        inner_name_to_inst_id[inner_name] = pref_id
        for f in t["fields"]:
            direction = f.get("dir") or "VARIABLE"
            _add_port_local(pref_id, f["name"], direction, f.get("dtype", ""))

    # 2) expressions on inner ports
    for ex in mpdef["exprs"]:
        pref_inst_id = inner_name_to_inst_id.get(ex["inst"])
        if pref_inst_id:
            exprs.append({
                "port_id": _id(pref_inst_id, ex["port"]),
                "kind": "expr",
                "raw": ex["raw"],
                "meta": None,
            })

    # 3) inner edges
    for ed in mpdef["edges"]:
        fi, fp = ed["from"]; ti, tp = ed["to"]
        fi_id = inner_name_to_inst_id.get(fi); ti_id = inner_name_to_inst_id.get(ti)
        if fi_id and ti_id:
            edges.append({"from_port_id": _id(fi_id, fp), "to_port_id": _id(ti_id, tp)})

    # 4) external ports on the mapplet instance
    for p in mpdef["in_ports"]:
        ports.append({
            "port_id": _id(inst_id, p),
            "instance_id": inst_id,
            "name": p,
            "dtype": "",
            "direction": "INPUT",
        })
    for p in mpdef["out_ports"]:
        ports.append({
            "port_id": _id(inst_id, p),
            "instance_id": inst_id,
            "name": p,
            "dtype": "",
            "direction": "OUTPUT",
        })

    # 5) bridges per-port to owning boundary transforms
    for p, owners in (mpdef.get("in_port_owners", {}) or {}).items():
        for owner in owners:
            inner_iid = inner_name_to_inst_id.get(owner)
            if inner_iid:
                edges.append({
                    "from_port_id": _id(inst_id, p),
                    "to_port_id":   _id(inner_iid, p),
                })
    for p, owners in (mpdef.get("out_port_owners", {}) or {}).items():
        for owner in owners:
            inner_iid = inner_name_to_inst_id.get(owner)
            if inner_iid:
                edges.append({
                    "from_port_id": _id(inner_iid, p),
                    "to_port_id":   _id(inst_id, p),
                })

# SQL override applier (uses parser buckets from caller)

def _apply_sql_override(mapping_id: str, sq_inst_id: str, inst_name: str, sql_text: str, is_lookup: bool,
                        instances: List[Dict], ports: List[Dict], edges: List[Dict], exprs: List[Dict],
                        physical_objects: List[Dict], map_sources: List[Dict], instance_phys: List[Dict]):
    tables, selects, joins_text = _parse_sql_dependencies(sql_text)
    # record full SQL always for debugging
    exprs.append({
        "port_id": _id(sq_inst_id, "__sql_override__"),
        "kind": "expr",
        "raw": sql_text,
        "meta": "sql_override"
    })
    if not tables and not selects:
        return

    # pseudo source instances for each table alias
    alias_to_inst: Dict[str, str] = {}
    for alias, full in tables.items():
        if not alias:
            continue
        src_inst_name = f"SQLSRC_{inst_name}_{alias}"
        src_inst_id   = _id(mapping_id, src_inst_name)
        instances.append({
            "instance_id": src_inst_id,
            "mapping_id": mapping_id,
            "type": "Source",
            "name": src_inst_name,
        })
        obj_id = _id("SRC", full)
        physical_objects.append({
            "object_id": obj_id,
            "kind": "SOURCE",
            "db": "", "schema": "", "name": full.split(".")[-1],
            "full_name": full,
        })
        map_sources.append({"mapping_id": mapping_id, "object_id": obj_id})
        instance_phys.append({"instance_id": src_inst_id, "object_id": obj_id, "role": "Source"})
        alias_to_inst[alias] = src_inst_id

    # SQ/Lookup output ports index (accept INPUT/OUTPUT for safety across exports)
    sq_out_ports = { p["name"]: p for p in ports
                     if p["instance_id"] == sq_inst_id and (p.get("direction") or "").upper() in ("OUTPUT", "INPUT") }

    # projections → edges
    for out_name, refs in selects.items():
        sqp = sq_out_ports.get(out_name)
        if not sqp:
            continue
        # annotate output port with projection label
        exprs.append({
            "port_id": _id(sq_inst_id, out_name),
            "kind": "expr",
            "raw": f"[sql_override] {out_name}",
            "meta": "sql_projection",
        })
        for (alias, col) in refs:
            if not col:
                continue
            src_inst_id = alias_to_inst.get(alias) or alias_to_inst.get(alias.upper()) or alias_to_inst.get(alias.lower())
            if not src_inst_id:
                continue
            src_port_id = _id(src_inst_id, col)
            if not any(p["port_id"] == src_port_id for p in ports):
                ports.append({
                    "port_id": src_port_id,
                    "instance_id": src_inst_id,
                    "name": col,
                    "dtype": "",
                    "direction": "OUTPUT",
                })
            edges.append({"from_port_id": src_port_id, "to_port_id": _id(sq_inst_id, out_name)})

    if joins_text:
        exprs.append({
            "port_id": _id(sq_inst_id, "__join__"),
            "kind": "join",
            "raw": joins_text,
            "meta": "sql_join",
        })

# ==========================
# Folder-level collectors
# ==========================

def _full(db, schema, name):
    db = (db or "").upper(); schema = (schema or "").upper()
    return f"{db}.{schema}.{name}" if db and schema else name

def _collect_folder_sources(folder_el) -> Dict[str, Dict]:
    out = {}
    for s in folder_el.findall("./SOURCE"):
        sname = (s.get("NAME") or "").strip()
        if not sname: continue
        fields = _collect_fields(s, ["SOURCEFIELD", "FIELD"])
        out[sname.upper()] = {
            "fields": fields,
            "db": (s.get("DBDNAME") or "").upper(),
            "schema": (s.get("OWNERNAME") or "").upper(),
            "name": sname,
            "full": _full(s.get("DBDNAME"), s.get("OWNERNAME"), sname),
        }
    return out

def _collect_folder_targets(folder_el) -> Dict[str, Dict]:
    out = {}
    for t in folder_el.findall("./TARGET"):
        tname = (t.get("NAME") or "").strip()
        if not tname: continue
        fields = _collect_fields(t, ["TARGETFIELD", "FIELD"])
        out[tname.upper()] = {
            "fields": fields,
            "db": (t.get("DBDNAME") or "").upper(),
            "schema": (t.get("OWNERNAME") or "").upper(),
            "name": tname,
            "full": _full(t.get("DBDNAME"), t.get("OWNERNAME"), tname),
        }
    return out

def _collect_folder_mapplets(folder_el) -> Dict[str, Dict]:
    out = {}
    for mp in folder_el.findall("./MAPPLET"):
        nameU = (mp.get("NAME") or "").strip().upper()
        if nameU:
            out[nameU] = _parse_mapplet_def(mp)
    return out

# ==========================
# Repo-level parser (single file with many mappings)
# ==========================

def parse_repo_file(xml_path: str) -> None:
    ctx = ET.iterparse(xml_path, events=("end",))
    for event, elem in ctx:
        if elem.tag == "FOLDER":
            folder_name = elem.get("NAME") or "UNKNOWN"
            f_sources  = _collect_folder_sources(elem)
            f_targets  = _collect_folder_targets(elem)
            f_mapplets = _collect_folder_mapplets(elem)
            for m in elem.findall("./MAPPING"):
                parse_mapping_element(m, folder_name, f_sources, f_targets, f_mapplets)
            elem.clear()
    del ctx

# Back-compat: first mapping in file

def parse_mapping_xml(xml_path: str) -> str:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    folder_el  = root.find(".//FOLDER")
    mapping_el = folder_el.find("./MAPPING") if folder_el is not None else root.find(".//MAPPING")
    if folder_el is None or mapping_el is None:
        raise ValueError(f"FOLDER or MAPPING not found in {xml_path}")

    folder_name   = folder_el.get("NAME") or "UNKNOWN"
    f_sources     = _collect_folder_sources(folder_el)
    f_targets     = _collect_folder_targets(folder_el)
    f_mapplets    = _collect_folder_mapplets(folder_el)

    return parse_mapping_element(mapping_el, folder_name, f_sources, f_targets, f_mapplets)
