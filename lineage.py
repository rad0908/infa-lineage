
from collections import deque
from difflib import SequenceMatcher
from typing import List, Dict, Tuple
import storage as st
import re

CROSSWORKFLOW_EXACT_ONLY = True
REQUIRE_UNIQUE_UPSTREAM = True

_IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def _resolved_expr_for_edge(up_pid: str, cur_pid: str, ports_idx: dict) -> str:
    """
    Prefer the OUTPUT expression unless it is a simple alias (single identifier)
    pointing to the upstream port name; in that case show the upstream port's formula.
    Falls back to upstream expr if OUTPUT has none.
    """
    cur_expr = _expr_for_port(cur_pid)
    if cur_expr:
        token = cur_expr.strip()
        if _IDENT.match(token):
            up = ports_idx.get(up_pid)
            if up and token.lower() == (up.get("name","").lower()):
                # OUTPUT just references the VAR; show the VAR's actual formula
                return _expr_for_port(up_pid) or cur_expr
        return cur_expr
    # no OUTPUT expr -> maybe the VAR/input carries the formula
    return _expr_for_port(up_pid)

def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _idnorm(pid: str) -> str:
    return (pid or "").lower()

def _best_name_match(name: str, candidates: List[str], threshold: float = 0.82) -> Tuple[str, float]:
    if not candidates:
        return ("", 0.0)
    n = _norm(name)
    for c in candidates:
        if _norm(c) == n:
            return (c, 1.0)
    best, score = "", 0.0
    for c in candidates:
        s = SequenceMatcher(None, n, _norm(c)).ratio()
        if s > score:
            best, score = c, s
    return (best, score if score >= threshold else 0.0)

def _index_ports() -> Dict[str, Dict]:
    return st.by_id("ports", "port_id")

def _index_instances() -> Dict[str, Dict]:
    return st.by_id("instances", "instance_id")

def _mapping_name_by_instance_id(instance_id: str) -> str:
    inst = _index_instances().get(instance_id)
    if not inst:
        return ""
    mapping = st.where("mappings", mapping_id=inst["mapping_id"])
    return mapping[0]["name"] if mapping else ""

def _expr_for_port(port_id: str) -> str:
    for e in st.all_rows("expressions"):
        if e.get("kind") == "expr" and e.get("port_id") == port_id:
            return e.get("raw", "")
    return ""

def _join_for_instance(instance_id: str) -> str:
    for e in st.all_rows("expressions"):
        if e.get("kind") == "join" and str(e.get("port_id","")).startswith(instance_id):
            return e.get("raw", "")
    return ""


def find_target_ports_by_field(field_like: str) -> List[Dict]:
    # Use ONLY exact case-insensitive match on the target column name.
    # If the user passes a qualified name like FOLDER:MAP:INST:COL or SCHEMA.TABLE.COL,
    # we still match exactly on the final COL token only.
    q_raw = field_like or ""
    if ":" in q_raw or "." in q_raw:
        q_raw = q_raw.split(":")[-1].split(".")[-1]
    q_ci = (q_raw or "").lower()
    if not q_ci:
        return []

    ports = st.all_rows("ports")
    insts = _index_instances()
    edges = st.all_rows("edges")

    has_out_norm = { (e["from_port_id"] or "").lower() for e in edges }
    has_in_norm  = { (e["to_port_id"]   or "").lower() for e in edges }

    results: List[Dict] = []

    for p in ports:
        inst = insts.get(p["instance_id"])
        if not inst:
            continue

        # treat as a target column if it's a real Target instance
        # OR an INPUT sink with no outgoing edge
        is_targetish = (inst.get("type") == "Target") or (
            p.get("direction") == "INPUT" and (p["port_id"] or "").lower() not in has_out_norm
        )
        if not is_targetish:
            continue

        # EXACT match only (case-insensitive). Do NOT strip underscores/punctuation.
        if (p.get("name", "") or "").lower() != q_ci:
            continue

        results.append({
            "port_id": p["port_id"],
            "instance_id": p["instance_id"],
            "port_name": p["name"],
            "instance_name": inst["name"],
            "mapping_name": _mapping_name_by_instance_id(p["instance_id"]).strip()
        })

    # deterministic: prefer starts that already have inbound edges
    results.sort(key=lambda r: ((r["port_id"] or "").lower() not in has_in_norm,
                                r["mapping_name"], r["instance_name"], r["port_name"]))
    return results

def attach_expr_and_join(from_port_id: str, from_instance_id: str) -> Dict[str, str]:
    exprs = [e for e in st.all_rows("expressions") if e["port_id"] == from_port_id and e["kind"] == "expr"]
    expr = exprs[0]["raw"] if exprs else ""
    joiners = [e for e in st.all_rows("expressions") if e["kind"] == "join" and e["port_id"].startswith(from_instance_id)]
    join_cond = joiners[0]["raw"] if joiners else ""
    return {"expression": expr, "join_condition": join_cond}

def _target_instance_for_physical(mapping_id: str, full_name: str) -> str:
    phys = st.by_id("physical_objects", "object_id")
    for mt in st.all_rows("map_targets"):
        if mt["mapping_id"] != mapping_id:
            continue
        obj = phys.get(mt["object_id"])
        if obj and obj["full_name"] == full_name:
            return obj["name"]
    return ""



def upstream_lineage_multi(field: str, max_rows: int = 10000) -> List[Dict]:
    """
    Strict lineage, ordered from target -> upstream.
    - Start ports come from exact (case-insensitive) field match.
    - Inside a mapping: only CONNECTOR edges and EXPRESSION-based edges.
    - Cross-workflow: exact physical object + exact column name.
    - Returns rows grouped per target ('chain_id'), ordered by 'level' and 'step_no'.
    """
    edges = st.all_rows("edges")
    ports_idx = st.by_id("ports", "port_id")
    inst_idx  = st.by_id("instances", "instance_id")
    maps_idx  = st.by_id("mappings", "mapping_id")

    def _idnorm(pid: str) -> str:
        return (pid or "").lower()

    # to_port -> [from_port...]
    to_index: Dict[str, List[str]] = {}
    for e in edges:
        to_index.setdefault(_idnorm(e["to_port_id"]), []).append(e["from_port_id"])

    # physical target map: full_name -> [mapping names]
    phys = st.by_id("physical_objects", "object_id")
    tgt_map_by_full = {}
    for mt in st.all_rows("map_targets"):
        full = phys[mt["object_id"]]["full_name"]
        tgt_map_by_full.setdefault(full, []).append(maps_idx[mt["mapping_id"]]["name"])

    def _target_instance_for_physical(mapping_id: str, full_name: str) -> str:
        for mt in st.all_rows("map_targets"):
            if mt["mapping_id"] != mapping_id:
                continue
            obj = phys.get(mt["object_id"])
            if obj and obj["full_name"] == full_name:
                return obj["name"]
        return ""

    starts = find_target_ports_by_field(field)  # exact-only, already implemented

    all_rows: List[Dict] = []
    chain_id = 0

    for start in starts:
        chain_id += 1
        start_pid = start["port_id"]

        dist = { start_pid: 0 }           # port_id -> hops from target
        visited_ports = { start_pid }
        visited_cross = set()
        dq = deque([start_pid])

        chain_rows: List[Dict] = []

        while dq and len(all_rows) + len(chain_rows) < max_rows:
            cur = dq.popleft()

            # upstream edges inside the same mapping
            ups = to_index.get(_idnorm(cur), [])
            for up in ups:
                fp = ports_idx.get(up); tp = ports_idx.get(cur)
                if not fp or not tp:
                    continue
                fi = inst_idx.get(fp["instance_id"]); ti = inst_idx.get(tp["instance_id"])
                if not fi or not ti:
                    continue

                level = dist[cur] + 1

                # Expression: prefer the OUTPUT side (cur), else FROM side (up)
                expr_cur = _expr_for_port(cur)
                expr_up  = _expr_for_port(up)
                expr_raw = _resolved_expr_for_edge(up, cur, ports_idx)
                expr_owner_inst = ti["instance_id"] if _expr_for_port(cur) else fi["instance_id"]
                join_cond = _join_for_instance(expr_owner_inst)

                chain_rows.append({
                    "chain_id": chain_id,
                    "level": level,
                    "mapping": maps_idx[ti["mapping_id"]]["name"],
                    "from_instance": fi["name"], "from_port": fp["name"], "from_type": fi["type"],
                    "to_instance": ti["name"],  "to_port":  tp["name"],  "to_type":  ti["type"],
                    "operation": "compute" if expr_raw else "passthrough",
                    "expression": expr_raw,
                    "join_condition": join_cond,
                    "stage": "mapping",
                    "evidence": f"{up}->{cur}",
                })

                if up not in visited_ports:
                    visited_ports.add(up)
                    dist[up] = level
                    dq.append(up)

                # Strict cross-workflow: only when FROM instance is a Source
                if fi["type"].lower() == "source":
                    src_inst_name = fi["name"]
                    src_map_id = fi["mapping_id"]

                    # physical full name for this source instance
                    full = ""
                    for ms in st.all_rows("map_sources"):
                        if ms["mapping_id"] != src_map_id:
                            continue
                        obj = st.by_id("physical_objects", "object_id").get(ms["object_id"])
                        if obj and obj["name"] == src_inst_name:
                            full = obj["full_name"]; break
                    if not full:
                        continue

                    # all mappings whose TARGET physical matches exactly
                    candidate_map_names = tgt_map_by_full.get(full, [])
                    if not candidate_map_names:
                        continue

                    exact_col = (fp["name"] or "").lower()
                    for upstream_map_name in candidate_map_names:
                        upstream_map_id = next((mid for mid, m in maps_idx.items()
                                                if m["name"] == upstream_map_name), "")
                        if not upstream_map_id:
                            continue
                        tgt_inst_name = _target_instance_for_physical(upstream_map_id, full)
                        if not tgt_inst_name:
                            continue

                        # exact column on that target's INPUT
                        candidate_ports = [p for p in ports_idx.values()
                            if p["instance_id"] == f"{upstream_map_id}:{tgt_inst_name}" and p["direction"] == "INPUT"]
                        match_port = next((p for p in candidate_ports
                                           if (p["name"] or "").lower() == exact_col), None)
                        if not match_port:
                            continue

                        cross_key = (full, upstream_map_name, fp["name"])
                        if cross_key in visited_cross:
                            continue
                        visited_cross.add(cross_key)

                        level_x = level  # cross step sits between levels
                        chain_rows.append({
                            "chain_id": chain_id,
                            "level": level_x,
                            "mapping": f"{maps_idx[src_map_id]['name']} -> {upstream_map_name}",
                            "from_instance": "(TARGET)", "from_port": match_port["name"], "from_type": "Target",
                            "to_instance": "(SOURCE)",  "to_port":   fp["name"],         "to_type":  "Source",
                            "operation": "cross_workflow (exact)",
                            "expression": "", "join_condition": "",
                            "stage": "cross_workflow",
                            "evidence": full,
                        })

                        next_pid = f"{upstream_map_id}:{tgt_inst_name}:{match_port['name']}"
                        if next_pid not in visited_ports:
                            visited_ports.add(next_pid)
                            dist[next_pid] = level_x   # next mapping edges will be level_x+1
                            dq.append(next_pid)

        # per-chain ordering and step numbering
        chain_rows.sort(key=lambda r: (r["level"], r["stage"], r["mapping"], r["from_instance"], r["from_port"]))
        for i, row in enumerate(chain_rows, start=1):
            row["step_no"] = i
        all_rows.extend(chain_rows)

    # global stable order across chains
    all_rows.sort(key=lambda r: (r["chain_id"], r["step_no"]))
    return all_rows
