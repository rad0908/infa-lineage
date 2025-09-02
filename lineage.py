
from collections import deque
from difflib import SequenceMatcher
from typing import List, Dict, Tuple
import storage as st

CROSSWORKFLOW_EXACT_ONLY = True
REQUIRE_UNIQUE_UPSTREAM = True

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
    edges = st.all_rows("edges")
    ports_idx = st.by_id("ports", "port_id")
    inst_idx  = st.by_id("instances", "instance_id")
    maps_idx  = st.by_id("mappings", "mapping_id")

    to_index: Dict[str, List[str]] = {}
    for e in edges:
        to_index.setdefault(_idnorm(e["to_port_id"]), []).append(e["from_port_id"])

    starts = find_target_ports_by_field(field)
    results: List[Dict] = []
    hop = 0

    phys = st.by_id("physical_objects", "object_id")
    tgt_map_by_full = {}
    for mt in st.all_rows("map_targets"):
        full = phys[mt["object_id"]]["full_name"]
        tgt_map_by_full.setdefault(full, []).append(maps_idx[mt["mapping_id"]]["name"])

    visited_ports = set()
    visited_cross = set()

    dq = deque(p["port_id"] for p in starts)

    while dq and len(results) < max_rows:
        cur = dq.popleft()
        if cur in visited_ports:
            continue
        visited_ports.add(cur)

        for up in to_index.get(_idnorm(cur), []):
            fp = ports_idx.get(up); tp = ports_idx.get(cur)
            if not fp or not tp:
                continue
            fi = inst_idx.get(fp["instance_id"]); ti = inst_idx.get(tp["instance_id"])
            if not fi or not ti:
                continue

            hop += 1
            extras = attach_expr_and_join(up, fi["instance_id"])
            results.append({
                "hop_no": hop,
                "mapping": maps_idx[ti["mapping_id"]]["name"],
                "from_instance": fi["name"], "from_port": fp["name"], "from_type": fi["type"],
                "to_instance": ti["name"],  "to_port":  tp["name"],  "to_type":  ti["type"],
                "operation": "compute" if extras["expression"] else "passthrough",
                "expression": extras["expression"],
                "join_condition": extras["join_condition"],
                "stage": "mapping",
                "evidence": f"{up}->{cur}"
            })

            # If the upstream instance is a Source, consider cross-workflow stitch
            if fi["type"].lower() != "source":
                dq.append(up)
            else:
                src_inst_name = fi["name"]
                src_map_id = fi["mapping_id"]

                # Resolve physical object full name for this Source instance
                full = ""
                for ms in st.all_rows("map_sources"):
                    if ms["mapping_id"] != src_map_id:
                        continue
                    obj = st.by_id("physical_objects", "object_id").get(ms["object_id"])
                    if obj and obj["name"] == src_inst_name:
                        full = obj["full_name"]; break
                if not full:
                    continue

                # Find mappings whose TARGET physical object matches this full name
                candidate_map_names = tgt_map_by_full.get(full, [])
                if not candidate_map_names:
                    continue

                # Collect only candidates that have an INPUT target port with exact same column name
                exact_col = (fp["name"] or "").lower()
                qualified_candidates = []
                for upstream_map_name in candidate_map_names:
                    upstream_map_id = next((mid for mid, m in maps_idx.items()
                                            if m["name"] == upstream_map_name), "")
                    if not upstream_map_id:
                        continue

                    # Which instance in that mapping is the Target bound to this physical full?
                    tgt_inst_name = _target_instance_for_physical(upstream_map_id, full)
                    if not tgt_inst_name:
                        continue

                    # Look for an INPUT port with EXACT same column name (case-insensitive)
                    candidate_ports = [p for p in ports_idx.values()
                        if p["instance_id"] == f"{upstream_map_id}:{tgt_inst_name}" and p["direction"] == "INPUT"]
                    match_port = next((p for p in candidate_ports if (p["name"] or "").lower() == exact_col), None)
                    if match_port:
                        qualified_candidates.append((upstream_map_id, upstream_map_name, tgt_inst_name, match_port["name"]))

                # Enforce uniqueness to prevent blow-up
                if REQUIRE_UNIQUE_UPSTREAM and len(qualified_candidates) != 1:
                    # ambiguous or none -> do not cross-stitch
                    continue

                # If not requiring uniqueness, you could loop over qualified_candidates;
                # here we take the unique one.
                upstream_map_id, upstream_map_name, tgt_inst_name, best_name = qualified_candidates[0]
                next_pid = f"{upstream_map_id}:{tgt_inst_name}:{best_name}"

                key = (full, upstream_map_name, fp["name"], best_name)
                if key in visited_cross:
                    continue
                visited_cross.add(key)

                hop += 1
                results.append({
                    "hop_no": hop,
                    "mapping": f"{maps_idx[src_map_id]['name']} -> {upstream_map_name}",
                    "from_instance": "(TARGET)", "from_port": best_name, "from_type": "Target",
                    "to_instance": "(SOURCE)",  "to_port":   fp["name"], "to_type":  "Source",
                    "operation": "cross_workflow (exact)",
                    "expression": "", "join_condition": "",
                    "stage": "cross_workflow",
                    "evidence": full
                })

                dq.append(next_pid)

    return results
