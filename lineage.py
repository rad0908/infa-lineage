from collections import deque
from difflib import SequenceMatcher
from typing import List, Dict
import storage as st

def _norm(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _best_name_match(name: str, candidates: List[str], threshold: float = 0.82) -> tuple[str, float]:
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
    field_like = (field_like or "").lower().replace("_", "")
    out = []
    ports = st.all_rows("ports")
    insts = _index_instances()
    for p in ports:
        inst = insts.get(p["instance_id"])
        if not inst or inst["type"] != "Target":
            continue
        norm = p["name"].lower().replace("_", "")
        if field_like in norm:
            out.append({
                "port_id": p["port_id"],
                "instance_id": p["instance_id"],
                "port_name": p["name"],
                "instance_name": inst["name"],
                "mapping_name": _mapping_name_by_instance_id(p["instance_id"])
            })
    return out

def attach_expr_and_join(from_port_id: str, from_instance_id: str) -> Dict[str, str]:
    exprs = [e for e in st.all_rows("expressions") if e["port_id"] == from_port_id and e["kind"] == "expr"]
    expr = exprs[0]["raw"] if exprs else ""
    joiners = [e for e in st.all_rows("expressions") if e["kind"] == "join" and e["port_id"].startswith(from_instance_id)]
    join_cond = joiners[0]["raw"] if joiners else ""
    return {"expression": expr, "join_condition": join_cond}

def _physical_full_for_source_instance(mapping_id: str, source_inst_name: str) -> str:
    for ms in st.all_rows("map_sources"):
        if ms["mapping_id"] != mapping_id:
            continue
        obj = st.by_id("physical_objects", "object_id").get(ms["object_id"])
        if obj and obj["name"] == source_inst_name:
            return obj["full_name"]
    return ""

def _target_instance_for_physical(mapping_id: str, full_name: str) -> str:
    phys = st.by_id("physical_objects", "object_id")
    for mt in st.all_rows("map_targets"):
        if mt["mapping_id"] != mapping_id:
            continue
        obj = phys.get(mt["object_id"])
        if obj and obj["full_name"] == full_name:
            return obj["name"]
    return ""

def build_crosslinks_deterministic() -> int:
    map_targets = st.all_rows("map_targets")
    map_sources = st.all_rows("map_sources")
    phys = st.by_id("physical_objects", "object_id")
    mappings = st.by_id("mappings", "mapping_id")

    tgt_by_full = {}
    for mt in map_targets:
        full = phys[mt["object_id"]]["full_name"]
        tgt_by_full.setdefault(full, []).append(mappings[mt["mapping_id"]]["name"])

    src_by_full = {}
    for ms in map_sources:
        full = phys[ms["object_id"]]["full_name"]
        src_by_full.setdefault(full, []).append(mappings[ms["mapping_id"]]["name"])

    rows = []
    for full, from_maps in tgt_by_full.items():
        for fm in from_maps:
            for tm in src_by_full.get(full, []):
                if fm != tm:
                    rows.append({"from_mapping": fm, "to_mapping": tm, "object_name": full})

    st.insert_if_missing("crosslinks", rows, ("from_mapping", "to_mapping", "object_name"))
    return len(rows)


def upstream_lineage_multi(field: str, max_rows: int = 10000) -> List[Dict]:
    edges = st.all_rows("edges")
    ports_idx = st.by_id("ports", "port_id")
    inst_idx  = st.by_id("instances", "instance_id")
    maps_idx  = st.by_id("mappings", "mapping_id")

    to_index = {}
    for e in edges:
        to_index.setdefault(e["to_port_id"], []).append(e["from_port_id"])

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

    from collections import deque
    dq = deque(p["port_id"] for p in starts)

    while dq and len(results) < max_rows:
        cur = dq.popleft()
        if cur in visited_ports:
            continue
        visited_ports.add(cur)

        for up in to_index.get(cur, []):
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

            if fi["type"].lower() != "source":
                dq.append(up)
            else:
                src_inst_name = fi["name"]
                src_map_id = fi["mapping_id"]
                full = ""
                for ms in st.all_rows("map_sources"):
                    if ms["mapping_id"] != src_map_id:
                        continue
                    obj = st.by_id("physical_objects", "object_id").get(ms["object_id"])
                    if obj and obj["name"] == src_inst_name:
                        full = obj["full_name"]
                        break
                if not full:
                    continue

                for upstream_map_name in tgt_map_by_full.get(full, []):
                    upstream_map_id = next((mid for mid, m in maps_idx.items() if m["name"] == upstream_map_name), "")
                    if not upstream_map_id:
                        continue

                    tgt_inst_name = _target_instance_for_physical(upstream_map_id, full)
                    if not tgt_inst_name:
                        continue

                    candidate_ports = [p for p in ports_idx.values()
                        if p["instance_id"] == f"{upstream_map_id}:{tgt_inst_name}" and p["direction"] == "INPUT"]
                    candidate_names = [p["name"] for p in candidate_ports]

                    best_name, score = "", 0.0
                    if candidate_names:
                        n = _norm(fp["name"])
                        for c in candidate_names:
                            if _norm(c) == n:
                                best_name, score = c, 1.0
                                break
                        if not best_name:
                            best, sc = "", 0.0
                            for c in candidate_names:
                                s = SequenceMatcher(None, n, _norm(c)).ratio()
                                if s > sc:
                                    best, sc = c, s
                            if sc >= 0.82:
                                best_name, score = best, sc

                    next_pid = f"{upstream_map_id}:{tgt_inst_name}:{best_name}" if best_name else ""

                    key = (full, upstream_map_name, fp["name"], best_name)
                    if key in visited_cross:
                        continue
                    visited_cross.add(key)

                    hop += 1
                    results.append({
                        "hop_no": hop,
                        "mapping": f"{maps_idx[src_map_id]['name']} -> {upstream_map_name}",
                        "from_instance": "(TARGET)", "from_port": best_name or "", "from_type": "Target",
                        "to_instance": "(SOURCE)",  "to_port":   fp["name"],       "to_type":   "Source",
                        "operation": f"cross_workflow{'' if not best_name else f' (col~{score:.2f})'}",
                        "expression": "", "join_condition": "",
                        "stage": "cross_workflow",
                        "evidence": full
                    })

                    if next_pid:
                        dq.append(next_pid)

    return results
