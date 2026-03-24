#!/usr/bin/env python3
"""
Vis-network data builder for CH Graphs.

Converts raw Neo4j graph data into vis-network compatible JSON,
including node/edge styling, merging, deduplication and layout.
"""

from search import _compute_levels


def _build_vis_data(nodes, rels):
    """Convert extracted graph data to vis-network compatible JSON."""
    label_colors = {
        "Company": "#4C8BF5", "Person": "#34A853", "CorporateEntity": "#FBBC04",
        "LegalPerson": "#EA4335", "Director": "#00BCD4", "SICCode": "#9C27B0", "Address": "#607D8B",
    }
    node_sizes = {
        "Company": 25, "Person": 15, "CorporateEntity": 20,
        "LegalPerson": 18, "Director": 15, "SICCode": 12, "Address": 12,
    }
    node_shapes = {
        "Company": "dot", "Person": "diamond", "CorporateEntity": "square",
        "LegalPerson": "triangle", "Director": "triangleDown", "SICCode": "star", "Address": "hexagon",
    }

    # Merge CorporateEntity -> Company
    ce_to_company = {}
    for r in rels:
        if r["type"] == "IS_COMPANY":
            ce_id, co_id = r["startId"], r["endId"]
            if ce_id in nodes and co_id in nodes:
                if "CorporateEntity" in nodes[ce_id]["labels"] and "Company" in nodes[co_id]["labels"]:
                    ce_to_company[ce_id] = co_id
    merged_nodes = {nid: n for nid, n in nodes.items() if nid not in ce_to_company}

    def remap(nid):
        return ce_to_company.get(nid, nid)
    merged_rels = []
    for r in rels:
        if r["type"] == "IS_COMPANY" and r["startId"] in ce_to_company:
            continue
        # Filter out ceased PSCs (ceasedOn is set and non-empty)
        if r["type"] == "HAS_SIGNIFICANT_CONTROL":
            ceased = r["properties"].get("ceasedOn", "")
            if ceased:
                continue
        merged_rels.append({"startId": remap(r["startId"]), "endId": remap(r["endId"]),
                            "type": r["type"], "properties": r["properties"]})

    # Merge Person + Director nodes (same person, different data sources)
    def _norm_name(s):
        return "".join(c for c in (s or "").upper() if c.isalpha() or c == " ").strip()

    def _person_key(n):
        p = n["properties"]
        fn = _norm_name(p.get("forename", ""))
        sn = _norm_name(p.get("surname", ""))
        if fn and sn:
            return sn + "|" + fn
        name = _norm_name(p.get("name", ""))
        parts = name.split()
        titles = {"MR", "MRS", "MS", "MISS", "DR", "SIR", "DAME", "LORD", "LADY", "PROF"}
        if len(parts) > 1 and parts[0] in titles:
            parts = parts[1:]
        if len(parts) >= 2:
            return parts[-1] + "|" + parts[0]
        return name

    def _director_key(n):
        raw_name = (n["properties"].get("name", "") or "").upper().strip()
        if "," in raw_name:
            parts = raw_name.split(",", 1)
            sn = _norm_name(parts[0])
            fn = _norm_name(parts[1]).split()[0] if _norm_name(parts[1]) else ""
            return sn + "|" + fn
        name = _norm_name(raw_name)
        parts = name.split()
        if len(parts) >= 2:
            return parts[-1] + "|" + parts[0]
        return name

    # Build per-company person/director lists
    company_persons = {}  # company_id -> [(node_id, key)]
    company_directors = {}
    for r in merged_rels:
        src = r["startId"]
        dst = r["endId"]
        if src in merged_nodes:
            n = merged_nodes[src]
            if "Person" in n["labels"] and r["type"] == "HAS_SIGNIFICANT_CONTROL":
                company_persons.setdefault(dst, []).append((src, _person_key(n)))
            if "Director" in n["labels"] and r["type"] == "OFFICER_OF":
                company_directors.setdefault(dst, []).append((src, _director_key(n)))

    dir_to_person = {}  # director_id -> person_id
    for co_id in company_persons:
        if co_id not in company_directors:
            continue
        for p_id, pk in company_persons[co_id]:
            if not pk:
                continue
            for d_id, dk in company_directors[co_id]:
                if d_id in dir_to_person:
                    continue
                if pk == dk:
                    dir_to_person[d_id] = p_id

    # Apply Person/Director merge: rewire director edges to person, remove director nodes
    dual_person_ids = set()
    if dir_to_person:
        new_rels = []
        for r in merged_rels:
            src = dir_to_person.get(r["startId"], r["startId"])
            new_rels.append({"startId": src, "endId": r["endId"],
                             "type": r["type"], "properties": r["properties"]})
        merged_rels = new_rels
        for d_id in dir_to_person:
            merged_nodes.pop(d_id, None)
        # Mark merged persons as dual-role
        dual_person_ids = set(dir_to_person.values())

    # Dedup Directors with same name (same person as officer of multiple companies)
    dir_name_to_id = {}  # normalized name -> first director node id
    dir_dedup = {}  # duplicate id -> canonical id
    for nid, n in list(merged_nodes.items()):
        if "Director" in n["labels"]:
            dname = _norm_name(n["properties"].get("name", ""))
            if dname in dir_name_to_id:
                dir_dedup[nid] = dir_name_to_id[dname]
            else:
                dir_name_to_id[dname] = nid
    if dir_dedup:
        merged_rels = [{"startId": dir_dedup.get(r["startId"], r["startId"]),
                        "endId": r["endId"], "type": r["type"], "properties": r["properties"]}
                       for r in merged_rels]
        for dup_id in dir_dedup:
            merged_nodes.pop(dup_id, None)

    # Also dedup Persons with same personId
    person_id_to_nid = {}
    person_dedup = {}
    for nid, n in list(merged_nodes.items()):
        if "Person" in n["labels"]:
            pid = n["properties"].get("personId", "")
            if pid and pid in person_id_to_nid:
                person_dedup[nid] = person_id_to_nid[pid]
            elif pid:
                person_id_to_nid[pid] = nid
    if person_dedup:
        merged_rels = [{"startId": person_dedup.get(r["startId"], r["startId"]),
                        "endId": r["endId"], "type": r["type"], "properties": r["properties"]}
                       for r in merged_rels]
        for dup_id in person_dedup:
            merged_nodes.pop(dup_id, None)

    # Remove orphaned nodes (no remaining edges after filters)
    connected_ids = set()
    for r in merged_rels:
        connected_ids.add(r["startId"])
        connected_ids.add(r["endId"])
    # Keep Company nodes even if orphaned (they're the anchor)
    merged_nodes = {nid: n for nid, n in merged_nodes.items()
                    if nid in connected_ids or "Company" in n["labels"]}

    # Compute hierarchy levels
    levels = _compute_levels(merged_nodes, merged_rels)

    # Compute control weight per node from ownership edges
    # Higher ownership % = bigger node
    def _control_score(noc_list):
        if not noc_list:
            return 0
        noc_str = " ".join(noc_list) if isinstance(noc_list, list) else str(noc_list)
        if "75-to-100" in noc_str:
            return 4
        if "50-to-75" in noc_str:
            return 3
        if "25-to-50" in noc_str:
            return 2
        return 1

    node_max_control = {}  # node_id -> max control score across all relationships
    for r in merged_rels:
        if r["type"] == "HAS_SIGNIFICANT_CONTROL":
            score = _control_score(r["properties"].get("naturesOfControl", []))
            src = r["startId"]
            node_max_control[src] = max(node_max_control.get(src, 0), score)
            # Also boost the controlled company based on how strongly it's controlled
            dst = r["endId"]
            node_max_control[dst] = max(node_max_control.get(dst, 0), score)

    seen_edges = set()
    vis_nodes = []
    for n in merged_nodes.values():
        label = n["labels"][0] if n["labels"] else "Unknown"
        props = n["properties"]
        display = props.get("name", props.get("companyNumber", str(n["id"])))
        if isinstance(display, str) and len(display) > 30:
            display = display[:27] + "..."
        cn = props.get("companyNumber", "")
        if label == "Company" and cn:
            display = f"{display} ({cn})"
        safe_props = {}
        for k, v in props.items():
            safe_props[k] = str(v) if not isinstance(v, list) else ", ".join(str(x) for x in v)
        node_data = {
            "id": n["id"], "label": display, "title": props.get("name", ""),
            "color": label_colors.get(label, "#999"), "group": label,
            "size": node_sizes.get(label, 15) + node_max_control.get(n["id"], 0) * 5,
            "shape": node_shapes.get(label, "dot"),
            "properties": safe_props,
        }
        if n["id"] in levels:
            node_data["level"] = levels[n["id"]]
        if dir_to_person and n["id"] in dual_person_ids:
            node_data["borderWidth"] = 4
            node_data["color"] = {"border": "#FF6600", "background": label_colors.get(label, "#999")}
        vis_nodes.append(node_data)

    def edge_color(rel_type, props):
        if rel_type == "OFFICER_OF":
            role = (props.get("role", "") or "").lower()
            if "secretary" in role:
                return "#78909C", 1.5  # grey-blue for secretaries
            return "#00BCD4", 2.0  # cyan for directors
        noc_list = props.get("naturesOfControl", [])
        if not noc_list:
            return "#666", 1.5
        noc_str = " ".join(noc_list) if isinstance(noc_list, list) else str(noc_list)
        if "75-to-100" in noc_str:
            return "#e74c3c", 3.5
        elif "50-to-75" in noc_str:
            return "#e67e22", 2.5
        elif "25-to-50" in noc_str:
            return "#f1c40f", 2.0
        elif "right-to-appoint" in noc_str:
            return "#9b59b6", 2.0
        elif "significant-influence" in noc_str:
            return "#3498db", 2.0
        return "#95a5a6", 1.5

    vis_edges = []
    for r in merged_rels:
        ek = (r["startId"], r["endId"], r["type"])
        if ek in seen_edges:
            continue
        seen_edges.add(ek)
        props = r["properties"]
        col, width = edge_color(r["type"], props)
        noc_raw = props.get("naturesOfControl", [])
        if isinstance(noc_raw, list):
            noc = ", ".join(
                x.replace("ownership-of-shares-", "shares:").replace("voting-rights-", "votes:")
                 .replace("right-to-appoint-and-remove-directors", "appoint dirs")
                 .replace("significant-influence-or-control", "sig. control")
                 .replace("-as-firm", " (firm)").replace("-as-trust", " (trust)")
                for x in noc_raw)
        else:
            noc = str(noc_raw)
        # Use role as label for OFFICER_OF
        if r["type"] == "OFFICER_OF":
            role = props.get("role", "officer") or "officer"
            elabel = role.replace("_", " ").title()
        else:
            elabel = r["type"].replace("HAS_SIGNIFICANT_CONTROL", "CONTROLS").replace("_", " ")
        tooltip = elabel + (": " + noc if noc else "")
        vis_edges.append({"from": r["startId"], "to": r["endId"],
                          "title": tooltip, "arrows": "to",
                          "color": {"color": col, "highlight": "#fff", "hover": col}, "width": width})

    # Compute positions server-side
    _compute_positions(vis_nodes, vis_edges)

    return {"nodes": vis_nodes, "edges": vis_edges}


def _compute_positions(vis_nodes, vis_edges):
    """Assign x/y positions to nodes. Companies in a grid, satellites orbiting."""
    import math

    company_ids = set()
    node_by_id = {}
    for n in vis_nodes:
        node_by_id[n["id"]] = n
        if n["group"] == "Company":
            company_ids.add(n["id"])

    # Map non-company nodes to their connected companies
    node_to_companies = {}
    for e in vis_edges:
        src, dst = e.get("from"), e.get("to")
        if dst in company_ids and src not in company_ids:
            node_to_companies.setdefault(src, []).append(dst)
        if src in company_ids and dst not in company_ids:
            node_to_companies.setdefault(dst, []).append(src)

    # Count satellites per company (for spacing calculation)
    company_sat_count = {cid: 0 for cid in company_ids}
    for n in vis_nodes:
        if n["id"] in company_ids:
            continue
        companies = [c for c in node_to_companies.get(n["id"], []) if c in company_ids]
        if len(companies) == 1:
            company_sat_count[companies[0]] = company_sat_count.get(companies[0], 0) + 1

    # Group companies by level
    level_companies = {}
    for n in vis_nodes:
        if n["id"] in company_ids:
            lvl = n.get("level", 0) or 0
            level_companies.setdefault(lvl, []).append(n["id"])

    if not level_companies:
        return

    # Dynamic spacing: wider for companies with many satellites
    BASE_RADIUS = 120
    RADIUS_PER_SAT = 15
    MIN_SPACING = 350
    Y_SPACING = 400
    MAX_PER_ROW = 4

    def sat_radius(cid):
        count = company_sat_count.get(cid, 0)
        return BASE_RADIUS + count * RADIUS_PER_SAT

    sorted_levels = sorted(level_companies.keys())
    company_positions = {}

    y_offset = 0
    for lvl in sorted_levels:
        companies = level_companies[lvl]
        rows = [companies[i:i+MAX_PER_ROW] for i in range(0, len(companies), MAX_PER_ROW)]
        for row in rows:
            # Calculate spacing for this row based on max satellite radius
            spacings = []
            for i in range(len(row)):
                r = sat_radius(row[i]) * 2 + 80
                spacings.append(max(MIN_SPACING, r))
            # Place companies with these spacings
            total_width = sum(spacings[:-1]) if len(spacings) > 1 else 0
            x = -total_width / 2
            for i, cid in enumerate(row):
                company_positions[cid] = (x, y_offset)
                node_by_id[cid]["x"] = x
                node_by_id[cid]["y"] = y_offset
                if i < len(row) - 1:
                    x += spacings[i]
            # Row height based on max satellite radius in this row
            max_rad = max(sat_radius(c) for c in row) if row else BASE_RADIUS
            y_offset += max(Y_SPACING, max_rad * 2 + 150)

    # Place satellite nodes
    placed = set()
    orphans = []

    # Multi-company satellites at centroid
    for n in vis_nodes:
        if n["id"] in company_ids:
            continue
        companies = [c for c in node_to_companies.get(n["id"], []) if c in company_positions]
        if len(companies) > 1:
            cx = sum(company_positions[c][0] for c in companies) / len(companies)
            cy = sum(company_positions[c][1] for c in companies) / len(companies)
            node_by_id[n["id"]]["x"] = cx + 50
            node_by_id[n["id"]]["y"] = cy - 80
            placed.add(n["id"])
        elif not companies:
            orphans.append(n["id"])

    # Single-company satellites in orbit
    company_sats = {}
    for n in vis_nodes:
        if n["id"] in company_ids or n["id"] in placed or n["id"] in set(orphans):
            continue
        companies = [c for c in node_to_companies.get(n["id"], []) if c in company_positions]
        if companies:
            company_sats.setdefault(companies[0], []).append(n["id"])

    for cid, sats in company_sats.items():
        cx, cy = company_positions.get(cid, (0, 0))
        radius = sat_radius(cid)
        count = len(sats)
        for i, sid in enumerate(sats):
            angle = (2 * math.pi * i / count) - math.pi / 2
            node_by_id[sid]["x"] = cx + radius * math.cos(angle)
            node_by_id[sid]["y"] = cy + radius * math.sin(angle)

    # Orphans below
    if orphans:
        max_y = max(p[1] for p in company_positions.values()) if company_positions else 0
        oy = max_y + Y_SPACING
        tw = (len(orphans) - 1) * 150
        for i, oid in enumerate(orphans):
            node_by_id[oid]["x"] = -tw / 2 + i * 150
            node_by_id[oid]["y"] = oy
