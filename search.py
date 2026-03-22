#!/usr/bin/env python3
"""
Companies House Search & Export Tool

Search the Neo4j graph and export results as:
  - CSV (tabular data)
  - JSON (structured data)
  - HTML (interactive graph visualization you can share)

Usage:
  # Search by company number and export ownership tree
  python search.py --company 00253240

  # Search by company name
  python search.py --name "national car parks"

  # Search by person surname
  python search.py --person "Smith" --limit 20

  # Custom Cypher query
  python search.py --cypher "MATCH (c:Company)-[:HAS_SIC]->(s:SICCode {code: '62020'}) RETURN c, s LIMIT 50"

  # Control output formats
  python search.py --company 00253240 --format csv json html

  # Set max ownership depth
  python search.py --company 00253240 --depth 20
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import requests
from neo4j import GraphDatabase

# Load .env file if present
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip("'\""))

DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "companies2026"
CH_API_BASE = "https://api.company-information.service.gov.uk"
RATE_LIMIT_DELAY = 0.2


# ---------------------------------------------------------------------------
# Directors API fetch + load (with ETag freshness tracking)
# ---------------------------------------------------------------------------
STALE_DAYS = 30


def fetch_officers(api_key, company_number, etag=None):
    """Fetch officers from CH API. Returns (officers, etag, modified)."""
    officers = []
    start_index = 0
    response_etag = None

    while True:
        headers = {}
        if etag and start_index == 0:
            headers["If-None-Match"] = etag
        try:
            resp = requests.get(
                f"{CH_API_BASE}/company/{company_number}/officers",
                auth=(api_key, ""),
                params={"start_index": start_index, "items_per_page": 100},
                headers=headers,
                timeout=10,
            )
        except requests.Timeout:
            return officers, response_etag, True

        if resp.status_code == 304:
            return [], etag, False
        if start_index == 0:
            response_etag = resp.headers.get("ETag", "")
        if resp.status_code == 404:
            return [], None, True
        if resp.status_code == 429:
            time.sleep(60)
            continue
        if resp.status_code != 200:
            return officers, response_etag, True

        data = resp.json()
        items = data.get("items", [])
        officers.extend(items)
        start_index += len(items)
        if start_index >= data.get("total_results", 0) or not items:
            break
        time.sleep(RATE_LIMIT_DELAY)
    return officers, response_etag, True


def make_director_id(officer):
    links = officer.get("links", {})
    self_link = links.get("self", "")
    if self_link:
        return self_link
    name = officer.get("name", "")
    dob = officer.get("date_of_birth", {})
    return f"{name}|{dob.get('month', '')}|{dob.get('year', '')}".upper()


UPSERT_DIRECTOR_QUERY = """
UNWIND $batch AS row
MATCH (c:Company {companyNumber: row.companyNumber})
MERGE (d:Director {directorId: row.directorId})
SET d.name         = row.name,
    d.nationality  = row.nationality,
    d.occupation   = row.occupation,
    d.countryOfResidence = row.countryOfResidence
MERGE (d)-[r:OFFICER_OF]->(c)
SET r.role         = row.role,
    r.appointedOn  = row.appointedOn,
    r.resignedOn   = row.resignedOn
"""

STAMP_FETCH_QUERY = """
MATCH (c:Company {companyNumber: $cn})
SET c.directorsFetchedAt = datetime(),
    c.directorsEtag      = $etag
"""


def load_officers_to_neo4j(driver, company_number, officers, etag=None):
    batch = []
    for o in officers:
        batch.append({
            "companyNumber": company_number,
            "directorId": make_director_id(o),
            "name": o.get("name", ""),
            "nationality": o.get("nationality", ""),
            "occupation": o.get("occupation", ""),
            "countryOfResidence": o.get("country_of_residence", ""),
            "role": o.get("officer_role", ""),
            "appointedOn": o.get("appointed_on", ""),
            "resignedOn": o.get("resigned_on", ""),
        })
    with driver.session() as session:
        if batch:
            session.run(UPSERT_DIRECTOR_QUERY, batch=batch)
        session.run(STAMP_FETCH_QUERY, cn=company_number, etag=etag or "")
    return len(batch)


def ensure_directors_for_tree(driver, api_key, company_number):
    """Fetch directors for all companies in the ownership tree.
    Skips companies fetched within STALE_DAYS. Uses ETags for conditional requests."""
    print("Checking for director data in the ownership tree...")

    # Get all company numbers in the tree
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Company {companyNumber: $cn}) "
            "CALL apoc.path.expandConfig(c, {"
            "  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY>',"
            "  minLevel: 1, maxLevel: 30,"
            "  uniqueness: 'NODE_GLOBAL'"
            "}) YIELD path "
            "UNWIND nodes(path) AS n WITH n WHERE n:Company "
            "RETURN DISTINCT n.companyNumber AS cn",
            cn=company_number,
        )
        tree_companies = [r["cn"] for r in result if r["cn"]]
    if company_number not in tree_companies:
        tree_companies.insert(0, company_number)

    # Get freshness metadata in one query
    with driver.session() as session:
        result = session.run(
            "UNWIND $cns AS cn MATCH (c:Company {companyNumber: cn}) "
            "RETURN cn, c.directorsFetchedAt AS fetchedAt, c.directorsEtag AS etag",
            cns=tree_companies,
        )
        meta = {r["cn"]: {"fetchedAt": r["fetchedAt"], "etag": r["etag"]} for r in result}

    # Filter to stale/missing companies
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    to_fetch = []
    for cn in tree_companies:
        m = meta.get(cn, {"fetchedAt": None, "etag": None})
        fetched_at = m["fetchedAt"]
        if fetched_at is None:
            to_fetch.append((cn, m["etag"]))
        else:
            age = now - fetched_at.to_native()
            if age.days >= STALE_DAYS:
                to_fetch.append((cn, m["etag"]))

    if not to_fetch:
        fresh = len(tree_companies)
        print(f"  All {fresh} companies have fresh director data (< {STALE_DAYS} days).")
        return

    print(f"  {len(to_fetch)}/{len(tree_companies)} companies need director data...")

    with driver.session() as session:
        session.run("CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.name)")

    total = 0
    unchanged = 0
    for i, (cn, old_etag) in enumerate(to_fetch, 1):
        print(f"    [{i}/{len(to_fetch)}] {cn}...", end=" ", flush=True)
        officers, new_etag, modified = fetch_officers(api_key, cn, etag=old_etag)

        if not modified:
            with driver.session() as session:
                session.run(STAMP_FETCH_QUERY, cn=cn, etag=old_etag)
            unchanged += 1
            print("unchanged")
            continue

        loaded = load_officers_to_neo4j(driver, cn, officers, etag=new_etag)
        total += loaded
        print(f"{loaded} officers")
        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Done: {total} officers loaded, {unchanged} unchanged.")


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------
def ownership_tree_query(company_number, depth):
    # Ownership chain only (PSC + corporate cross-refs). Directors added separately.
    return (
        f"MATCH (c:Company {{companyNumber: '{company_number}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 1, maxLevel: {depth}, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path "
        f"RETURN path"
    )


def directors_for_tree_query(company_number, depth):
    """Get directors for all companies in the ownership tree."""
    return (
        f"MATCH (c:Company {{companyNumber: '{company_number}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 0, maxLevel: {depth}, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path "
        f"UNWIND nodes(path) AS n "
        f"WITH n WHERE n:Company "
        f"MATCH dirPath = (d:Director)-[:OFFICER_OF]->(n) "
        f"RETURN dirPath AS path"
    )


def company_name_query(name, limit):
    return (
        f"CALL db.index.fulltext.queryNodes('company_name_fulltext', $name) "
        f"YIELD node, score "
        f"MATCH (node)<-[r:HAS_SIGNIFICANT_CONTROL]-(psc) "
        f"RETURN node, r, psc, score ORDER BY score DESC LIMIT {limit}"
    )


def person_query(name, limit):
    return (
        f"CALL db.index.fulltext.queryNodes('person_name_fulltext', $name) "
        f"YIELD node, score "
        f"MATCH (node)-[r:HAS_SIGNIFICANT_CONTROL]->(c:Company) "
        f"RETURN node, r, c, score ORDER BY score DESC LIMIT {limit}"
    )


# ---------------------------------------------------------------------------
# Data extraction from Neo4j results
# ---------------------------------------------------------------------------
def extract_graph_data(records):
    """Extract nodes and relationships from Neo4j records into plain dicts."""
    nodes = {}
    rels = []
    flat_rows = []

    for record in records:
        row = {}
        for key in record.keys():
            val = record[key]
            _extract_value(val, nodes, rels)
            # Flatten for CSV
            if hasattr(val, 'labels'):
                for prop_key, prop_val in dict(val).items():
                    row[f"{key}.{prop_key}"] = prop_val
            elif hasattr(val, 'type'):
                for prop_key, prop_val in dict(val).items():
                    row[f"{key}.{prop_key}"] = prop_val
            elif hasattr(val, '__iter__') and not isinstance(val, (str, dict)):
                pass  # path — already extracted
            else:
                row[key] = val
        if row:
            flat_rows.append(row)

    return nodes, rels, flat_rows


def _extract_value(val, nodes, rels):
    """Recursively extract nodes and relationships from a value."""
    if hasattr(val, 'labels'):  # Node
        nid = val.element_id
        if nid not in nodes:
            props = dict(val)
            labels = list(val.labels)
            nodes[nid] = {"id": nid, "labels": labels, "properties": props}
    elif hasattr(val, 'type'):  # Relationship
        props = dict(val)
        rels.append({
            "startId": val.start_node.element_id,
            "endId": val.end_node.element_id,
            "type": val.type,
            "properties": props,
        })
        _extract_value(val.start_node, nodes, rels)
        _extract_value(val.end_node, nodes, rels)
    elif hasattr(val, 'nodes'):  # Path
        for node in val.nodes:
            _extract_value(node, nodes, rels)
        for rel in val.relationships:
            _extract_value(rel, nodes, rels)


# ---------------------------------------------------------------------------
# Export: CSV
# ---------------------------------------------------------------------------
def export_csv(nodes, rels, flat_rows, output_path):
    """Export nodes and relationships as CSV files."""
    base = output_path.replace('.csv', '')

    # Nodes CSV
    node_rows = []
    for n in nodes.values():
        row = {"_id": n["id"], "_labels": ";".join(n["labels"])}
        row.update(n["properties"])
        node_rows.append(row)

    if node_rows:
        all_keys = list(dict.fromkeys(k for r in node_rows for k in r.keys()))
        path = f"{base}_nodes.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
            w.writeheader()
            w.writerows(node_rows)
        print(f"  Nodes CSV: {path} ({len(node_rows)} rows)")

    # Relationships CSV
    if rels:
        rel_rows = []
        for r in rels:
            row = {"_start": r["startId"], "_end": r["endId"], "_type": r["type"]}
            row.update(r["properties"])
            rel_rows.append(row)
        all_keys = list(dict.fromkeys(k for r in rel_rows for k in r.keys()))
        path = f"{base}_rels.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
            w.writeheader()
            w.writerows(rel_rows)
        print(f"  Rels CSV:  {path} ({len(rel_rows)} rows)")


# ---------------------------------------------------------------------------
# Export: JSON
# ---------------------------------------------------------------------------
def export_json(nodes, rels, flat_rows, output_path):
    """Export as a JSON file."""
    data = {
        "nodes": list(nodes.values()),
        "relationships": rels,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  JSON: {output_path} ({len(nodes)} nodes, {len(rels)} relationships)")


# ---------------------------------------------------------------------------
# Export: Interactive HTML
# ---------------------------------------------------------------------------
def _compute_levels(merged_nodes, merged_rels, root_hint=None):
    """Compute hierarchical levels from merged graph via BFS.
    After merging, CONTROLS edges go from parent (PSC/Company) -> child (Company).
    We find the deepest child (the search target) and BFS upward."""
    from collections import deque

    # Build parent->children map from CONTROLS and OFFICER_OF edges
    # CONTROLS: startId controls endId, so startId is the parent
    # OFFICER_OF: startId is officer of endId, so startId is the parent
    parents_of = {}  # childId -> [parentId]
    for r in merged_rels:
        if r["type"] in ("HAS_SIGNIFICANT_CONTROL", "OFFICER_OF"):
            parents_of.setdefault(r["endId"], []).append(r["startId"])

    # Find root: the node that is controlled (appears as endId) and has the most
    # incoming CONTROLS. Or use root_hint if provided.
    if root_hint and root_hint in merged_nodes:
        root = root_hint
    else:
        incoming_count = {}
        for r in merged_rels:
            if r["type"] in ("HAS_SIGNIFICANT_CONTROL", "OFFICER_OF"):
                incoming_count[r["endId"]] = incoming_count.get(r["endId"], 0) + 1
        if incoming_count:
            root = max(incoming_count, key=incoming_count.get)
        else:
            return {}

    # BFS upward from root: root gets highest level number (bottom of tree)
    # Parents get lower level numbers (top of tree)
    levels = {root: 100}  # start high so we can go negative if needed
    queue = deque([root])
    visited = {root}

    while queue:
        nid = queue.popleft()
        for parent in parents_of.get(nid, []):
            if parent not in visited:
                visited.add(parent)
                levels[parent] = levels[nid] - 1
                queue.append(parent)

    # Also walk downward (in case root controls other companies)
    children_of = {}
    for r in merged_rels:
        if r["type"] in ("HAS_SIGNIFICANT_CONTROL", "OFFICER_OF"):
            children_of.setdefault(r["startId"], []).append(r["endId"])

    queue = deque([root])
    while queue:
        nid = queue.popleft()
        for child in children_of.get(nid, []):
            if child not in visited:
                visited.add(child)
                levels[child] = levels[nid] + 1
                queue.append(child)

    # Normalize so min level = 0 (top of tree)
    if levels:
        min_level = min(levels.values())
        levels = {k: v - min_level for k, v in levels.items()}

    return levels


def export_html(nodes, rels, flat_rows, output_path):
    """Export as a self-contained interactive HTML graph using vis-network CDN."""
    label_colors = {
        "Company": "#4C8BF5",
        "Person": "#34A853",
        "CorporateEntity": "#FBBC04",
        "LegalPerson": "#EA4335",
        "Director": "#00BCD4",
        "SICCode": "#9C27B0",
        "Address": "#607D8B",
    }

    node_sizes = {
        "Company": 25,
        "Person": 15,
        "CorporateEntity": 20,
        "LegalPerson": 18,
        "Director": 15,
        "SICCode": 12,
        "Address": 12,
    }

    node_shapes = {
        "Company": "dot",
        "Person": "diamond",
        "CorporateEntity": "square",
        "LegalPerson": "triangle",
        "Director": "triangleDown",
        "SICCode": "star",
        "Address": "hexagon",
    }

    # Merge CorporateEntity nodes into their linked Company nodes
    # When IS_COMPANY links CorporateEntity -> Company, they represent the same
    # real-world entity. Merge them into one visual node (keep Company).
    ce_to_company = {}  # CorporateEntity id -> Company id
    for r in rels:
        if r["type"] == "IS_COMPANY":
            ce_id = r["startId"]
            co_id = r["endId"]
            if ce_id in nodes and co_id in nodes:
                ce_node = nodes[ce_id]
                co_node = nodes[co_id]
                if "CorporateEntity" in ce_node["labels"] and "Company" in co_node["labels"]:
                    ce_to_company[ce_id] = co_id

    # Build merged node set (skip CorporateEntities that map to a Company)
    merged_nodes = {nid: n for nid, n in nodes.items() if nid not in ce_to_company}

    # For merged Company nodes, add the CorporateEntity's extra properties as context
    for ce_id, co_id in ce_to_company.items():
        ce_props = nodes[ce_id]["properties"]
        co_node = merged_nodes[co_id]
        for k in ("legalForm", "legalAuthority", "countryRegistered", "placeRegistered"):
            if k in ce_props and ce_props[k]:
                co_node["properties"].setdefault(k, ce_props[k])

    def remap(nid):
        return ce_to_company.get(nid, nid)

    # Remap relationships — skip IS_COMPANY (now redundant), redirect others
    merged_rels = []
    for r in rels:
        if r["type"] == "IS_COMPANY" and r["startId"] in ce_to_company:
            continue  # drop merged IS_COMPANY edges
        merged_rels.append({
            "startId": remap(r["startId"]),
            "endId": remap(r["endId"]),
            "type": r["type"],
            "properties": r["properties"],
        })

    # Compute hierarchy levels on the MERGED graph
    levels = _compute_levels(merged_nodes, merged_rels)

    # Deduplicate relationships
    seen_edges = set()

    vis_nodes = []
    for n in merged_nodes.values():
        label = n["labels"][0] if n["labels"] else "Unknown"
        props = n["properties"]
        # Pick a display name
        display = props.get("name", props.get("companyNumber",
                  props.get("code", props.get("postcode",
                  props.get("personId", props.get("entityId", str(n["id"])))))))
        # Truncate long names
        if isinstance(display, str) and len(display) > 50:
            display = display[:47] + "..."

        # Add type and key info to label
        cn = props.get("companyNumber", "")
        if label == "Company" and cn:
            display = f"{display}\n({cn})"

        # Tooltip with all properties
        title_lines = [f"<b>{label}</b>"]
        safe_props = {}
        for k, v in props.items():
            val = str(v) if not isinstance(v, list) else ", ".join(str(x) for x in v)
            if len(val) > 200:
                val = val[:197] + "..."
            title_lines.append(f"{k}: {val}")
            safe_props[k] = val
        title = "<br>".join(title_lines)

        color = label_colors.get(label, "#999999")
        node_data = {
            "id": n["id"],
            "label": display,
            "title": title,
            "color": color,
            "group": label,
            "size": node_sizes.get(label, 15),
            "shape": node_shapes.get(label, "dot"),
            "properties": safe_props,
        }
        if n["id"] in levels:
            node_data["level"] = levels[n["id"]]
        vis_nodes.append(node_data)

    # Edge color coding by control level
    def edge_color_and_width(rel_type, noc_list):
        """Return (color, width) based on relationship type and natures of control."""
        if rel_type == "OFFICER_OF":
            return "#00BCD4", 2.0   # Cyan — director/officer
        if not noc_list:
            return "#666", 1.5
        noc_str = " ".join(noc_list) if isinstance(noc_list, list) else str(noc_list)
        if "75-to-100" in noc_str:
            return "#e74c3c", 3.5   # Red — dominant control
        elif "50-to-75" in noc_str:
            return "#e67e22", 2.5   # Orange — majority
        elif "25-to-50" in noc_str:
            return "#f1c40f", 2.0   # Yellow — significant minority
        elif "right-to-appoint" in noc_str:
            return "#9b59b6", 2.0   # Purple — director appointment rights
        elif "significant-influence" in noc_str:
            return "#3498db", 2.0   # Blue — significant influence
        else:
            return "#95a5a6", 1.5   # Grey — other

    vis_edges = []
    for r in merged_rels:
        edge_key = (r["startId"], r["endId"], r["type"])
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        noc_raw = r["properties"].get("naturesOfControl", [])
        edge_col, edge_width = edge_color_and_width(r["type"], noc_raw)

        if isinstance(noc_raw, list):
            noc = ", ".join(
                x.replace("ownership-of-shares-", "shares:")
                 .replace("voting-rights-", "votes:")
                 .replace("right-to-appoint-and-remove-directors", "appoint directors")
                 .replace("significant-influence-or-control", "significant control")
                 .replace("-as-firm", " (firm)")
                 .replace("-as-trust", " (trust)")
                for x in noc_raw
            )
        else:
            noc = str(noc_raw)

        # For OFFICER_OF, show role instead of natures_of_control
        if r["type"] == "OFFICER_OF":
            role = r["properties"].get("role", "")
            appointed = r["properties"].get("appointedOn", "")
            resigned = r["properties"].get("resignedOn", "")
            tooltip_parts = [role] if role else []
            if appointed:
                tooltip_parts.append(f"appointed: {appointed}")
            if resigned:
                tooltip_parts.append(f"resigned: {resigned}")
            noc = ", ".join(tooltip_parts) or "Officer"

        edge_label = r["type"].replace("HAS_SIGNIFICANT_CONTROL", "CONTROLS").replace("OFFICER_OF", "DIRECTOR").replace("_", " ")
        vis_edges.append({
            "from": r["startId"],
            "to": r["endId"],
            "label": edge_label,
            "title": noc or r["type"],
            "arrows": "to",
            "color": {"color": edge_col, "highlight": "#fff", "hover": edge_col},
            "width": edge_width,
        })

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Companies House Graph Export</title>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>
    body {{ margin: 0; font-family: -apple-system, sans-serif; background: #1a1a2e; color: #eee; }}
    #graph {{ width: 100vw; height: 100vh; }}
    #controls {{ position: fixed; top: 10px; left: 10px; background: rgba(0,0,0,0.8);
             padding: 14px 18px; border-radius: 10px; font-size: 13px; z-index: 10; max-width: 320px; }}
    #controls h3 {{ margin: 0 0 8px 0; font-size: 16px; }}
    .legend {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }}
    .legend-item {{ display: flex; align-items: center; gap: 5px; }}
    .legend-dot {{ width: 12px; height: 12px; border-radius: 2px; }}
    .btn {{ background: #2a2a4a; border: 1px solid #4a4a7a; color: #ccc; padding: 6px 12px;
            border-radius: 5px; cursor: pointer; font-size: 12px; margin: 2px; }}
    .btn:hover {{ background: #3a3a6a; color: #fff; }}
    .btn-row {{ margin-top: 8px; display: flex; flex-wrap: wrap; gap: 4px; }}
    #details {{ position: fixed; bottom: 10px; right: 10px; background: rgba(0,0,0,0.85);
             padding: 14px 18px; border-radius: 10px; font-size: 12px; z-index: 10;
             max-width: 400px; max-height: 50vh; overflow-y: auto; display: none; }}
    #details h4 {{ margin: 0 0 6px 0; }}
    #details table {{ border-collapse: collapse; width: 100%; }}
    #details td {{ padding: 2px 6px; border-bottom: 1px solid #333; vertical-align: top; word-break: break-all; }}
    #details td:first-child {{ color: #aaa; white-space: nowrap; }}
  </style>
</head>
<body>
  <div id="controls">
    <h3>Companies House Ownership Tree</h3>
    <div>{len(vis_nodes)} nodes, {len(vis_edges)} relationships</div>
    <div class="legend">
      {"".join(f'<span class="legend-item"><span class="legend-dot" style="background:{c}"></span>{l}</span>' for l, c in label_colors.items())}
    </div>
    <div style="margin-top: 8px; font-size: 11px; color: #aaa;">Arrow colours:</div>
    <div class="legend" style="margin-top: 2px;">
      <span class="legend-item"><span class="legend-dot" style="background:#e74c3c"></span>75-100%</span>
      <span class="legend-item"><span class="legend-dot" style="background:#e67e22"></span>50-75%</span>
      <span class="legend-item"><span class="legend-dot" style="background:#f1c40f"></span>25-50%</span>
      <span class="legend-item"><span class="legend-dot" style="background:#9b59b6"></span>Directors</span>
      <span class="legend-item"><span class="legend-dot" style="background:#3498db"></span>Influence</span>
      <span class="legend-item"><span class="legend-dot" style="background:#00BCD4"></span>Director</span>
    </div>
    <div class="btn-row">
      <button class="btn" onclick="toggleLayout()">Toggle Layout</button>
      <button class="btn" onclick="network.fit()">Fit View</button>
      <button class="btn" onclick="togglePhysics()">Toggle Physics</button>
    </div>
  </div>
  <div id="details"></div>
  <div id="graph"></div>
  <script>
    var nodes = new vis.DataSet({json.dumps(vis_nodes, default=str)});
    var edges = new vis.DataSet({json.dumps(vis_edges, default=str)});
    var container = document.getElementById('graph');
    var data = {{ nodes: nodes, edges: edges }};
    var hierarchical = true;
    var physicsOn = true;

    var hierarchicalOptions = {{
      layout: {{
        hierarchical: {{
          enabled: true,
          direction: 'UD',
          sortMethod: 'directed',
          levelSeparation: 150,
          nodeSpacing: 200,
          treeSpacing: 250,
          blockShifting: true,
          edgeMinimization: true,
          parentCentralization: true
        }}
      }},
      physics: {{
        enabled: true,
        hierarchicalRepulsion: {{
          centralGravity: 0.0,
          springLength: 150,
          springConstant: 0.01,
          nodeDistance: 200,
          damping: 0.09
        }},
        stabilization: {{ iterations: 300 }}
      }},
      nodes: {{
        font: {{ color: '#eee', size: 11, multi: 'md' }},
        borderWidth: 2
      }},
      edges: {{
        color: {{ color: '#666', highlight: '#fff', hover: '#aaa' }},
        font: {{ color: '#999', size: 9, strokeWidth: 0 }},
        smooth: {{ type: 'cubicBezier', forceDirection: 'vertical', roundness: 0.4 }},
        width: 1.5
      }},
      interaction: {{ hover: true, tooltipDelay: 100, navigationButtons: true, keyboard: true }}
    }};

    var forceOptions = {{
      layout: {{ hierarchical: {{ enabled: false }} }},
      physics: {{
        solver: 'forceAtlas2Based',
        forceAtlas2Based: {{ gravitationalConstant: -100, springLength: 180 }},
        stabilization: {{ iterations: 300 }}
      }},
      nodes: hierarchicalOptions.nodes,
      edges: {{
        color: hierarchicalOptions.edges.color,
        font: hierarchicalOptions.edges.font,
        smooth: {{ type: 'continuous' }},
        width: 1.5
      }},
      interaction: hierarchicalOptions.interaction
    }};

    var network = new vis.Network(container, data, hierarchical ? hierarchicalOptions : forceOptions);

    function toggleLayout() {{
      hierarchical = !hierarchical;
      network.setOptions(hierarchical ? hierarchicalOptions : forceOptions);
    }}
    function togglePhysics() {{
      physicsOn = !physicsOn;
      network.setOptions({{ physics: {{ enabled: physicsOn }} }});
    }}

    // Click node to show details — use stored properties object
    network.on('click', function(params) {{
      var panel = document.getElementById('details');
      if (params.nodes.length > 0) {{
        var nodeId = params.nodes[0];
        var node = nodes.get(nodeId);
        var props = node.properties || {{}};
        var h = '<h4>' + escHtml(node.group || '') + '</h4><table>';
        Object.keys(props).forEach(function(key) {{
          h += '<tr><td>' + escHtml(key) + '</td><td>' + escHtml(props[key]) + '</td></tr>';
        }});
        h += '</table>';
        panel.innerHTML = h;
        panel.style.display = 'block';
      }} else {{
        panel.style.display = 'none';
      }}
    }});

    function escHtml(s) {{
      var d = document.createElement('div');
      d.textContent = s;
      return d.innerHTML;
    }}
  </script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  HTML: {output_path} ({len(vis_nodes)} nodes, {len(vis_edges)} edges)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
EXPORTERS = {
    "csv": export_csv,
    "json": export_json,
    "html": export_html,
}


def main():
    parser = argparse.ArgumentParser(description="Search Companies House graph and export results")
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--company", help="Company number — exports full ownership tree")
    group.add_argument("--name", help="Company name search (full-text)")
    group.add_argument("--person", help="Person name search (full-text)")
    group.add_argument("--cypher", help="Custom Cypher query")

    parser.add_argument("--depth", type=int, default=20, help="Max ownership tree depth (default: 20)")
    parser.add_argument("--limit", type=int, default=50, help="Max results for name/person search")
    parser.add_argument("--format", nargs="+", default=["html", "json"], choices=["csv", "json", "html"],
                        help="Output formats (default: html json)")
    parser.add_argument("--output", default="export", help="Output filename prefix (default: export)")
    parser.add_argument("--api-key", default=os.environ.get("CH_API_KEY", os.environ.get("CH_API", "")),
                        help="Companies House API key for director data (or set CH_API_KEY env var)")
    parser.add_argument("--no-directors", action="store_true", help="Skip fetching director data")
    args = parser.parse_args()

    # Build query
    params = {}
    if args.company:
        query = ownership_tree_query(args.company, args.depth)
        args.output = args.output if args.output != "export" else f"export_{args.company}"
    elif args.name:
        query = company_name_query(args.name, args.limit)
        params = {"name": args.name}
    elif args.person:
        query = person_query(args.person, args.limit)
        params = {"name": args.person}
    else:
        query = args.cypher

    print(f"Connecting to {args.uri}...")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()

    # Auto-fetch directors for the ownership tree if API key is available
    if args.company and args.api_key and not args.no_directors:
        ensure_directors_for_tree(driver, args.api_key, args.company)

    print(f"Running query...")
    with driver.session() as session:
        result = session.run(query, **params)
        records = list(result)

        # If --company, also fetch director relationships as a second query
        if args.company and not args.no_directors:
            dir_query = directors_for_tree_query(args.company, args.depth)
            dir_result = session.run(dir_query)
            dir_records = list(dir_result)
            if dir_records:
                print(f"  + {len(dir_records)} director records")
                records.extend(dir_records)

    driver.close()

    if not records:
        print("No results found.")
        return

    print(f"Processing {len(records)} records...")
    nodes, rels, flat_rows = extract_graph_data(records)
    print(f"  Found {len(nodes)} nodes, {len(rels)} relationships")

    os.makedirs("exports", exist_ok=True)

    for fmt in args.format:
        ext = fmt if fmt != "html" else "html"
        path = f"exports/{args.output}.{ext}"
        EXPORTERS[fmt](nodes, rels, flat_rows, path)

    print(f"\nDone! Files in exports/")


if __name__ == "__main__":
    main()
