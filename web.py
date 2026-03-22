#!/usr/bin/env python3
"""
CH Graphs — Web Interface

A simple Flask app that lets you search for a company and view its
ownership tree as an interactive graph.
"""

import os
import sys
import time
from pathlib import Path

from flask import Flask, render_template_string, request, redirect, url_for, send_from_directory, jsonify
from neo4j import GraphDatabase

# Load .env
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip("'\""))

# Import director fetching from fetch_directors.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch_directors import fetch_officers, load_officers_to_neo4j, make_director_id, get_tree_company_numbers, get_fetch_metadata, needs_refresh, STAMP_FETCH_QUERY, CREATE_INDEXES_QUERY
from search import extract_graph_data, export_html, _compute_levels

app = Flask(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "companies2026")
CH_API_KEY = os.environ.get("CH_API_KEY", os.environ.get("CH_API", ""))
RATE_LIMIT_DELAY = 0.2

driver = None


def get_driver():
    global driver
    if driver is None:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
    return driver


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------
HOME_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>CH Graphs — UK Company Ownership Search</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0f0f1a; color: #eee; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; }
    h1 { font-size: 2em; margin-bottom: 8px; }
    .subtitle { color: #888; margin-bottom: 32px; }
    .search-box { display: flex; gap: 8px; margin-bottom: 24px; }
    input[type=text] { padding: 12px 16px; font-size: 16px; border: 1px solid #333; border-radius: 8px; background: #1a1a2e; color: #eee; width: 350px; outline: none; }
    input[type=text]:focus { border-color: #4C8BF5; }
    button { padding: 12px 24px; font-size: 16px; border: none; border-radius: 8px; background: #4C8BF5; color: #fff; cursor: pointer; }
    button:hover { background: #3a7be0; }
    .or { color: #666; margin: 16px 0; }
    .name-search { display: flex; gap: 8px; }
    .results { margin-top: 24px; width: 600px; }
    .result-item { padding: 12px 16px; border: 1px solid #333; border-radius: 8px; margin-bottom: 8px; background: #1a1a2e; cursor: pointer; transition: border-color 0.2s; }
    .result-item:hover { border-color: #4C8BF5; }
    .result-item .name { font-weight: bold; }
    .result-item .meta { color: #888; font-size: 13px; margin-top: 4px; }
    a { color: #4C8BF5; text-decoration: none; }
    .recent { margin-top: 40px; width: 600px; }
    .recent h3 { color: #888; font-size: 14px; margin-bottom: 8px; }
    .status { color: #f1c40f; margin-top: 12px; font-size: 14px; }
    .error { color: #e74c3c; }
  </style>
</head>
<body>
  <h1>CH Graphs</h1>
  <p class="subtitle">UK Company Ownership Explorer</p>

  <form class="search-box" action="/graph" method="get">
    <input type="text" name="company" placeholder="Company number (e.g. 00253240)" pattern="[A-Za-z0-9]+" required>
    <button type="submit">View Graph</button>
  </form>

  <p class="or">— or search by name —</p>

  <form class="name-search" action="/search" method="get">
    <input type="text" name="q" placeholder="Company name (e.g. Tesco)" required>
    <button type="submit">Search</button>
  </form>

  {% if error %}<p class="error">{{ error }}</p>{% endif %}

  {% if recent %}
  <div class="recent">
    <h3>Recent exports</h3>
    {% for f in recent %}
    <div class="result-item" onclick="window.location='/exports/{{ f }}'">
      <span class="name">{{ f }}</span>
    </div>
    {% endfor %}
  </div>
  {% endif %}
</body>
</html>"""

LOADING_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Loading {{ company }}...</title>
  <meta http-equiv="refresh" content="3;url=/graph?company={{ company }}">
  <style>
    body { font-family: -apple-system, sans-serif; background: #0f0f1a; color: #eee; display: flex; align-items: center; justify-content: center; min-height: 100vh; flex-direction: column; }
    .spinner { width: 40px; height: 40px; border: 4px solid #333; border-top: 4px solid #4C8BF5; border-radius: 50%; animation: spin 1s linear infinite; margin-bottom: 24px; }
    @keyframes spin { to { transform: rotate(360deg); } }
    p { color: #888; }
  </style>
</head>
<body>
  <div class="spinner"></div>
  <h2>Building ownership graph for {{ company }}</h2>
  <p>{{ status }}</p>
  <p style="margin-top: 12px; color: #666;">This page will auto-refresh...</p>
</body>
</html>"""

SEARCH_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Search: {{ q }}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, sans-serif; background: #0f0f1a; color: #eee; padding: 40px; }
    h2 { margin-bottom: 16px; }
    a { color: #4C8BF5; text-decoration: none; }
    .back { margin-bottom: 24px; display: inline-block; }
    .result-item { padding: 12px 16px; border: 1px solid #333; border-radius: 8px; margin-bottom: 8px; background: #1a1a2e; max-width: 700px; }
    .result-item:hover { border-color: #4C8BF5; }
    .name { font-weight: bold; }
    .name a { color: #eee; }
    .name a:hover { color: #4C8BF5; }
    .meta { color: #888; font-size: 13px; margin-top: 4px; }
    .none { color: #666; }
  </style>
</head>
<body>
  <a class="back" href="/">← Back</a>
  <h2>Results for "{{ q }}"</h2>
  {% if results %}
    {% for r in results %}
    <div class="result-item">
      <div class="name"><a href="/graph?company={{ r.number }}">{{ r.name }}</a></div>
      <div class="meta">{{ r.number }} · {{ r.status }} · {{ r.category }}</div>
    </div>
    {% endfor %}
  {% else %}
    <p class="none">No results found.</p>
  {% endif %}
</body>
</html>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def home():
    # List recent exports
    exports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
    recent = []
    if os.path.isdir(exports_dir):
        files = sorted(
            [f for f in os.listdir(exports_dir) if f.endswith(".html") and f != ".gitkeep"],
            key=lambda f: os.path.getmtime(os.path.join(exports_dir, f)),
            reverse=True,
        )
        recent = files[:10]
    return render_template_string(HOME_TEMPLATE, recent=recent, error=request.args.get("error"))


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return redirect(url_for("home"))

    d = get_driver()
    with d.session() as session:
        result = session.run(
            "CALL db.index.fulltext.queryNodes('company_name_fulltext', $q) "
            "YIELD node, score "
            "RETURN node.name AS name, node.companyNumber AS number, "
            "node.status AS status, node.category AS category, score "
            "ORDER BY score DESC LIMIT 20",
            q=q,
        )
        results = [dict(r) for r in result]

    return render_template_string(SEARCH_TEMPLATE, q=q, results=results)


@app.route("/graph")
def graph():
    company = request.args.get("company", "").strip().upper()
    if not company:
        return redirect(url_for("home"))

    exports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
    html_path = os.path.join(exports_dir, f"export_{company}.html")

    # If export already exists and is recent (< 1 day), serve it
    if os.path.exists(html_path):
        age_hours = (time.time() - os.path.getmtime(html_path)) / 3600
        if age_hours < 24:
            return send_from_directory(exports_dir, f"export_{company}.html")

    # Otherwise, generate it
    d = get_driver()

    # Check the company exists
    with d.session() as session:
        result = session.run(
            "MATCH (c:Company {companyNumber: $cn}) RETURN c.name AS name",
            cn=company,
        )
        record = result.single()
        if not record:
            return redirect(url_for("home", error=f"Company {company} not found"))

    # Fetch directors if API key available
    if CH_API_KEY:
        _ensure_directors(d, company)

    # Run ownership tree query
    query = (
        f"MATCH (c:Company {{companyNumber: '{company}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 1, maxLevel: 30, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path "
        f"RETURN path"
    )

    with d.session() as session:
        result = session.run(query)
        records = list(result)

        # Also get directors
        dir_query = (
            f"MATCH (c:Company {{companyNumber: '{company}'}}) "
            f"CALL apoc.path.expandConfig(c, {{"
            f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
            f"  minLevel: 0, maxLevel: 30, "
            f"  uniqueness: 'NODE_GLOBAL'"
            f"}}) YIELD path "
            f"UNWIND nodes(path) AS n "
            f"WITH n WHERE n:Company "
            f"MATCH dirPath = (d:Director)-[:OFFICER_OF]->(n) "
            f"RETURN dirPath AS path"
        )
        dir_result = session.run(dir_query)
        dir_records = list(dir_result)
        records.extend(dir_records)

    if not records:
        return redirect(url_for("home", error=f"No ownership data found for {company}"))

    nodes, rels, flat_rows = extract_graph_data(records)

    os.makedirs(exports_dir, exist_ok=True)
    export_html(nodes, rels, flat_rows, html_path)

    # Also save JSON
    import json
    json_path = os.path.join(exports_dir, f"export_{company}.json")
    json_data = {"nodes": list(nodes.values()), "relationships": rels}
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2, default=str)

    return send_from_directory(exports_dir, f"export_{company}.html")


@app.route("/exports/<path:filename>")
def serve_export(filename):
    exports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
    return send_from_directory(exports_dir, filename)


def _ensure_directors(d, company_number):
    """Fetch directors for companies in the tree that need refreshing."""
    try:
        tree = get_tree_company_numbers(d, company_number)
        meta = get_fetch_metadata(d, tree)
        to_fetch = [(cn, meta.get(cn, {}).get("etag")) for cn in tree if needs_refresh(meta.get(cn, {}).get("fetchedAt"))]

        for cn, old_etag in to_fetch:
            officers, new_etag, modified = fetch_officers(CH_API_KEY, cn, etag=old_etag)
            if modified and officers:
                load_officers_to_neo4j(d, cn, officers, etag=new_etag)
            elif not modified:
                with d.session() as session:
                    session.run(STAMP_FETCH_QUERY, cn=cn, etag=old_etag)
            time.sleep(RATE_LIMIT_DELAY)
    except Exception as e:
        print(f"Warning: director fetch failed: {e}", flush=True)


@app.route("/api/expand")
def api_expand():
    """API endpoint: return graph data for a company's ownership tree as JSON.
    Used by the frontend to expand nodes inline."""
    company = request.args.get("company", "").strip().upper()
    if not company:
        return jsonify({"error": "Missing company parameter"}), 400

    d = get_driver()

    # Check company exists
    with d.session() as session:
        result = session.run(
            "MATCH (c:Company {companyNumber: $cn}) RETURN c.name AS name", cn=company
        )
        if not result.single():
            return jsonify({"error": f"Company {company} not found"}), 404

    # Fetch directors if needed
    if CH_API_KEY:
        _ensure_directors(d, company)

    # Ownership tree
    query = (
        f"MATCH (c:Company {{companyNumber: '{company}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 1, maxLevel: 30, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path RETURN path"
    )
    dir_query = (
        f"MATCH (c:Company {{companyNumber: '{company}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 0, maxLevel: 30, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path "
        f"UNWIND nodes(path) AS n WITH n WHERE n:Company "
        f"MATCH dirPath = (dd:Director)-[:OFFICER_OF]->(n) "
        f"RETURN dirPath AS path"
    )

    with d.session() as session:
        records = list(session.run(query))
        records.extend(list(session.run(dir_query)))

    if not records:
        return jsonify({"nodes": [], "edges": []})

    nodes, rels, _ = extract_graph_data(records)

    # Build vis-compatible JSON
    from search import export_html as _unused  # we just need the helper data
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

    # Merge CE->Company
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
        merged_rels.append({
            "startId": remap(r["startId"]), "endId": remap(r["endId"]),
            "type": r["type"], "properties": r["properties"],
        })

    seen_edges = set()
    vis_nodes = []
    for n in merged_nodes.values():
        label = n["labels"][0] if n["labels"] else "Unknown"
        props = n["properties"]
        display = props.get("name", props.get("companyNumber", str(n["id"])))
        if isinstance(display, str) and len(display) > 50:
            display = display[:47] + "..."
        cn = props.get("companyNumber", "")
        if label == "Company" and cn:
            display = f"{display}\n({cn})"

        safe_props = {}
        for k, v in props.items():
            safe_props[k] = str(v) if not isinstance(v, list) else ", ".join(str(x) for x in v)

        vis_nodes.append({
            "id": n["id"], "label": display,
            "color": label_colors.get(label, "#999"),
            "group": label, "size": node_sizes.get(label, 15),
            "shape": node_shapes.get(label, "dot"),
            "properties": safe_props,
        })

    vis_edges = []
    for r in merged_rels:
        edge_key = (r["startId"], r["endId"], r["type"])
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)
        noc_raw = r["properties"].get("naturesOfControl", [])
        if isinstance(noc_raw, list):
            noc = ", ".join(str(x) for x in noc_raw)
        else:
            noc = str(noc_raw)
        vis_edges.append({
            "from": r["startId"], "to": r["endId"],
            "label": r["type"].replace("HAS_SIGNIFICANT_CONTROL", "CONTROLS").replace("OFFICER_OF", "DIRECTOR").replace("_", " "),
            "title": noc or r["type"], "arrows": "to",
        })

    return jsonify({"nodes": vis_nodes, "edges": vis_edges})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print(f"Starting CH Graphs on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
