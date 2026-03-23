#!/usr/bin/env python3
"""
CH Graphs — Web Interface

A simple Flask app that lets you search for a company and view its
ownership tree as an interactive graph.
"""

import os
import sys
import time

from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify
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
from fetch_directors import fetch_officers, load_officers_to_neo4j, get_tree_company_numbers, get_fetch_metadata, needs_refresh, STAMP_FETCH_QUERY
from search import extract_graph_data
from vis import _build_vis_data, _compute_positions

app = Flask(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "companies2026")
CH_API_KEY = os.environ.get("CH_API_KEY", os.environ.get("CH_API", ""))
RATE_LIMIT_DELAY = 0.2

# Metrics
stats = {"ch_api_calls": 0, "ch_api_cached": 0, "neo4j_queries": 0, "graphs_served": 0}

driver = None


def get_driver():
    global driver
    if driver is None:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        _fix_is_company_links(driver)
    return driver


def _fix_is_company_links(d):
    """Create missing IS_COMPANY rels where registration_number needs zero-padding.

    Uses APOC periodic.iterate for batched execution to avoid long-running
    single transactions.  Only needed for databases imported before the
    load_data.py zero-padding fix.
    """
    with d.session() as s:
        # First ensure an index exists for the scan
        s.run("CREATE INDEX ce_reg IF NOT EXISTS FOR (ce:CorporateEntity) ON (ce.registrationNumber)")
        result = s.run("""
            CALL apoc.periodic.iterate(
              'MATCH (ce:CorporateEntity)
               WHERE ce.registrationNumber IS NOT NULL
                 AND size(ce.registrationNumber) < 8
                 AND NOT (ce)-[:IS_COMPANY]->(:Company)
               WITH ce, right("00000000" + ce.registrationNumber, 8) AS padded
               RETURN ce, padded',
              'WITH ce, padded
               MATCH (co:Company {companyNumber: padded})
               MERGE (ce)-[:IS_COMPANY]->(co)',
              {batchSize: 500, parallel: false}
            ) YIELD batches, total, errorMessages
            RETURN total, errorMessages
        """)
        rec = result.single()
        n = rec["total"]
        if n:
            errs = rec["errorMessages"]
            print(f"[startup] Fixed {n} IS_COMPANY links via zero-padding"
                  + (f" (errors: {errs})" if errs else ""))


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
    return render_template("home.html", recent=recent, error=request.args.get("error"), stats=stats)


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

    return render_template("search.html", q=q, results=results)


@app.route("/graph")
def graph():
    company = request.args.get("company", "").strip().upper()
    if not company:
        return redirect(url_for("home"))
    return render_template("graph.html", company=company)


@app.route("/api/stream")
def api_stream():
    """SSE endpoint that streams graph data as it's built."""
    company = request.args.get("company", "").strip().upper()
    include_former = request.args.get("former", "") == "1"
    if not company:
        return "Missing company", 400

    from flask import Response

    def generate():
        import json as _json

        def send(event, data):
            if event != "data":
                print(f"  [{company}] {data}", flush=True)
            else:
                d = _json.loads(data)
                print(f"  [{company}] +{len(d.get('nodes',[]))} nodes, +{len(d.get('edges',[]))} edges", flush=True)
            # SSE data fields can't contain bare newlines — split into multiple data: lines
            lines = data.replace('\n', '\ndata: ')
            return f"event: {event}\ndata: {lines}\n\n"

        d = get_driver()

        # 1. Check company exists
        yield send("status", f"Looking up {company}...")
        with d.session() as session:
            result = session.run(
                "MATCH (c:Company {companyNumber: $cn}) RETURN c.name AS name", cn=company
            )
            record = result.single()
            if not record:
                yield send("error_msg", f"Company {company} not found in database")
                return
        yield send("status", f"Found: {record['name']}")

        all_records = []

        # 2. Ownership tree — send immediately so graph appears fast
        yield send("status", "Querying ownership tree...")
        query = _ownership_query(company, "both")
        with d.session() as session:
            records = list(session.run(query))
        if records:
            all_records.extend(records)
            nodes_data, rels_data, _ = extract_graph_data(records)
            vis = _build_vis_data(nodes_data, rels_data)
            yield send("data", _json.dumps(vis, default=str))
            yield send("status", f"Ownership tree: {len(vis['nodes'])} nodes")
        else:
            yield send("status", "No ownership chain — loading direct PSCs...")
            with d.session() as session:
                fallback = list(session.run(
                    "MATCH (c:Company {companyNumber: $cn}) "
                    "OPTIONAL MATCH path1 = (psc)-[:HAS_SIGNIFICANT_CONTROL]->(c) "
                    "RETURN c, path1", cn=company
                ))
            all_records.extend(fallback)
            if fallback:
                nodes_data, rels_data, _ = extract_graph_data(fallback)
                vis = _build_vis_data(nodes_data, rels_data)
                yield send("data", _json.dumps(vis, default=str))

        # 3. Fetch directors from API if needed
        if CH_API_KEY:
            yield send("status", "Fetching director data from Companies House API...")
            try:
                _ensure_directors(d, company)
            except Exception as e:
                yield send("status", f"Director fetch warning: {e}")

            # 4. Query directors and merge with ownership data
            yield send("status", "Loading directors...")
            dir_query = _directors_query(company, include_former=include_former)
            with d.session() as session:
                dir_records = list(session.run(dir_query))

            if dir_records:
                # Process directors only (not combined with ownership — that was already sent)
                # But include ownership records for Person/Director merge detection
                all_records.extend(dir_records)
                all_nodes, all_rels, _ = extract_graph_data(all_records)
                vis = _build_vis_data(all_nodes, all_rels)

                # Only send nodes/edges that are Director-related (not already sent ownership data)
                dir_node_ids = set()
                for r in all_rels:
                    if r["type"] == "OFFICER_OF":
                        dir_node_ids.add(r["startId"])
                # Filter to director nodes + their edges only
                dir_nodes = [n for n in vis.get("nodes", []) if n["id"] in dir_node_ids or n.get("borderWidth")]
                dir_edges = [e for e in vis.get("edges", []) if e.get("title", "").startswith(("Director", "Secretary", "Corporate"))]

                if dir_nodes or dir_edges:
                    yield send("data", _json.dumps({"nodes": dir_nodes, "edges": dir_edges}, default=str))
                yield send("status", f"{len(dir_nodes)} directors loaded")

        stats["graphs_served"] += 1
        yield send("done", "Complete")

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


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

        stats["ch_api_cached"] += len(tree) - len(to_fetch)
        for cn, old_etag in to_fetch:
            stats["ch_api_calls"] += 1
            officers, new_etag, modified = fetch_officers(CH_API_KEY, cn, etag=old_etag)
            if modified and officers:
                load_officers_to_neo4j(d, cn, officers, etag=new_etag)
            elif not modified:
                stats["ch_api_cached"] += 1  # 304 Not Modified counts as cached
                with d.session() as session:
                    session.run(STAMP_FETCH_QUERY, cn=cn, etag=old_etag)
            time.sleep(RATE_LIMIT_DELAY)
    except Exception as e:
        print(f"Warning: director fetch failed: {e}", flush=True)


def _ownership_query(company, direction="both"):
    """Build the ownership tree Cypher query."""
    return (
        f"MATCH (c:Company {{companyNumber: '{company}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY>', "
        f"  minLevel: 1, maxLevel: 15, "
        f"  uniqueness: 'NODE_GLOBAL', "
        f"  limit: 200"
        f"}}) YIELD path RETURN path"
    )


def _directors_query(company, include_former=False):
    """Get directors for the starting company and its direct parent/child companies."""
    where_clause = "" if include_former else "WHERE r.resignedOn IS NULL OR r.resignedOn = '' "
    # Simple direct match — don't use APOC tree traversal for directors
    return (
        f"MATCH dirPath = (dd:Director)-[r:OFFICER_OF]->(c:Company {{companyNumber: '{company}'}}) "
        f"{where_clause}"
        f"RETURN dirPath AS path"
    )


@app.route("/api/stats")
def api_stats():
    return jsonify(stats)


@app.route("/api/expand")
def api_expand():
    """API endpoint: return graph data for expanding a node inline.
    Supports ?type=company&id=00253240 or ?type=person&id=<neo4j_id>
    Legacy: ?company=00253240 still works."""
    expand_type = request.args.get("type", "company")
    expand_id = request.args.get("id", request.args.get("company", "")).strip()
    offset = int(request.args.get("offset", "0"))
    limit = 50
    if not expand_id:
        return jsonify({"error": "Missing id parameter"}), 400

    d = get_driver()
    has_more = False

    if expand_type == "company":
        company = expand_id.upper()
        with d.session() as session:
            result = session.run(
                "MATCH (c:Company {companyNumber: $cn}) RETURN c.name AS name", cn=company
            )
            if not result.single():
                return jsonify({"error": f"Company {company} not found"}), 404

        if CH_API_KEY:
            _ensure_directors(d, company)

        query = _ownership_query(company, "both")
        dir_query = _directors_query(company)

        with d.session() as session:
            records = list(session.run(query))
            records.extend(list(session.run(dir_query)))
            # Also get direct incoming/outgoing PSC edges so the node connects
            # to existing graph when expanded from another company
            records.extend(list(session.run(
                "MATCH path = (psc)-[:HAS_SIGNIFICANT_CONTROL]->(c:Company {companyNumber: $cn}) "
                "RETURN path",
                cn=company,
            )))
            records.extend(list(session.run(
                "MATCH (c:Company {companyNumber: $cn})<-[:IS_COMPANY]-(ce:CorporateEntity) "
                "MATCH path = (ce)-[:HAS_SIGNIFICANT_CONTROL]->(target:Company) "
                "RETURN path",
                cn=company,
            )))

    elif expand_type == "person":
        person_name = request.args.get("name", "").strip()
        with d.session() as session:
            # Try by name first (covers all deduped Director/Person nodes)
            if not person_name:
                # Get name from Neo4j if not provided
                name_result = session.run(
                    "MATCH (n) WHERE elementId(n) = $nid RETURN n.name AS name", nid=expand_id
                )
                name_rec = name_result.single()
                person_name = name_rec["name"] if name_rec else ""

            person_records = []
            if person_name:
                # Find all Director appointments with this name
                person_records.extend(list(session.run(
                    "MATCH (n:Director) WHERE n.name = $name "
                    "MATCH path = (n)-[:OFFICER_OF]->(c:Company) "
                    "RETURN path LIMIT $lim",
                    name=person_name, lim=limit,
                )))
                # Find all Person PSC records with this name
                person_records.extend(list(session.run(
                    "MATCH (n:Person) WHERE n.name = $name "
                    "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL]->(c:Company) "
                    "RETURN path LIMIT $lim",
                    name=person_name, lim=limit,
                )))

            # Also try elementId direct lookup
            if not person_records:
                person_records = list(session.run(
                    "MATCH (n) WHERE elementId(n) = $nid "
                    "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL|OFFICER_OF]->(c:Company) "
                    "RETURN path LIMIT $lim",
                    nid=expand_id, lim=limit,
                ))
            if len(person_records) > limit:
                person_records = person_records[:limit]
                has_more = True
            records = list(person_records)

            # Also get PSC data for each found company
            company_numbers = set()
            for rec in person_records:
                for key in rec.keys():
                    val = rec[key]
                    if hasattr(val, 'nodes'):
                        for node in val.nodes:
                            if "Company" in node.labels:
                                cn = node.get("companyNumber", "")
                                if cn:
                                    company_numbers.add(cn)
            for cn in company_numbers:
                tree_records = list(session.run(
                    "MATCH (c:Company {companyNumber: $cn}) "
                    "MATCH path = (psc)-[:HAS_SIGNIFICANT_CONTROL]->(c) "
                    "RETURN path",
                    cn=cn,
                ))
                records.extend(tree_records)

    elif expand_type == "corporate":
        with d.session() as session:
            corp_records = list(session.run(
                "MATCH (n) WHERE elementId(n) = $nid "
                "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL]->(c:Company) "
                "RETURN path SKIP $skip LIMIT $lim",
                nid=expand_id, skip=offset, lim=limit + 1,
            ))
            if len(corp_records) > limit:
                corp_records = corp_records[:limit]
                has_more = True
            records = list(corp_records)

    elif expand_type == "directors":
        company = expand_id.upper()
        if CH_API_KEY:
            # Direct fetch for this specific company (not tree-based)
            from fetch_directors import fetch_officers as _fetch, load_officers_to_neo4j as _load, get_fetch_metadata as _meta, needs_refresh as _needs
            meta = _meta(d, [company])
            m = meta.get(company, {"fetchedAt": None, "etag": None})
            if _needs(m["fetchedAt"]):
                stats["ch_api_calls"] += 1
                print(f"  [directors] Fetching officers for {company}...", flush=True)
                officers, new_etag, modified = _fetch(CH_API_KEY, company, etag=m.get("etag"))
                if modified and officers:
                    _load(d, company, officers, etag=new_etag)
                    print(f"  [directors] Loaded {len(officers)} officers for {company}", flush=True)
                elif not modified:
                    stats["ch_api_cached"] += 1
                    from fetch_directors import STAMP_FETCH_QUERY as _stamp
                    with d.session() as session:
                        session.run(_stamp, cn=company, etag=m.get("etag") or "")
            else:
                stats["ch_api_cached"] += 1
        with d.session() as session:
            records = list(session.run(
                "MATCH dirPath = (dd:Director)-[r:OFFICER_OF]->(c:Company {companyNumber: $cn}) "
                "WHERE r.resignedOn IS NULL OR r.resignedOn = '' "
                "RETURN dirPath AS path",
                cn=company,
            ))

    elif expand_type == "address":
        parts = expand_id.split("|", 1)
        postcode = parts[0].strip()
        addr_line = parts[1].strip() if len(parts) > 1 else ""
        with d.session() as session:
            records = list(session.run(
                "MATCH (c:Company) "
                "WHERE c.postcode = $pc AND c.addressLine1 = $addr "
                "RETURN c SKIP $skip LIMIT $lim",
                pc=postcode, addr=addr_line, skip=offset, lim=limit + 1,
            ))
            if len(records) > limit:
                records = records[:limit]
                has_more = True

    elif expand_type == "postcode":
        postcode = expand_id.strip()
        with d.session() as session:
            records = list(session.run(
                "MATCH (c:Company) "
                "WHERE c.postcode = $pc "
                "RETURN c SKIP $skip LIMIT $lim",
                pc=postcode, skip=offset, lim=limit + 1,
            ))
            if len(records) > limit:
                records = records[:limit]
                has_more = True

    elif expand_type == "subsidiaries":
        # Find companies where this company is a PSC (via CorporateEntity with matching registration number)
        company = expand_id.upper()
        with d.session() as session:
            # Find CorporateEntities whose registration number matches this company
            records = list(session.run(
                "MATCH (ce:CorporateEntity)-[:IS_COMPANY]->(c:Company {companyNumber: $cn}) "
                "MATCH path = (ce)-[:HAS_SIGNIFICANT_CONTROL]->(subsidiary:Company) "
                "RETURN path SKIP $skip LIMIT $lim",
                cn=company, skip=offset, lim=limit + 1,
            ))
            if not records:
                # Also try matching by name (for CEs without IS_COMPANY link)
                records = list(session.run(
                    "MATCH (c:Company {companyNumber: $cn}) "
                    "MATCH (ce:CorporateEntity) WHERE ce.registrationNumber = $cn "
                    "MATCH path = (ce)-[:HAS_SIGNIFICANT_CONTROL]->(subsidiary:Company) "
                    "RETURN path SKIP $skip LIMIT $lim",
                    cn=company, skip=offset, lim=limit + 1,
                ))
            if len(records) > limit:
                records = records[:limit]
                has_more = True

    else:
        return jsonify({"error": f"Unknown type: {expand_type}"}), 400

    if not records:
        return jsonify({"nodes": [], "edges": [], "hasMore": False})

    nodes, rels, _ = extract_graph_data(records)
    vis = _build_vis_data(nodes, rels)
    vis["hasMore"] = has_more
    vis["nextOffset"] = offset + limit
    return jsonify(vis)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print(f"Starting CH Graphs on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
