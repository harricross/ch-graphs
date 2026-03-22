#!/usr/bin/env python3
"""
CH Graphs — Web Interface

A simple Flask app that lets you search for a company and view its
ownership tree as an interactive graph.
"""

import os
import sys
import time

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
from fetch_directors import fetch_officers, load_officers_to_neo4j, get_tree_company_numbers, get_fetch_metadata, needs_refresh, STAMP_FETCH_QUERY
from search import extract_graph_data, _compute_levels

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

  <div style="margin-top: 32px; color: #555; font-size: 12px;">
    Graphs served: {{ stats.graphs_served }} · CH API calls: {{ stats.ch_api_calls }} · Served from cache: {{ stats.ch_api_cached }}
  </div>

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
    return render_template_string(HOME_TEMPLATE, recent=recent, error=request.args.get("error"), stats=stats)


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


GRAPH_PAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>CH Graphs — {{ company }}</title>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, sans-serif; background: #0f0f1a; color: #eee; }
    #graph { width: 100vw; height: 100vh; }
    #controls { position: fixed; top: 10px; left: 10px; background: rgba(0,0,0,0.85);
      padding: 14px 18px; border-radius: 10px; font-size: 13px; z-index: 10; max-width: 360px; max-height: 90vh; overflow-y: auto; }
    #controls h3 { margin: 0 0 8px 0; font-size: 16px; }
    #status { color: #f1c40f; margin: 8px 0; font-size: 12px; min-height: 16px; }
    #status.done { color: #34A853; }
    #status.error { color: #e74c3c; }
    .legend { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
    .legend-item { display: flex; align-items: center; gap: 5px; font-size: 12px; }
    .legend-shape { width: 14px; height: 14px; display: inline-block; }
    .legend-line { width: 20px; height: 3px; display: inline-block; border-radius: 1px; }
    .btn { background: #2a2a4a; border: 1px solid #4a4a7a; color: #ccc; padding: 6px 12px;
      border-radius: 5px; cursor: pointer; font-size: 12px; margin: 2px; }
    .btn.active { background: #4C8BF5; border-color: #4C8BF5; color: #fff; }
    .btn-row { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 4px; }
    #details { position: fixed; bottom: 10px; right: 10px; background: rgba(0,0,0,0.85);
      padding: 14px 18px; border-radius: 10px; font-size: 12px; z-index: 10;
      max-width: 400px; max-height: 50vh; overflow-y: auto; display: none; }
    #details h4 { margin: 0 0 6px 0; }
    #details table { border-collapse: collapse; width: 100%; }
    #details td { padding: 2px 6px; border-bottom: 1px solid #333; vertical-align: top; word-break: break-all; }
    #details td:first-child { color: #aaa; white-space: nowrap; }
    .expand-btn { background: #4C8BF5; border: none; color: #fff; padding: 6px 14px; border-radius: 5px;
      cursor: pointer; font-size: 12px; margin-bottom: 8px; display: block; }
    .expand-btn:hover { background: #3a7be0; }
    .spinner-sm { width: 20px; height: 20px; border: 3px solid #333; border-top: 3px solid #4C8BF5;
      border-radius: 50%; animation: spin 0.8s linear infinite; margin: 8px 0; display: inline-block; }
    @keyframes spin { to { transform: rotate(360deg); } }
    #stats { font-size: 12px; color: #888; }
    a.back { color: #4C8BF5; text-decoration: none; font-size: 12px; }
  </style>
</head>
<body>
  <div id="controls">
    <a class="back" href="/">← Back to search</a>
    <h3 style="margin-top: 6px; cursor: pointer;" onclick="focusRoot()" id="title">{{ company }}</h3>
    <div id="stats">Loading...</div>
    <div id="status"><span class="spinner-sm"></span> Querying ownership tree...</div>
    <div class="legend">
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><circle cx="7" cy="7" r="6" fill="#4C8BF5"/></svg>Company</span>
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><polygon points="7,1 13,7 7,13 1,7" fill="#34A853"/></svg>Person</span>
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><rect x="1" y="1" width="12" height="12" fill="#FBBC04"/></svg>Corp Entity</span>
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><polygon points="7,1 13,13 1,13" fill="#EA4335"/></svg>Legal Person</span>
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><polygon points="7,13 1,1 13,1" fill="#00BCD4"/></svg>Director</span>
      <span class="legend-item"><svg class="legend-shape" viewBox="0 0 14 14"><circle cx="7" cy="7" r="5" fill="none" stroke="#FF6600" stroke-width="3"/></svg>PSC + Director</span>
    </div>
    <div style="margin-top: 6px; font-size: 11px; color: #aaa;">Arrow colours:</div>
    <div class="legend" style="margin-top: 2px;">
      <span class="legend-item"><span class="legend-line" style="background:#e74c3c"></span>75-100%</span>
      <span class="legend-item"><span class="legend-line" style="background:#e67e22"></span>50-75%</span>
      <span class="legend-item"><span class="legend-line" style="background:#f1c40f"></span>25-50%</span>
      <span class="legend-item"><span class="legend-line" style="background:#9b59b6"></span>Appoint dirs</span>
      <span class="legend-item"><span class="legend-line" style="background:#3498db"></span>Influence</span>
      <span class="legend-item"><span class="legend-line" style="background:#00BCD4"></span>Director</span>
      <span class="legend-item"><span class="legend-line" style="background:#78909C"></span>Secretary</span>
    </div>
    <div class="btn-row">
      <button class="btn" onclick="toggleLayout()">Re-layout</button>
      <button class="btn" onclick="network.fit()">Fit View</button>
      <button class="btn" id="autoBtn" onclick="autoResolve()">Auto-resolve all</button>
      <label style="font-size:11px; color:#888; margin-left:4px;">Hops: <input type="number" id="autoMaxHops" value="3" min="1" max="20" style="width:40px; background:#1a1a2e; color:#eee; border:1px solid #4a4a7a; border-radius:3px; padding:2px 4px; font-size:11px;"></label>
      <div id="autoStatus" style="font-size:11px; color:#888; margin-top:4px;"></div>
      <button class="btn" id="formerBtn" onclick="toggleFormer()">Former Officers: Off</button>
      <button class="btn" id="dormantBtn" onclick="toggleDormant()">Dormant: Shown</button>
    </div>
  </div>
  <div id="details"></div>
  <div id="graph"></div>
  <script>
    var nodes = new vis.DataSet();
    var edges = new vis.DataSet();
    var container = document.getElementById('graph');
    var data = { nodes: nodes, edges: edges };
    // Positions computed server-side, no physics needed initially
    var initialFitDone = false;

    var defaultOpts = {
      layout: { hierarchical: { enabled: false } },
      physics: { enabled: false },
      nodes: { font: { color: '#eee', size: 10, multi: 'md', face: 'arial' },
        borderWidth: 2, widthConstraint: { maximum: 150 } },
      edges: { color: { color: '#666', highlight: '#fff', hover: '#aaa' },
        font: { size: 0 },
        smooth: { type: 'curvedCW', roundness: 0.15 }, width: 1.5 },
      interaction: { hover: true, tooltipDelay: 100, navigationButtons: true, keyboard: true,
        zoomView: true, dragView: true, dragNodes: true, zoomSpeed: 1 }
    };
    var physicsOpts = Object.assign({}, defaultOpts, {
      physics: { enabled: true, solver: 'barnesHut',
        barnesHut: { gravitationalConstant: -15000, springLength: 400, springConstant: 0.005, damping: 0.3, avoidOverlap: 0.8 },
        stabilization: { iterations: 400 } }
    });

    var network = new vis.Network(container, data, defaultOpts);
    // Toggle re-layouts with physics then freezes
    function toggleLayout() {
      // Re-run physics to re-layout, then freeze
      network.setOptions(physicsOpts);
      network.once('stabilizationIterationsDone', function() { network.setOptions({ physics: { enabled: false } }); });
    }
    var dormantHidden = false;
    function toggleDormant() {
      dormantHidden = !dormantHidden;
      var allN = nodes.get();
      allN.forEach(function(n) {
        if (n.group === 'Company') {
          var status = ((n.properties || {}).status || '').toLowerCase();
          if (status.indexOf('dormant') >= 0 || status.indexOf('dissolved') >= 0) {
            nodes.update({ id: n.id, hidden: dormantHidden });
          }
        }
      });
      var btn = document.getElementById('dormantBtn');
      btn.textContent = dormantHidden ? 'Dormant: Hidden' : 'Dormant: Shown';
      btn.classList.toggle('active', dormantHidden);
    }
    function toggleFormer() {
      var url = new URL(window.location);
      if (url.searchParams.get('former') === '1') {
        url.searchParams.delete('former');
      } else {
        url.searchParams.set('former', '1');
      }
      window.location = url;
    }
    // Set initial Former button state from URL
    (function() {
      var btn = document.getElementById('formerBtn');
      if (new URLSearchParams(window.location.search).get('former') === '1') {
        btn.textContent = 'Former Officers: On';
        btn.classList.add('active');
      }
    })();
    function escHtml(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

    function findPathToRoot(startId) {
      if (!rootNodeId || startId === rootNodeId) return '';
      // BFS from startId to rootNodeId through edges (ignore direction)
      var visited = {};
      var parent = {};
      var queue = [startId];
      visited[startId] = true;
      var found = false;
      var allEdges = edges.get();
      while (queue.length > 0 && !found) {
        var current = queue.shift();
        for (var i = 0; i < allEdges.length; i++) {
          var e = allEdges[i];
          var neighbor = null;
          if (e.from === current) neighbor = e.to;
          else if (e.to === current) neighbor = e.from;
          if (neighbor && !visited[neighbor]) {
            visited[neighbor] = true;
            parent[neighbor] = { from: current, edge: e };
            if (neighbor === rootNodeId) { found = true; break; }
            queue.push(neighbor);
          }
        }
      }
      if (!found) return '<span style="color:#666; font-size:11px;">No path found</span>';
      // Build path
      var path = [];
      var cur = rootNodeId;
      while (cur !== startId) {
        path.unshift(cur);
        var p = parent[cur];
        if (!p) break;
        cur = p.from;
      }
      path.unshift(startId);
      // Render as clickable breadcrumbs
      var html = '<div style="font-size: 11px; line-height: 1.8;">';
      path.forEach(function(nid, idx) {
        var n = nodes.get(nid);
        if (!n) return;
        var name = (n.properties || {}).name || (n.properties || {}).companyNumber || n.label || '?';
        if (name.length > 30) name = name.substring(0, 27) + '...';
        var color = n.color.background || n.color || '#999';
        html += '<span style="cursor:pointer; color:' + color + ';" onclick="focusNode(&quot;' + nid + '&quot;)">';
        html += escHtml(name);
        html += '</span>';
        if (idx < path.length - 1) html += ' <span style="color:#555;">→</span> ';
      });
      html += '</div>';
      return html;
    }

    // Click to show details
    network.on('click', function(params) {
      var panel = document.getElementById('details');
      if (params.nodes.length > 0) {
        var nodeId = params.nodes[0];
        var node = nodes.get(nodeId);
        var props = node.properties || {};
        var cn = props.companyNumber || '';
        var h = '<h4>' + escHtml(node.group || '') + '</h4>';
        if (cn && node.group === 'Company') {
          h += '<button class="expand-btn" onclick="expandNode(&quot;company&quot;, &quot;' + escHtml(cn) + '&quot;)">Expand ownership tree</button>';
          h += '<button class="expand-btn" onclick="expandNode(&quot;directors&quot;, &quot;' + escHtml(cn) + '&quot;)">Load directors</button>';
          var pc = props.postcode || '';
          var addr = props.addressLine1 || '';
          if (pc) {
            h += '<button class="expand-btn" onclick="expandNode(&quot;address&quot;, &quot;' + escHtml(pc) + '|' + escHtml(addr) + '&quot;)">Companies at ' + escHtml(addr ? addr + ', ' + pc : pc) + '</button>';
            h += '<button class="expand-btn" onclick="expandNode(&quot;postcode&quot;, &quot;' + escHtml(pc) + '&quot;)">All companies at ' + escHtml(pc) + '</button>';
          }
        }
        if (node.group === 'Person' || node.group === 'Director') {
          h += '<button class="expand-btn" onclick="expandNode(&quot;person&quot;, &quot;' + escHtml(nodeId) + '&quot;)">Find all companies</button>';
        }
        if (node.group === 'CorporateEntity') {
          var regNum = props.registrationNumber || '';
          if (regNum) {
            // Pad to 8 digits if purely numeric
            var padded = /^\\d+$/.test(regNum) ? ('00000000' + regNum).slice(-8) : regNum;
            h += '<button class="expand-btn" onclick="expandNode(&quot;company&quot;, &quot;' + escHtml(padded) + '&quot;)">Expand as company</button>';
          }
          h += '<button class="expand-btn" onclick="expandNode(&quot;corporate&quot;, &quot;' + escHtml(nodeId) + '&quot;)">Find controlled companies</button>';
        }
        h += '<table>';
        Object.keys(props).forEach(function(key) {
          h += '<tr><td>' + escHtml(key) + '</td><td>' + escHtml(props[key]) + '</td></tr>';
        });
        h += '</table>';

        // Find path back to root node
        var pathHtml = findPathToRoot(nodeId);
        if (pathHtml) {
          h += '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #333;">';
          h += '<div style="color: #888; font-size: 11px; margin-bottom: 4px;">Path to root:</div>';
          h += pathHtml;
          h += '</div>';
        }

        panel.innerHTML = h;
        panel.style.display = 'block';

        // Highlight neighbourhood: fade out nodes beyond 4 hops + path to root
        highlightNeighbourhood(nodeId, 4);
      } else {
        panel.style.display = 'none';
        resetHighlight();
      }
    });

    var highlightActive = false;
    function highlightNeighbourhood(centerId, maxHops) {
      var allEdges = edges.get();
      // BFS to find all nodes within maxHops
      var nearby = {};
      nearby[centerId] = true;
      var queue = [{ id: centerId, depth: 0 }];
      while (queue.length > 0) {
        var cur = queue.shift();
        if (cur.depth >= maxHops) continue;
        allEdges.forEach(function(e) {
          var neighbor = null;
          if (e.from === cur.id) neighbor = e.to;
          else if (e.to === cur.id) neighbor = e.from;
          if (neighbor && !nearby[neighbor]) {
            nearby[neighbor] = true;
            queue.push({ id: neighbor, depth: cur.depth + 1 });
          }
        });
      }
      // Also include path to root
      if (rootNodeId && !nearby[rootNodeId]) {
        var visited = {};
        var parent = {};
        var bfsQ = [centerId];
        visited[centerId] = true;
        var found = false;
        while (bfsQ.length > 0 && !found) {
          var c = bfsQ.shift();
          for (var i = 0; i < allEdges.length; i++) {
            var e = allEdges[i];
            var nb = null;
            if (e.from === c) nb = e.to;
            else if (e.to === c) nb = e.from;
            if (nb && !visited[nb]) {
              visited[nb] = true;
              parent[nb] = c;
              if (nb === rootNodeId) { found = true; break; }
              bfsQ.push(nb);
            }
          }
        }
        if (found) {
          var p = rootNodeId;
          while (p && p !== centerId) { nearby[p] = true; p = parent[p]; }
        }
      }
      // Apply opacity
      var nodeUpdates = [];
      var edgeUpdates = [];
      nodes.get().forEach(function(n) {
        var inSet = nearby[n.id];
        nodeUpdates.push({ id: n.id, opacity: inSet ? 1.0 : 0.15 });
      });
      allEdges.forEach(function(e) {
        var inSet = nearby[e.from] && nearby[e.to];
        edgeUpdates.push({ id: e.id, hidden: !inSet });
      });
      nodes.update(nodeUpdates);
      edges.update(edgeUpdates);
      highlightActive = true;
    }
    function resetHighlight() {
      if (!highlightActive) return;
      var nodeUpdates = [];
      var edgeUpdates = [];
      nodes.get().forEach(function(n) { nodeUpdates.push({ id: n.id, opacity: 1.0 }); });
      edges.get().forEach(function(e) { edgeUpdates.push({ id: e.id, hidden: false }); });
      nodes.update(nodeUpdates);
      edges.update(edgeUpdates);
      highlightActive = false;
    }

    // Double-click to expand
    network.on('doubleClick', function(params) {
      if (params.nodes.length > 0) {
        var node = nodes.get(params.nodes[0]);
        var cn = (node.properties || {}).companyNumber;
        if (cn && node.group === 'Company') expandNode('company', cn);
        else if (node.group === 'Person' || node.group === 'Director') expandNode('person', params.nodes[0]);
        else if (node.group === 'CorporateEntity') {
          var regNum = (node.properties || {}).registrationNumber;
          if (regNum) expandNode('company', regNum);
        }
      }
    });

    var expanding = {};
    function expandNode(type, id, offset) {
      var key = type + ':' + id;
      if (expanding[key]) return;
      expanding[key] = true;
      setStatus('Expanding...');
      var url = '/api/expand?type=' + encodeURIComponent(type) + '&id=' + encodeURIComponent(id);
      if (offset) url += '&offset=' + offset;
      fetch(url)
        .then(function(r) { return r.json(); })
        .then(function(d) {
          mergeData(d);
          expanding[key] = false;
          var msg = 'Added items';
          if (d.hasMore) {
            msg += ' (more available)';
            var panel = document.getElementById('details');
            panel.innerHTML += '<button class="expand-btn" onclick="expandNode(&quot;' + type + '&quot;, &quot;' + escHtml(id) + '&quot;, ' + d.nextOffset + ')">Load more</button>';
          }
          setStatus(msg, 'done');
        })
        .catch(function(e) { expanding[key] = false; setStatus('Error: ' + e, 'error'); });
    }

    function mergeData(d) {
      var added = 0;
      // Check if existing nodes use levels
      var hasLevels = nodes.get().some(function(n) { return n.level !== undefined; });

      // Build a map of registrationNumber -> existing CorporateEntity node ID
      // so we can merge Company nodes with their CorporateEntity counterparts
      var regNumToNodeId = {};
      nodes.get().forEach(function(n) {
        if (n.group === 'CorporateEntity') {
          var reg = (n.properties || {}).registrationNumber || '';
          if (reg) {
            // Store both raw and padded versions
            regNumToNodeId[reg] = n.id;
            if (/^\\d+$/.test(reg)) regNumToNodeId[('00000000' + reg).slice(-8)] = n.id;
          }
        }
      });

      // Also map companyNumber -> existing Company node ID
      var cnToNodeId = {};
      nodes.get().forEach(function(n) {
        if (n.group === 'Company') {
          var cn = (n.properties || {}).companyNumber || '';
          if (cn) cnToNodeId[cn] = n.id;
        }
      });

      // Map entityId -> existing CorporateEntity/Person node ID for dedup
      var entityToNodeId = {};
      nodes.get().forEach(function(n) {
        var eid = (n.properties || {}).entityId || (n.properties || {}).personId || '';
        if (eid) entityToNodeId[eid] = n.id;
      });

      // Map Director name -> existing Director node ID for dedup
      var dirNameToNodeId = {};
      nodes.get().forEach(function(n) {
        if (n.group === 'Director') {
          var name = ((n.properties || {}).name || '').toUpperCase().trim();
          if (name) dirNameToNodeId[name] = n.id;
        }
      });

      // Map Person name -> existing Person node ID for dedup
      var personNameToNodeId = {};
      nodes.get().forEach(function(n) {
        if (n.group === 'Person') {
          var name = ((n.properties || {}).name || '').toUpperCase().trim();
          if (name) personNameToNodeId[name] = n.id;
        }
      });

      // Remap table: new node ID -> existing node ID (for merging)
      var remap = {};

      (d.nodes || []).forEach(function(n) {
        // If node already exists, update it with any new properties
        var existing = nodes.get(n.id);
        if (existing) {
          var updates = { id: n.id };
          var needsUpdate = false;
          if (n.borderWidth && !existing.borderWidth) { updates.borderWidth = n.borderWidth; updates.color = n.color; needsUpdate = true; }
          if (n.level !== undefined && existing.level === undefined) { updates.level = n.level; needsUpdate = true; }
          if (needsUpdate) nodes.update(updates);
          return;
        }
        var cn = (n.properties || {}).companyNumber || '';

          // Check if this Company already exists in the graph (different neo4j ID, same companyNumber)
          if (n.group === 'Company' && cn && cnToNodeId[cn]) {
            remap[n.id] = cnToNodeId[cn];
            return;  // skip duplicate company
          }

          // Check if this new Company node matches an existing CorporateEntity
          if (n.group === 'Company' && cn && regNumToNodeId[cn]) {
            // Merge: update existing CE node to look like a Company, remap edges
            var ceId = regNumToNodeId[cn];
            remap[n.id] = ceId;
            // Update the existing node with company data
            var update = { id: ceId, label: n.label, color: n.color, group: 'Company',
              size: n.size, shape: n.shape, properties: n.properties, title: n.title };
            if (n.level !== undefined) update.level = n.level;
            nodes.update(update);
            return;  // don't add as new node
          }

          // Check if this new CorporateEntity matches an existing Company
          if (n.group === 'CorporateEntity') {
            var reg = (n.properties || {}).registrationNumber || '';
            var padded = /^\\d+$/.test(reg) ? ('00000000' + reg).slice(-8) : reg;
            if (reg && cnToNodeId[padded]) {
              remap[n.id] = cnToNodeId[padded];
              return;  // skip, company already exists
            }
            if (reg && cnToNodeId[reg]) {
              remap[n.id] = cnToNodeId[reg];
              return;
            }
          }

          // Check if this entity (Person/CorporateEntity) already exists by entityId/personId
          var eid = (n.properties || {}).entityId || (n.properties || {}).personId || '';
          if (eid && entityToNodeId[eid]) {
            remap[n.id] = entityToNodeId[eid];
            return;
          }

          // Check if this Director already exists by name
          if (n.group === 'Director') {
            var dname = ((n.properties || {}).name || '').toUpperCase().trim();
            if (dname && dirNameToNodeId[dname]) {
              remap[n.id] = dirNameToNodeId[dname];
              return;
            }
            if (dname) dirNameToNodeId[dname] = n.id;
          }

          // Check if this Person already exists by name
          if (n.group === 'Person') {
            var pname = ((n.properties || {}).name || '').toUpperCase().trim();
            if (pname && personNameToNodeId[pname]) {
              remap[n.id] = personNameToNodeId[pname];
              return;
            }
            if (pname) personNameToNodeId[pname] = n.id;
          }

          // If existing graph uses levels but this node doesn't have one, assign one
          if (hasLevels && n.level === undefined) {
            // Find a connected edge to determine level relative to a known node
            var parentLevel = null;
            (d.edges || []).forEach(function(e) {
              if (e.from === n.id) {
                var target = nodes.get(e.to);
                if (target && target.level !== undefined) parentLevel = target.level - 1;
              }
              if (e.to === n.id) {
                var source = nodes.get(e.from);
                if (source && source.level !== undefined) parentLevel = source.level + 1;
              }
            });
            if (parentLevel === null) {
              // Check existing edges too
              edges.get().forEach(function(e) {
                if (e.from === n.id) {
                  var target = nodes.get(e.to);
                  if (target && target.level !== undefined) parentLevel = target.level - 1;
                }
                if (e.to === n.id) {
                  var source = nodes.get(e.from);
                  if (source && source.level !== undefined) parentLevel = source.level + 1;
                }
              });
            }
            if (parentLevel !== null) n.level = parentLevel;
            else n.level = 0;  // fallback
          }
          nodes.add(n);
          added++;
      });
      var existing = edges.get();
      (d.edges || []).forEach(function(e) {
        // Apply remap to edge endpoints
        var from = remap[e.from] || e.from;
        var to = remap[e.to] || e.to;
        var dup = existing.some(function(ex) { return ex.from === from && ex.to === to && ex.label === e.label; });
        if (!dup && nodes.get(from) && nodes.get(to)) {
          edges.add({ from: from, to: to, label: e.label, title: e.title, arrows: e.arrows, color: e.color, width: e.width });
          added++;
        }
      });
      updateStats();
      if (added > 0 && !initialFitDone) {
        initialFitDone = true;
        network.fit({ animation: true });
      }
    }

    function updateStats() {
      document.getElementById('stats').textContent = nodes.length + ' nodes, ' + edges.length + ' edges';
      // Track root node
      if (!rootNodeId) {
        var allN = nodes.get();
        for (var i = 0; i < allN.length; i++) {
          if (allN[i].group === 'Company' && (allN[i].properties || {}).companyNumber === '{{ company }}') {
            rootNodeId = allN[i].id;
            break;
          }
        }
      }
    }

    function focusRoot() {
      if (rootNodeId) focusNode(rootNodeId);
    }

    function focusNode(nid) {
      network.focus(nid, { scale: 1.2, animation: { duration: 500, easingFunction: 'easeInOutQuad' } });
      network.selectNodes([nid]);
    }

    function setStatus(msg, cls) {
      var el = document.getElementById('status');
      el.className = cls || '';
      el.textContent = msg;
    }

    // Stream data from server via SSE
    var urlParams = new URLSearchParams(window.location.search);
    var showFormer = urlParams.get('former') === '1';
    var streamUrl = '/api/stream?company={{ company }}' + (showFormer ? '&former=1' : '');
    var evtSource = new EventSource(streamUrl);
    var rootNodeId = null;
    evtSource.addEventListener('status', function(e) {
      setStatus(e.data);
      // Capture company name from "Found: COMPANY NAME" status
      if (e.data.indexOf('Found: ') === 0) {
        var name = e.data.substring(7);
        document.getElementById('title').textContent = name + ' (' + '{{ company }}' + ')';
        document.title = name + ' — CH Graphs';
      }
    });
    evtSource.addEventListener('data', function(e) {
      var d = JSON.parse(e.data);
      mergeData(d);
    });
    evtSource.addEventListener('done', function(e) {
      setStatus(e.data || 'Complete', 'done');
      updateStats();
      evtSource.close();
    });
    evtSource.addEventListener('error_msg', function(e) {
      setStatus(e.data, 'error');
      evtSource.close();
    });
    evtSource.onerror = function() { evtSource.close(); };

    // Auto-resolve: keep expanding all unexpanded Company/CorporateEntity nodes
    var autoRunning = false;
    var expanded = {};
    function autoResolve() {
      if (autoRunning) { autoRunning = false; document.getElementById('autoBtn').textContent = 'Auto-resolve all'; return; }
      autoRunning = true;
      document.getElementById('autoBtn').textContent = 'Stop auto-resolve';
      autoHop = 0;
      autoMaxHops = parseInt(document.getElementById('autoMaxHops').value) || 3;
      // Track which companies existed before this hop (to find new ones after)
      autoPrevCompanies = new Set(nodes.get().filter(function(n) { return n.group === 'Company'; }).map(function(n) { return (n.properties || {}).companyNumber || ''; }));
      autoHopStep('companies');
    }

    var autoHop = 0;
    var autoMaxHops = 3;
    var autoPrevCompanies = new Set();

    function autoHopStep(phase) {
      if (!autoRunning) { document.getElementById('autoStatus').textContent = 'Stopped'; return; }

      if (phase === 'companies') {
        // Find unexpanded companies in the current frontier
        var allNodes = nodes.get();
        var target = null;
        for (var i = 0; i < allNodes.length; i++) {
          var n = allNodes[i];
          var cn = (n.properties || {}).companyNumber || '';
          if (n.group === 'Company' && cn && !expanded['company:' + cn]) {
            target = { type: 'company', id: cn, key: 'company:' + cn };
            break;
          }
          if (n.group === 'CorporateEntity' && !expanded['corporate:' + n.id]) {
            var regNum = (n.properties || {}).registrationNumber || '';
            if (regNum) {
              var padded = /^\\d+$/.test(regNum) ? ('00000000' + regNum).slice(-8) : regNum;
              if (!expanded['company:' + padded]) {
                target = { type: 'company', id: padded, key: 'company:' + padded };
                break;
              }
            }
            expanded['corporate:' + n.id] = true;
          }
        }

        if (!target) {
          // All companies in this hop expanded — now fetch directors for this hop
          autoHopStep('directors');
          return;
        }

        expanded[target.key] = true;
        document.getElementById('autoStatus').textContent = 'Hop ' + (autoHop + 1) + '/' + autoMaxHops + ': expanding ' + target.id + ' (' + allNodes.length + ' nodes)';
        fetch('/api/expand?type=' + encodeURIComponent(target.type) + '&id=' + encodeURIComponent(target.id))
          .then(function(r) { return r.json(); })
          .then(function(d) {
            if (d.nodes && d.nodes.length > 0) mergeData(d);
            setTimeout(function() { autoHopStep('companies'); }, 200);
          })
          .catch(function(e) {
            document.getElementById('autoStatus').textContent = 'Error: ' + e;
            setTimeout(function() { autoHopStep('companies'); }, 500);
          });

      } else if (phase === 'directors') {
        // Fetch directors for all companies that don't have them
        var allNodes = nodes.get();
        var target = null;
        for (var j = 0; j < allNodes.length; j++) {
          var nd = allNodes[j];
          var cnum = (nd.properties || {}).companyNumber || '';
          if (nd.group === 'Company' && cnum && !expanded['directors:' + cnum]) {
            target = { type: 'directors', id: cnum, key: 'directors:' + cnum };
            break;
          }
        }

        if (!target) {
          // Directors done for this hop — check if we should do another hop
          autoHop++;
          if (autoHop >= autoMaxHops) {
            autoRunning = false;
            document.getElementById('autoBtn').textContent = 'Auto-resolve all';
            document.getElementById('autoStatus').textContent = 'Done — ' + autoHop + ' hops (' + allNodes.length + ' nodes, ' + edges.length + ' edges)';
            network.fit({ animation: true });
            return;
          }
          // Check if new companies were discovered in this hop
          var currentCompanies = new Set(nodes.get().filter(function(n) { return n.group === 'Company'; }).map(function(n) { return (n.properties || {}).companyNumber || ''; }));
          var newCount = 0;
          currentCompanies.forEach(function(cn) { if (!autoPrevCompanies.has(cn)) newCount++; });
          if (newCount === 0) {
            autoRunning = false;
            document.getElementById('autoBtn').textContent = 'Auto-resolve all';
            document.getElementById('autoStatus').textContent = 'Done — no new companies after hop ' + autoHop + ' (' + allNodes.length + ' nodes)';
            network.fit({ animation: true });
            return;
          }
          autoPrevCompanies = currentCompanies;
          document.getElementById('autoStatus').textContent = 'Starting hop ' + (autoHop + 1) + '/' + autoMaxHops + ' (' + newCount + ' new companies)...';
          setTimeout(function() { autoHopStep('companies'); }, 200);
          return;
        }

        expanded[target.key] = true;
        document.getElementById('autoStatus').textContent = 'Hop ' + (autoHop + 1) + ': directors for ' + target.id;
        fetch('/api/expand?type=' + encodeURIComponent(target.type) + '&id=' + encodeURIComponent(target.id))
          .then(function(r) { return r.json(); })
          .then(function(d) {
            if (d.nodes && d.nodes.length > 0) mergeData(d);
            setTimeout(function() { autoHopStep('directors'); }, 200);
          })
          .catch(function(e) {
            document.getElementById('autoStatus').textContent = 'Error: ' + e;
            setTimeout(function() { autoHopStep('directors'); }, 500);
          });
      }
    }
  </script>
</body>
</html>"""


@app.route("/graph")
def graph():
    company = request.args.get("company", "").strip().upper()
    if not company:
        return redirect(url_for("home"))
    return render_template_string(GRAPH_PAGE_TEMPLATE, company=company)


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
            if not dir_records:
                resigned_filter = "" if include_former else "WHERE r.resignedOn IS NULL OR r.resignedOn = '' "
                with d.session() as session:
                    dir_records = list(session.run(
                        f"MATCH dirPath = (dd:Director)-[r:OFFICER_OF]->(c:Company {{companyNumber: $cn}}) "
                        f"{resigned_filter}"
                        f"RETURN dirPath AS path", cn=company
                    ))

            if dir_records:
                # Combine ALL records (ownership + directors) for proper merge
                all_records.extend(dir_records)
                all_nodes, all_rels, _ = extract_graph_data(all_records)
                vis = _build_vis_data(all_nodes, all_rels)
                # Send as chunks
                chunk_size = 100
                vn = vis.get("nodes", [])
                ve = vis.get("edges", [])
                for i in range(0, max(len(vn), len(ve)), chunk_size):
                    chunk = {"nodes": vn[i:i+chunk_size], "edges": ve[i:i+chunk_size]}
                    if chunk["nodes"] or chunk["edges"]:
                        yield send("data", _json.dumps(chunk, default=str))
                yield send("status", f"{len(vn)} nodes, {len(ve)} edges (merged)")

        stats["graphs_served"] += 1
        yield send("done", "Complete")

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


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
            if "secretary" in role: return "#78909C", 1.5  # grey-blue for secretaries
            return "#00BCD4", 2.0  # cyan for directors
        noc_list = props.get("naturesOfControl", [])
        if not noc_list: return "#666", 1.5
        noc_str = " ".join(noc_list) if isinstance(noc_list, list) else str(noc_list)
        if "75-to-100" in noc_str: return "#e74c3c", 3.5
        elif "50-to-75" in noc_str: return "#e67e22", 2.5
        elif "25-to-50" in noc_str: return "#f1c40f", 2.0
        elif "right-to-appoint" in noc_str: return "#9b59b6", 2.0
        elif "significant-influence" in noc_str: return "#3498db", 2.0
        return "#95a5a6", 1.5

    vis_edges = []
    for r in merged_rels:
        ek = (r["startId"], r["endId"], r["type"])
        if ek in seen_edges: continue
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
    """Build the ownership tree Cypher query.
    direction: 'up' = who controls this company, 'down' = what it controls, 'both' """
    if direction == "both":
        # Bidirectional: traverse HAS_SIGNIFICANT_CONTROL in both directions + IS_COMPANY
        return (
            f"MATCH (c:Company {{companyNumber: '{company}'}}) "
            f"CALL apoc.path.expandConfig(c, {{"
            f"  relationshipFilter: 'HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
            f"  minLevel: 1, maxLevel: 30, "
            f"  uniqueness: 'NODE_GLOBAL'"
            f"}}) YIELD path RETURN path"
        )
    else:
        # Upward only (who controls this company)
        return (
            f"MATCH (c:Company {{companyNumber: '{company}'}}) "
            f"CALL apoc.path.expandConfig(c, {{"
            f"  relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
            f"  minLevel: 1, maxLevel: 30, "
            f"  uniqueness: 'NODE_GLOBAL'"
            f"}}) YIELD path RETURN path"
        )


def _directors_query(company, include_former=False):
    """Get directors for all companies in the ownership tree."""
    where_clause = "" if include_former else "WHERE r.resignedOn IS NULL OR r.resignedOn = '' "
    return (
        f"MATCH (c:Company {{companyNumber: '{company}'}}) "
        f"CALL apoc.path.expandConfig(c, {{"
        f"  relationshipFilter: 'HAS_SIGNIFICANT_CONTROL, IS_COMPANY', "
        f"  minLevel: 0, maxLevel: 30, "
        f"  uniqueness: 'NODE_GLOBAL'"
        f"}}) YIELD path "
        f"UNWIND nodes(path) AS n WITH n WHERE n:Company "
        f"MATCH dirPath = (dd:Director)-[r:OFFICER_OF]->(n) "
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
        with d.session() as session:
            # Get companies this person controls/is officer of
            person_records = list(session.run(
                "MATCH (n) WHERE elementId(n) = $nid "
                "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL|OFFICER_OF]->(c:Company) "
                "RETURN path SKIP $skip LIMIT $lim",
                nid=expand_id, skip=offset, lim=limit + 1,
            ))

            # If no results (possibly deduped node), try matching Director by name
            if not person_records:
                # Get the node's name from Neo4j
                name_result = session.run(
                    "MATCH (n) WHERE elementId(n) = $nid RETURN n.name AS name", nid=expand_id
                )
                name_rec = name_result.single()
                if name_rec and name_rec["name"]:
                    name = name_rec["name"]
                    person_records = list(session.run(
                        "MATCH (n:Director) WHERE n.name = $name "
                        "MATCH path = (n)-[:OFFICER_OF]->(c:Company) "
                        "RETURN path LIMIT $lim",
                        name=name, lim=limit,
                    ))
                    # Also try Person
                    person_records.extend(list(session.run(
                        "MATCH (n:Person) WHERE n.name = $name "
                        "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL]->(c:Company) "
                        "RETURN path LIMIT $lim",
                        name=name, lim=limit,
                    )))
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
