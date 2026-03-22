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
      <button class="btn" id="layoutBtn" onclick="toggleLayout()">Layout: Tree</button>
      <button class="btn" onclick="network.fit()">Fit View</button>
      <button class="btn" id="autoBtn" onclick="autoResolve()">Auto-resolve all</button>
      <label style="font-size:11px; color:#888; margin-left:4px;">Max: <input type="number" id="autoMax" value="200" min="10" max="5000" style="width:55px; background:#1a1a2e; color:#eee; border:1px solid #4a4a7a; border-radius:3px; padding:2px 4px; font-size:11px;"></label>
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
    var hierarchical = true;

    var hierOpts = {
      layout: { hierarchical: { enabled: true, direction: 'UD', sortMethod: 'directed',
        levelSeparation: 150, nodeSpacing: 250, treeSpacing: 250 } },
      physics: { enabled: true, hierarchicalRepulsion: { centralGravity: 0.0,
        springLength: 150, springConstant: 0.01, nodeDistance: 200, damping: 0.09 },
        stabilization: { iterations: 300 } },
      nodes: { font: { color: '#eee', size: 10, multi: 'md', face: 'arial' },
        borderWidth: 2, widthConstraint: { maximum: 150 } },
      edges: { color: { color: '#666', highlight: '#fff', hover: '#aaa' },
        font: { color: '#999', size: 8, strokeWidth: 0, background: 'rgba(0,0,0,0.5)' },
        smooth: { type: 'cubicBezier', forceDirection: 'vertical', roundness: 0.4 }, width: 1.5 },
      interaction: { hover: true, tooltipDelay: 100, navigationButtons: true, keyboard: true }
    };
    var forceOpts = { layout: { hierarchical: { enabled: false } },
      physics: { solver: 'forceAtlas2Based', forceAtlas2Based: { gravitationalConstant: -200, springLength: 250, springConstant: 0.02, damping: 0.4 },
        stabilization: { iterations: 300 } },
      nodes: hierOpts.nodes, edges: Object.assign({}, hierOpts.edges, { smooth: { type: 'continuous' } }),
      interaction: hierOpts.interaction };

    var network = new vis.Network(container, data, hierOpts);
    // Auto-disable physics after stabilization so nodes stay in place
    network.on('stabilizationIterationsDone', function() { network.setOptions({ physics: { enabled: false } }); });
    function toggleLayout() {
      hierarchical = !hierarchical;
      network.setOptions(hierarchical ? hierOpts : forceOpts);
      var btn = document.getElementById('layoutBtn');
      btn.textContent = hierarchical ? 'Layout: Tree' : 'Layout: Force';
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
      } else { panel.style.display = 'none'; }
    });

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

      // Remap table: new node ID -> existing node ID (for merging)
      var remap = {};

      (d.nodes || []).forEach(function(n) {
        if (!nodes.get(n.id)) {
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
        }
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
      if (added > 0) {
        // Auto-switch to force layout when graph gets large
        if (hierarchical && nodes.length > 50) {
          hierarchical = false;
          network.setOptions(forceOpts);
        }
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
      // Switch to force layout for large auto-resolved graphs
      if (hierarchical) { hierarchical = false; network.setOptions(forceOpts); }
      autoRunning = true;
      document.getElementById('autoBtn').textContent = 'Stop auto-resolve';
      autoStep();
    }
    function autoStep() {
      if (!autoRunning) { document.getElementById('autoStatus').textContent = 'Stopped'; return; }
      var allNodes = nodes.get();

      // Safety limit
      var maxNodes = parseInt(document.getElementById('autoMax').value) || 200;
      if (allNodes.length >= maxNodes) {
        autoRunning = false;
        document.getElementById('autoBtn').textContent = 'Auto-resolve all';
        document.getElementById('autoStatus').textContent = 'Stopped — limit of ' + maxNodes + ' nodes reached (' + allNodes.length + ' nodes)';
        network.fit({ animation: true });
        return;
      }

      var target = null;

      // Phase 1: Find unexpanded Company or CorporateEntity nodes
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

      // Phase 2: If all ownership trees expanded, fetch directors for companies missing them
      if (!target) {
        for (var j = 0; j < allNodes.length; j++) {
          var nd = allNodes[j];
          var cnum = (nd.properties || {}).companyNumber || '';
          if (nd.group === 'Company' && cnum && !expanded['directors:' + cnum]) {
            target = { type: 'directors', id: cnum, key: 'directors:' + cnum };
            break;
          }
        }
      }

      if (!target) {
        autoRunning = false;
        document.getElementById('autoBtn').textContent = 'Auto-resolve all';
        document.getElementById('autoStatus').textContent = 'Done — all nodes resolved (' + allNodes.length + ' nodes, ' + edges.length + ' edges)';
        network.fit({ animation: true });
        return;
      }
      expanded[target.key] = true;
      var count = Object.keys(expanded).length;
      var label = target.type === 'directors' ? 'directors for ' + target.id : target.id;
      document.getElementById('autoStatus').textContent = 'Resolving ' + label + ' (' + count + ' expanded, ' + allNodes.length + ' nodes)...';
      fetch('/api/expand?type=' + encodeURIComponent(target.type) + '&id=' + encodeURIComponent(target.id))
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.nodes && d.nodes.length > 0) mergeData(d);
          // Re-check cap after merge
          var maxN = parseInt(document.getElementById('autoMax').value) || 200;
          if (nodes.length >= maxN) {
            autoRunning = false;
            document.getElementById('autoBtn').textContent = 'Auto-resolve all';
            document.getElementById('autoStatus').textContent = 'Stopped — ' + nodes.length + ' nodes (limit ' + maxN + ')';
            network.fit({ animation: true });
            return;
          }
          setTimeout(autoStep, 200);
        })
        .catch(function(e) {
          document.getElementById('autoStatus').textContent = 'Error on ' + label + ': ' + e;
          setTimeout(autoStep, 500);
        });
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
        name = _norm_name(n["properties"].get("name", ""))
        if "," in name:
            parts = name.split(",", 1)
            sn = parts[0].strip()
            fn = parts[1].strip().split()[0] if parts[1].strip() else ""
            return sn + "|" + fn
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

    # Compute hierarchy levels
    levels = _compute_levels(merged_nodes, merged_rels)

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
            "size": node_sizes.get(label, 15), "shape": node_shapes.get(label, "dot"),
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
        vis_edges.append({"from": r["startId"], "to": r["endId"], "label": elabel,
                          "title": noc or r["type"], "arrows": "to",
                          "color": {"color": col, "highlight": "#fff", "hover": col}, "width": width})

    return {"nodes": vis_nodes, "edges": vis_edges}


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
            records = list(session.run(
                "MATCH (n) WHERE elementId(n) = $nid "
                "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL|OFFICER_OF]->(c:Company) "
                "RETURN path SKIP $skip LIMIT $lim",
                nid=expand_id, skip=offset, lim=limit + 1,
            ))
            if len(records) > limit:
                records = records[:limit]
                has_more = True

    elif expand_type == "corporate":
        with d.session() as session:
            records = list(session.run(
                "MATCH (n) WHERE elementId(n) = $nid "
                "MATCH path = (n)-[:HAS_SIGNIFICANT_CONTROL]->(c:Company) "
                "RETURN path SKIP $skip LIMIT $lim",
                nid=expand_id, skip=offset, lim=limit + 1,
            ))
            if len(records) > limit:
                records = records[:limit]
                has_more = True

    elif expand_type == "directors":
        company = expand_id.upper()
        if CH_API_KEY:
            # Direct fetch for this specific company (not tree-based)
            from fetch_directors import fetch_officers as _fetch, load_officers_to_neo4j as _load, get_fetch_metadata as _meta, needs_refresh as _needs
            meta = _meta(d, [company])
            m = meta.get(company, {"fetchedAt": None, "etag": None})
            if _needs(m["fetchedAt"]):
                print(f"  [directors] Fetching officers for {company}...", flush=True)
                officers, new_etag, modified = _fetch(CH_API_KEY, company, etag=m.get("etag"))
                if modified and officers:
                    _load(d, company, officers, etag=new_etag)
                    print(f"  [directors] Loaded {len(officers)} officers for {company}", flush=True)
                elif not modified:
                    from fetch_directors import STAMP_FETCH_QUERY as _stamp
                    with d.session() as session:
                        session.run(_stamp, cn=company, etag=m.get("etag") or "")
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
