# CH Graphs — Copilot Instructions

This is a Python project that loads UK Companies House bulk data into Neo4j and provides a Flask web interface and CLI tools for exploring corporate ownership graphs.

## Project structure

```
web.py               Flask app (routes, SSE, API endpoints)
search.py            CLI search/export tool + shared graph helpers
vis.py               vis-network data builder (extracted from web.py)
fetch_directors.py   Companies House API fetcher for directors
load_data.py         Bulk CSV generator for neo4j-admin import
templates/           HTML templates
  graph.html         Interactive graph page (Jinja2, rendered by web.py)
  home.html          Homepage (Jinja2)
  search.html        Search results (Jinja2)
  loading.html       Loading spinner (Jinja2)
  export.html        Standalone export (string.Template, rendered by search.py)
data/                Input data files and generated import CSVs (gitignored)
exports/             Generated HTML exports (gitignored)
```

## Templates

- `templates/graph.html`, `home.html`, `search.html`, `loading.html` are **Jinja2** templates — use `{{ var }}`, `{% for %}`, `{% if %}` syntax.
- `templates/export.html` is a **`string.Template`** file — use `${PLACEHOLDER}` for the four injection points (`${VIS_NODES_JSON}`, `${VIS_EDGES_JSON}`, `${NODE_COUNT}`, `${EDGE_COUNT}`). All other braces are literal JavaScript/CSS.

## Module boundaries

- `vis.py` imports `_compute_levels` from `search.py`. It has no dependency on Flask.
- `web.py` imports `_build_vis_data`, `_compute_positions` from `vis.py` and `extract_graph_data` from `search.py`.
- `web.py` imports helper functions and constants from `fetch_directors.py`.
- `search.py` and `fetch_directors.py` are usable standalone from the CLI.

## Conventions

- Company numbers are zero-padded to 8 digits for purely numeric values.
- PSC `naturesOfControl` values are stored as semicolon-delimited arrays in Neo4j.
- Neo4j connection defaults: bolt://localhost:7687, user `neo4j`, password `companies2026`. Override via `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` env vars.
- Director data is cached in Neo4j for 30 days using ETags and timestamps on each Company node.
- The app loads `.env` at startup without `python-dotenv` — just a lightweight key=value parser.
- No test suite is configured. Use `python -c "import ast; ast.parse(open('file.py').read())"` to verify syntax after edits.
