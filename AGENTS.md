# CH Graphs — Agent Instructions

This project loads UK Companies House data into Neo4j and provides a Flask web interface and CLI tools for exploring corporate ownership graphs.

## Project layout

```
ch-graphs/
├── web.py               # Flask app — routes, SSE streaming, API endpoints
├── search.py            # CLI search/export tool + shared graph data helpers
├── vis.py               # vis-network data builder (_build_vis_data, _compute_positions)
├── fetch_directors.py   # Companies House API fetcher for director/officer data
├── load_data.py         # Bulk CSV generator for neo4j-admin import
├── templates/           # Jinja2 templates (web.py) and string.Template exports (search.py)
│   ├── graph.html       # Interactive graph page — vis-network, SSE streaming, expand API
│   ├── home.html        # Homepage with company number and name search
│   ├── search.html      # Name search results listing
│   ├── loading.html     # Loading spinner (reserved for future use)
│   └── export.html      # Standalone export — rendered via string.Template, not Jinja2
├── data/                # Input data files and generated import CSVs (gitignored)
└── exports/             # Generated HTML exports (gitignored)
```

## Module responsibilities

| Module | Purpose |
|--------|---------|
| `web.py` | Flask routes (`/`, `/search`, `/graph`, `/api/stream`, `/api/expand`, `/api/stats`). Calls `vis.py` for graph rendering and imports helpers from `fetch_directors.py` and `search.py`. |
| `search.py` | CLI tool and shared helpers: `extract_graph_data()`, `_compute_levels()`, `export_csv()`, `export_json()`, `export_html()`. |
| `vis.py` | Converts raw Neo4j records into vis-network JSON. Handles CE→Company merging, Person/Director deduplication, and server-side layout. Imports `_compute_levels` from `search.py`. |
| `fetch_directors.py` | Fetches officer data from the Companies House REST API and writes it to Neo4j. Used standalone or imported by `web.py`. |
| `load_data.py` | Streams raw CH bulk data into neo4j-admin import CSVs. Run once to prepare the database. |

## Templates

- **`templates/graph.html`, `home.html`, `search.html`, `loading.html`** — Jinja2 templates rendered by `web.py` via `render_template()`. They use standard Jinja2 syntax (`{{ var }}`, `{% if %}`, etc.).
- **`templates/export.html`** — A standalone self-contained HTML file rendered by `search.py` via `string.Template`. Uses `${PLACEHOLDER}` syntax for the four substitution points: `${VIS_NODES_JSON}`, `${VIS_EDGES_JSON}`, `${NODE_COUNT}`, `${EDGE_COUNT}`. All other `{` / `}` braces in the file are literal JavaScript and CSS. Do not use Jinja2 syntax in this file.

## Key conventions

- **Neo4j connection**: bolt://localhost:7687, user `neo4j`, password `companies2026`. All configurable via env vars (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`).
- **Companies House API key**: env var `CH_API_KEY` (also accepts `CH_API` as fallback). Used by `fetch_directors.py` and `web.py`. The web app degrades gracefully if the key is absent.
- **`.env` file**: Both `web.py` and `search.py` and `fetch_directors.py` load `.env` at startup using a lightweight parser (no `python-dotenv` dependency).
- **Company numbers**: Always zero-padded to 8 digits for purely numeric numbers (e.g. `253240` → `00253240`).
- **PSC natures of control**: Stored as semicolon-delimited arrays in Neo4j (`--array-delimiter=";"`).
- **Director freshness**: Cached in Neo4j with `directorsFetchedAt` and `directorsEtag` properties. Stale after 30 days (`STALE_DAYS`).

## Running the app

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # set CH_API_KEY if you have one

# Start Neo4j + web app
docker compose up -d
python web.py          # runs on http://localhost:5000
```

## Running the CLI tools

```bash
# Export ownership tree
python search.py --company 00253240 --format html json

# Fetch directors
python fetch_directors.py --company 00253240 --follow-tree

# Rebuild import CSVs
python load_data.py
```

## Dependencies

Listed in `requirements.txt`. Key packages: `flask`, `neo4j`, `requests`. No test suite or linter is configured — manually verify changes with Python's `ast.parse()` and by running the tools directly.
