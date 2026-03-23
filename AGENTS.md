# AGENTS.md — AI/LLM Codebase Guide

This document is the primary reference for AI agents and LLMs working on the `ch-graphs` codebase. Read it in full before making any changes.

---

## Project Overview

**ch-graphs** loads UK Companies House bulk data into a Neo4j graph database and provides tools to explore corporate ownership structures, identify persons with significant control (PSCs), and trace ownership chains across millions of companies.

Key capabilities:
- Imports ~5.7M companies and ~15.3M PSC records into Neo4j via bulk CSV import
- Builds a directed ownership graph (who controls whom)
- Cross-references corporate PSCs back to their company records (enabling chain traversal)
- Fetches director/officer data on demand via the Companies House REST API, with ETag-based caching
- Exports interactive, self-contained HTML graph visualisations
- Provides a Flask web UI for searching and interactive exploration

---

## Repository Layout

```
ch-graphs/
├── load_data.py          # Phase 1: stream-converts raw CH data → neo4j-admin import CSVs
├── search.py             # CLI search tool + graph export (CSV, JSON, HTML)
├── fetch_directors.py    # Fetches director/officer data from CH API → Neo4j
├── web.py                # Flask web application (search UI + live graph viewer)
├── requirements.txt      # Python dependencies
├── Dockerfile            # Builds the Flask web container
├── docker-compose.yml    # Runs Neo4j + web service together
├── .env.example          # Template for environment variables
├── data/                 # Raw input files (not committed); data/import/ holds generated CSVs
└── exports/              # Output HTML/JSON exports (gitkeep placeholder committed)
```

There are **no tests** and **no linter configuration** in this project. Do not add them unless explicitly asked.

---

## Architecture

### Two-Phase Data Loading

**Phase 1 — `load_data.py`**: Reads the raw Companies House files (one large CSV, one large JSONL) and streams them into neo4j-admin import CSVs in `data/import/`. Uses `ProcessPoolExecutor` to process companies and PSC data in parallel. Designed to be RAM-safe (streams line by line, no in-memory accumulation of full datasets).

**Phase 2 — `neo4j-admin database import full`**: Run manually (outside Python) against the generated CSVs. This uses Neo4j's bulk importer for speed (millions of rows in minutes). See `README.md` for the exact command.

### Runtime Services

```
docker-compose.yml
├── neo4j        — Neo4j 5 Community with APOC plugin (Bolt :7687, Browser :7474)
└── web          — Flask/Gunicorn app (gevent workers) exposed on :8080
```

The `web` service depends on Neo4j being healthy before starting.

### Module Dependencies

`web.py` imports from both `search.py` and `fetch_directors.py` at startup:

```python
from fetch_directors import fetch_officers, load_officers_to_neo4j, get_tree_company_numbers, get_fetch_metadata, needs_refresh, STAMP_FETCH_QUERY
from search import extract_graph_data, _compute_levels
```

This means `search.py` and `fetch_directors.py` must remain importable as modules (they have `if __name__ == "__main__"` guards for CLI use).

---

## Graph Data Model

All node and relationship types used in Neo4j:

### Node Labels

| Label | Key Properties | Description |
|---|---|---|
| `Company` | `companyNumber` (unique ID), `name`, `status`, `category`, `postcode`, `incorporationDate`, `dissolutionDate` | A UK registered company |
| `Person` | `personId` (composite ID), `name`, `surname`, `forename`, `nationality`, `dobMonth`, `dobYear` | An individual PSC |
| `CorporateEntity` | `entityId` (composite ID), `name`, `registrationNumber`, `legalForm`, `countryRegistered` | A corporate PSC (may be cross-referenced to a `Company` node) |
| `LegalPerson` | `entityId` (composite ID), `name`, `legalForm`, `legalAuthority` | A legal person PSC (trusts, government bodies, etc.) |
| `Director` | `directorId` (stable ID from CH API self-link), `name`, `nationality`, `occupation` | An officer/director fetched from the CH REST API |
| `Address` | `addressId` (postcode+line1), `postcode`, `addressLine1`, `postTown` | A registered address (deduplicated across companies) |
| `SICCode` | `code` (unique), `description` | Standard Industrial Classification code |

### Relationship Types

| Relationship | From → To | Key Properties | Description |
|---|---|---|---|
| `HAS_SIGNIFICANT_CONTROL` | Person/CorporateEntity/LegalPerson → Company | `naturesOfControl[]`, `notifiedOn`, `ceasedOn` | PSC ownership/control link |
| `IS_COMPANY` | CorporateEntity → Company | — | Links a CorporateEntity PSC to its Companies House record |
| `OFFICER_OF` | Director → Company | `role`, `appointedOn`, `resignedOn` | Director/officer appointment |
| `REGISTERED_AT` | Company → Address | — | Registered address |
| `HAS_SIC` | Company → SICCode | — | Industry classification |

### Critical: IS_COMPANY Direction

`IS_COMPANY` goes **CorporateEntity → Company** (forward, never reversed). This was a deliberate fix. When displaying graphs, `CorporateEntity` nodes linked via `IS_COMPANY` are **merged into** their corresponding `Company` node visually (see `export_html` in `search.py`).

### Company Number Normalisation

Purely numeric company numbers are **zero-padded to 8 digits** everywhere (see `pad_company_number()` in `load_data.py` and `fetch_directors.py`). This is critical for cross-referencing. E.g. `253240` → `00253240`. Non-numeric numbers (Scottish `SC`, Welsh `NI`, etc.) are uppercased but not padded.

### Person ID Construction

Person IDs are composite keys: `FORENAME|MIDDLE_NAME|SURNAME|DOB_MONTH|DOB_YEAR` (uppercased). This deduplicates PSC individuals across companies without relying on a CH-assigned identifier.

### CorporateEntity ID Construction

Corporate entities are identified as either `REG|<padded_reg_number>` or `NAME|<uppercased_name>`, preferring registration number when available.

---

## Key Patterns

### .env Loading

All three Python scripts (`web.py`, `search.py`, `fetch_directors.py`) load `.env` manually at the top using the same pattern — they do **not** use `python-dotenv`. Precedence: existing env vars take priority (`os.environ.setdefault`).

### Neo4j Connection

Default connection settings are:
- URI: `bolt://localhost:7687` (overridden in Docker via `NEO4J_URI=bolt://neo4j:7687`)
- User: `neo4j`
- Password: `companies2026`

The driver is a global singleton in `web.py`, initialised lazily via `get_driver()`.

### APOC Dependency

The ownership tree traversal queries use `apoc.path.expandConfig`. APOC must be installed (it is in the Docker Compose config via `NEO4J_PLUGINS=["apoc"]`). Without APOC, ownership tree queries will fail.

The web app also uses `apoc.periodic.iterate` at startup to fix missing `IS_COMPANY` links for databases imported before the zero-padding fix was added.

### Director Data Freshness

Director data is cached in Neo4j on the `Company` node via two properties:
- `directorsFetchedAt` — Neo4j `datetime()` of last fetch
- `directorsEtag` — HTTP ETag from the CH API for conditional requests (HTTP 304)

Data older than `STALE_DAYS = 30` is considered stale and re-fetched. The CH API rate limit is 600 requests/5 minutes; code delays 0.2s between requests.

### HTML Graph Visualisation

Interactive graphs use the **vis-network** library loaded from CDN (`https://unpkg.com/vis-network`). The `export_html` function in `search.py` produces self-contained HTML with all graph data embedded as JSON. The web app's `/graph` route uses a different live-loading approach (AJAX via `/api/graph-data`).

**Node merging**: During HTML export, `CorporateEntity` nodes with an `IS_COMPANY` link are merged into their corresponding `Company` node to avoid duplicates in the visualisation.

**Hierarchy levels**: `_compute_levels()` uses BFS to assign vertical levels — the controlled company (root) gets the highest level number, controllers sit above it.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CH_API_KEY` (or `CH_API`) | — | Companies House API key (required for director fetching) |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `companies2026` | Neo4j password |

---

## Neo4j Indexes

These must be created once after import (run in the Neo4j browser at `http://localhost:7474`):

```cypher
CREATE FULLTEXT INDEX company_name_fulltext IF NOT EXISTS FOR (c:Company) ON EACH [c.name];
CREATE FULLTEXT INDEX person_name_fulltext IF NOT EXISTS FOR (p:Person) ON EACH [p.name];
CREATE INDEX company_number IF NOT EXISTS FOR (c:Company) ON (c.companyNumber);
CREATE INDEX company_name IF NOT EXISTS FOR (c:Company) ON (c.name);
CREATE INDEX company_status IF NOT EXISTS FOR (c:Company) ON (c.status);
CREATE INDEX company_postcode IF NOT EXISTS FOR (c:Company) ON (c.postcode);
CREATE INDEX person_surname IF NOT EXISTS FOR (p:Person) ON (p.surname);
CREATE INDEX corp_entity_name IF NOT EXISTS FOR (ce:CorporateEntity) ON (ce.name);
CREATE INDEX corp_entity_reg IF NOT EXISTS FOR (ce:CorporateEntity) ON (ce.registrationNumber);
CREATE INDEX address_postcode IF NOT EXISTS FOR (a:Address) ON (a.postcode);
```

Director indexes are created automatically by `fetch_directors.py` / the web app on first run:
```cypher
CREATE CONSTRAINT director_id IF NOT EXISTS FOR (d:Director) REQUIRE d.directorId IS UNIQUE;
CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.name);
CREATE FULLTEXT INDEX director_name_fulltext IF NOT EXISTS FOR (d:Director) ON EACH [d.name];
```

---

## Web App Routes

| Route | Method | Description |
|---|---|---|
| `/` | GET | Home page with company number search and name search |
| `/search?q=<name>` | GET | Full-text name search, returns list of matching companies |
| `/graph?company=<num>` | GET | Interactive graph viewer for a company's ownership tree |
| `/api/graph-data?company=<num>&depth=<n>&directors=<0/1>&former=<0/1>` | GET | JSON graph data for the live viewer |
| `/api/expand?type=<type>&id=<id>` | GET | Incrementally expand a node (subsidiaries, address, person, directors) |
| `/exports/<filename>` | GET | Serve a static exported HTML file |

The `/api/graph-data` endpoint fetches director data automatically if `CH_API_KEY` is configured and data is stale.

---

## CLI Tools

### `search.py`

```bash
python search.py --company 00253240 --format html json   # ownership tree export
python search.py --name "tesco" --format html            # name search + export
python search.py --person "Smith" --limit 20             # PSC search
python search.py --cypher "MATCH (c:Company) WHERE c.postcode='SW1A 2DD' RETURN c" --format html
```

Options: `--depth N` (default 20), `--format csv json html` (default: print to stdout), `--output PATH`, `--uri`, `--user`, `--password`.

### `fetch_directors.py`

```bash
python fetch_directors.py --company 00253240 --follow-tree   # fetch whole tree
python fetch_directors.py --company 00253240 --force          # skip freshness check
python fetch_directors.py --company 00253240 --active-only    # skip resigned officers
```

### `load_data.py`

```bash
python load_data.py   # generates data/import/*.csv from data/*.csv and data/*.txt
```

Expects files matching `data/BasicCompanyDataAsOneFile-*.csv` and `data/persons-with-significant-control-snapshot-*.txt`.

---

## Important Caveats for AI Agents

1. **No test suite exists.** Do not invent or run tests. Validate logic by reading the code carefully.

2. **Data files are not in the repo.** `data/` only contains `data/import/` (empty). The raw CH bulk files are too large to commit and must be downloaded separately.

3. **APOC is required.** Any Cypher query using `apoc.*` functions will fail without the APOC plugin. Do not replace APOC queries with pure Cypher unless you verify the alternative works for large graphs (millions of nodes).

4. **Company number zero-padding is critical.** Always use `pad_company_number()` when comparing or cross-referencing company numbers. Inconsistent padding causes silent mismatches.

5. **`search.py` has duplicate function implementations.** `fetch_officers`, `make_director_id`, `load_officers_to_neo4j` exist in both `search.py` and `fetch_directors.py`. The `web.py` app imports these from `fetch_directors.py`. The copies in `search.py` are used only when running `search.py` as a standalone CLI script. Avoid introducing further divergence between these copies.

6. **`IS_COMPANY` direction is forward only (CorporateEntity → Company).** Do not reverse this. The `_fix_is_company_links` startup function in `web.py` patches legacy databases that had this wrong.

7. **`naturesOfControl` is a semicolon-delimited string array** in the import CSVs (Neo4j array delimiter is `;`). In the graph it becomes a list property. Always treat it as a list when reading from Neo4j.

8. **The web app uses inline HTML templates** (multi-line strings in `web.py`), not separate template files. When modifying UI, edit these strings directly.

9. **Memory requirements:** The import process streams data so RAM usage during `load_data.py` is modest. However, Neo4j needs ~16GB RAM for the full dataset (configurable in `docker-compose.yml`).

10. **The exports directory** is committed with only a `.gitkeep`. Never commit actual HTML/JSON exports.
