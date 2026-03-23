# GitHub Copilot Custom Instructions for ch-graphs

## Project context

This is a UK Companies House graph analysis tool. It loads millions of company and PSC (Persons with Significant Control) records into Neo4j and provides tools to explore corporate ownership chains.

Read `AGENTS.md` for full codebase documentation before making any changes.

## Stack

- **Python 3.10+** — all scripts; no framework for CLI tools, Flask for web
- **Neo4j 5 (Community)** with APOC plugin — graph database
- **vis-network** (CDN) — interactive graph visualisation in HTML exports
- **Docker Compose** — runs Neo4j + Flask web service
- **Companies House REST API** — fetches director/officer data on demand

## Key rules

- There are **no tests**. Do not add tests unless explicitly requested.
- There is **no linter config**. Follow the existing code style (PEP 8, single quotes, 4-space indent).
- Do not add new Python dependencies unless strictly necessary.
- Always use `pad_company_number()` when working with company numbers — purely numeric numbers must be zero-padded to 8 digits.
- `IS_COMPANY` relationships go **CorporateEntity → Company** (forward only).
- APOC functions (`apoc.path.expandConfig`, `apoc.periodic.iterate`) are available and intentional — do not replace them with pure Cypher alternatives without good reason.
- `naturesOfControl` is stored as a semicolon-delimited array in Neo4j. Treat it as a list.
- The web app uses inline HTML template strings in `web.py`, not separate template files.
- `search.py` and `fetch_directors.py` contain some duplicate function implementations (by design). `web.py` imports from `fetch_directors.py`. Keep these in sync when modifying shared logic.
- The `exports/` directory should never have actual export files committed — only `.gitkeep`.
- The `data/` directory is not committed (bulk files are too large). Only `data/import/` exists as a directory.

## Graph data model summary

```
(Person)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(CorporateEntity)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(CorporateEntity)-[:IS_COMPANY]->(Company)        ← forward only
(LegalPerson)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(Director)-[:OFFICER_OF]->(Company)
(Company)-[:REGISTERED_AT]->(Address)
(Company)-[:HAS_SIC]->(SICCode)
```

## Default Neo4j credentials (local dev)

- URI: `bolt://localhost:7687`
- User: `neo4j`
- Password: `companies2026`
