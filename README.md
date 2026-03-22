# CH Graphs

UK Companies House data loaded into Neo4j for graph-based corporate ownership analysis.

## What it does

- Imports **5.7M companies** and **15.3M PSC (Persons with Significant Control)** records into Neo4j
- Builds a corporate ownership graph linking companies, individuals, corporate entities, and legal persons
- Cross-references corporate PSCs to their Companies House records, enabling **ownership chain traversal**
- Fetches director/officer data on demand via the Companies House API (with ETag-based caching)
- Exports interactive HTML graph visualisations you can share

## Graph model

```
(Person)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(CorporateEntity)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(CorporateEntity)-[:IS_COMPANY]->(Company)
(LegalPerson)-[:HAS_SIGNIFICANT_CONTROL]->(Company)
(Director)-[:OFFICER_OF]->(Company)
(Company)-[:REGISTERED_AT]->(Address)
(Company)-[:HAS_SIC]->(SICCode)
```

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- ~16GB RAM recommended (8GB minimum)
- Companies House bulk data files:
  - [Basic Company Data](http://download.companieshouse.gov.uk/en_output.html) (CSV, ~2.6GB)
  - [PSC Snapshot](http://download.companieshouse.gov.uk/en_pscdata.html) (JSON lines, ~12GB)

## Quick start

```bash
# 1. Clone and setup
git clone git@github.com:harricross/ch-graphs.git
cd ch-graphs
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # add your CH API key

# 2. Download data files into data/
mkdir -p data
# Place BasicCompanyDataAsOneFile-*.csv and persons-with-significant-control-snapshot-*.txt in data/

# 3. Generate import CSVs (streams line-by-line, RAM-safe)
python load_data.py

# 4. Start Neo4j and run bulk import
docker compose up -d neo4j
# Wait for Neo4j to be healthy, then:
docker compose down
docker run --rm \
  -v "$(pwd)/data/import:/import" \
  -v ch-graphs_neo4j_data:/data \
  neo4j:5-community \
  neo4j-admin database import full \
    --overwrite-destination \
    --nodes=Company=/import/companies.csv \
    --nodes=Address=/import/addresses.csv \
    --nodes=SICCode=/import/sic_codes.csv \
    --nodes=Person=/import/persons.csv \
    --nodes=CorporateEntity=/import/corporate_entities.csv \
    --nodes=LegalPerson=/import/legal_persons.csv \
    --relationships=REGISTERED_AT=/import/rel_registered_at.csv \
    --relationships=HAS_SIC=/import/rel_has_sic.csv \
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_individual.csv \
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_corporate.csv \
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_legal.csv \
    --relationships=IS_COMPANY=/import/rel_is_company.csv \
    --skip-bad-relationships \
    --skip-duplicate-nodes \
    --bad-tolerance=10000000 \
    --array-delimiter=";" \
    --trim-strings=true \
    neo4j

# 5. Start everything
docker compose up -d

# 6. Create indexes (run once in Neo4j browser at http://localhost:7474)
# See search.py output for index creation statements
```

## Usage

```bash
# Export ownership tree as interactive HTML + JSON
python search.py --company 00253240 --format html json

# Fetch directors for the ownership tree (requires CH API key)
python fetch_directors.py --company 00253240 --follow-tree

# Search by company name
python search.py --name "tesco" --format html

# Search by person
python search.py --person "Smith" --limit 20

# Custom Cypher query
python search.py --cypher "MATCH (c:Company) WHERE c.postcode = 'SW1A 2DD' RETURN c" --format html

# View exports
open exports/export_00253240.html     # or visit http://localhost:8080/
```

## Services

| Service | URL | Purpose |
|---------|-----|---------|
| Neo4j Browser | http://localhost:7474 | Query & explore the graph (neo4j/companies2026) |
| Exports | http://localhost:8080 | Browse exported HTML visualisations |

## Data freshness

Director data fetched via the API is cached in Neo4j with timestamps and ETags. Data older than 30 days is automatically refreshed on next query. The bulk company and PSC data reflects the snapshot date of the downloaded files.
