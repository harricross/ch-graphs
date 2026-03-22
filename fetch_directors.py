#!/usr/bin/env python3
"""
Companies House Officers (Directors) Fetcher

Fetches director/officer data from the Companies House REST API and loads it
into the Neo4j graph database.

Requires a Companies House API key:
  https://developer.company-information.service.gov.uk/

Usage:
  # Fetch directors for a single company
  python fetch_directors.py --api-key YOUR_KEY --company 00253240

  # Fetch directors for a company and all companies in its ownership tree
  python fetch_directors.py --api-key YOUR_KEY --company 00253240 --follow-tree

  # Use an env var for the API key
  export CH_API_KEY=YOUR_KEY
  python fetch_directors.py --company 00253240 --follow-tree

Graph additions:
  Node:  (d:Director) — officers/directors of companies
  Relationships:
    (d:Director)-[:OFFICER_OF {role, appointedOn, resignedOn}]->(c:Company)
"""

import argparse
import os
import sys
import time

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

# Rate limit: Companies House allows 600 requests per 5 minutes
RATE_LIMIT_DELAY = 0.2  # seconds between requests


def fetch_officers(api_key, company_number, etag=None):
    """Fetch all officers for a company from the CH API.
    Returns (officers, etag, modified) where modified is False if 304 Not Modified."""
    officers = []
    start_index = 0
    items_per_page = 100
    response_etag = None

    while True:
        url = f"{CH_API_BASE}/company/{company_number}/officers"
        headers = {}
        # Only send ETag on the first page request
        if etag and start_index == 0:
            headers["If-None-Match"] = etag

        try:
            print(f"\n      GET {url}?start_index={start_index}", end="", flush=True)
            resp = requests.get(
                url,
                auth=(api_key, ""),
                params={"start_index": start_index, "items_per_page": items_per_page},
                headers=headers,
                timeout=10,
            )
            print(f" -> {resp.status_code} ({len(resp.content)} bytes)", end="", flush=True)
        except requests.Timeout:
            print(f" -> TIMEOUT", end="", flush=True)
            return officers, response_etag, True
        except requests.RequestException as e:
            print(f" -> ERROR: {e}", end="", flush=True)
            return officers, response_etag, True

        if resp.status_code == 304:
            return [], etag, False  # Not modified

        # Capture ETag from first response
        if start_index == 0:
            response_etag = resp.headers.get("ETag", "")

        if resp.status_code == 404:
            return [], None, True
        elif resp.status_code == 429:
            print(f"rate limited...", end="")
            time.sleep(60)
            continue
        elif resp.status_code != 200:
            return officers, response_etag, True

        data = resp.json()
        items = data.get("items", [])
        total = data.get("total_results", 0)
        officers.extend(items)
        print(f" [{len(officers)}/{total} officers]", end="", flush=True)

        start_index += len(items)
        if start_index >= total or not items:
            break

        time.sleep(RATE_LIMIT_DELAY)

    return officers, response_etag, True


def make_director_id(officer):
    """Create a stable director ID from officer links or name + DOB."""
    # Use the self link as a stable ID if available
    links = officer.get("links", {})
    self_link = links.get("self", "")
    if self_link:
        return self_link

    # Fallback: name + DOB
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

GET_FETCH_METADATA_QUERY = """
UNWIND $cns AS cn
MATCH (c:Company {companyNumber: cn})
RETURN cn, c.directorsFetchedAt AS fetchedAt, c.directorsEtag AS etag
"""

STALE_DAYS = 30

CREATE_INDEXES_QUERY = [
    "CREATE CONSTRAINT director_id IF NOT EXISTS FOR (d:Director) REQUIRE d.directorId IS UNIQUE",
    "CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.name)",
    "CREATE FULLTEXT INDEX director_name_fulltext IF NOT EXISTS FOR (d:Director) ON EACH [d.name]",
]


SINGLE_DIRECTOR_QUERY = """
MATCH (c:Company {companyNumber: $companyNumber})
MERGE (d:Director {directorId: $directorId})
SET d.name         = $name,
    d.nationality  = $nationality,
    d.occupation   = $occupation,
    d.countryOfResidence = $countryOfResidence
MERGE (d)-[r:OFFICER_OF]->(c)
SET r.role         = $role,
    r.appointedOn  = $appointedOn,
    r.resignedOn   = $resignedOn
"""


def load_officers_to_neo4j(driver, company_number, officers, etag=None):
    """Load officers into Neo4j one at a time and stamp fetch metadata."""
    loaded = 0
    for o in officers:
        params = {
            "companyNumber": company_number,
            "directorId": make_director_id(o),
            "name": o.get("name", ""),
            "nationality": o.get("nationality", ""),
            "occupation": o.get("occupation", ""),
            "countryOfResidence": o.get("country_of_residence", ""),
            "role": o.get("officer_role", ""),
            "appointedOn": o.get("appointed_on", ""),
            "resignedOn": o.get("resigned_on", ""),
        }
        with driver.session(database="neo4j") as session:
            session.run(SINGLE_DIRECTOR_QUERY, **params)
        loaded += 1

    with driver.session(database="neo4j") as session:
        session.run(STAMP_FETCH_QUERY, cn=company_number, etag=etag or "")

    return loaded


def get_fetch_metadata(driver, company_numbers):
    """Get fetch timestamps and ETags for a list of companies.
    Returns dict: cn -> {fetchedAt: datetime|None, etag: str|None}"""
    meta = {cn: {"fetchedAt": None, "etag": None} for cn in company_numbers}
    with driver.session() as session:
        result = session.run(GET_FETCH_METADATA_QUERY, cns=company_numbers)
        for r in result:
            cn = r["cn"]
            meta[cn] = {"fetchedAt": r["fetchedAt"], "etag": r["etag"]}
    return meta


def needs_refresh(fetched_at):
    """Return True if data is missing or older than STALE_DAYS."""
    if fetched_at is None:
        return True
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    age = now - fetched_at.to_native()
    return age.days >= STALE_DAYS


def get_tree_company_numbers(driver, company_number):
    """Get all company numbers in the ownership tree of a company."""
    query = """
    MATCH (c:Company {companyNumber: $cn})
    CALL apoc.path.expandConfig(c, {
      relationshipFilter: '<HAS_SIGNIFICANT_CONTROL, IS_COMPANY>',
      minLevel: 1, maxLevel: 30,
      uniqueness: 'NODE_GLOBAL'
    }) YIELD path
    UNWIND nodes(path) AS n
    WITH n WHERE n:Company
    RETURN DISTINCT n.companyNumber AS cn
    """
    with driver.session() as session:
        result = session.run(query, cn=company_number)
        numbers = [r["cn"] for r in result if r["cn"]]
    # Include the root company itself
    if company_number not in numbers:
        numbers.insert(0, company_number)
    return numbers


def main():
    parser = argparse.ArgumentParser(description="Fetch Companies House directors into Neo4j")
    parser.add_argument("--api-key", default=os.environ.get("CH_API_KEY", os.environ.get("CH_API", "")),
                        help="Companies House API key (or set CH_API_KEY env var)")
    parser.add_argument("--company", required=True, help="Company number")
    parser.add_argument("--follow-tree", action="store_true",
                        help="Also fetch directors for all companies in the ownership tree")
    parser.add_argument("--active-only", action="store_true",
                        help="Only load currently active officers (no resigned_on date)")
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    args = parser.parse_args()

    if not args.api_key:
        sys.exit("Error: No API key. Set CH_API_KEY env var or use --api-key")

    print(f"Connecting to Neo4j at {args.uri}...")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()

    # Create indexes
    with driver.session() as session:
        for q in CREATE_INDEXES_QUERY:
            session.run(q)

    # Get list of companies to fetch
    if args.follow_tree:
        print(f"Finding all companies in the ownership tree of {args.company}...")
        companies = get_tree_company_numbers(driver, args.company)
        print(f"  Found {len(companies)} companies in tree")
    else:
        companies = [args.company]

    total_officers = 0
    fetched = 0
    skipped = 0
    unchanged = 0

    # Get freshness metadata for all companies at once
    print("  Checking data freshness...", flush=True)
    meta = get_fetch_metadata(driver, companies)
    stale_count = sum(1 for cn in companies if needs_refresh(meta.get(cn, {}).get("fetchedAt")))
    print(f"  {stale_count} need fetching, {len(companies) - stale_count} are fresh", flush=True)

    for i, cn in enumerate(companies, 1):
        m = meta.get(cn, {"fetchedAt": None, "etag": None})
        stale = needs_refresh(m["fetchedAt"])

        if not stale:
            skipped += 1
            continue

        print(f"  [{i}/{len(companies)}] {cn}...", end=" ", flush=True)
        officers, new_etag, modified = fetch_officers(args.api_key, cn, etag=m["etag"])

        if not modified:
            # 304 Not Modified — just update the timestamp
            with driver.session() as session:
                session.run(STAMP_FETCH_QUERY, cn=cn, etag=m["etag"])
            unchanged += 1
            print("unchanged (304)", flush=True)
            continue

        if args.active_only:
            officers = [o for o in officers if not o.get("resigned_on")]

        loaded = load_officers_to_neo4j(driver, cn, officers, etag=new_etag)
        total_officers += loaded
        fetched += 1
        print(f"-> {loaded} officers loaded", flush=True)
        time.sleep(RATE_LIMIT_DELAY)

    driver.close()
    print(f"\nDone! {fetched} fetched, {unchanged} unchanged, {skipped} fresh (< {STALE_DAYS} days old)")
    print(f"Total officers loaded: {total_officers}")


if __name__ == "__main__":
    main()
