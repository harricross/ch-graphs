#!/usr/bin/env python3
"""
Companies House Data Loader for Neo4j — Bulk Import Edition

Two-phase approach for maximum speed:
  Phase 1 (this script): Stream-converts raw CH data into neo4j-admin import CSVs
  Phase 2 (run after):   neo4j-admin database import full (see instructions at end)

This avoids the Bolt transaction overhead entirely and loads millions of rows
in minutes rather than hours.

Usage:
  # Phase 1: Generate import CSVs
  python load_data.py

  # Phase 2: Run the import (printed at end of Phase 1)
"""

import csv
import glob
import json
import os
import sys
import time


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _find_file(pattern, label):
    matches = sorted(glob.glob(pattern))
    if matches:
        return matches[-1]  # most recent
    return None


COMPANY_CSV = _find_file("data/BasicCompanyDataAsOneFile-*.csv", "Company CSV")
PSC_JSONL = _find_file("data/persons-with-significant-control-snapshot-*.txt", "PSC JSONL")
OUTPUT_DIR = "data/import"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_sic(text):
    """Parse '99999 - Dormant Company' into (code, description)."""
    if not text or text.strip() == "" or text.strip() == "None Supplied":
        return None
    text = text.strip()
    parts = text.split(" - ", 1)
    if len(parts) == 2:
        return (parts[0].strip(), parts[1].strip())
    return (text, text)


def make_address_id(postcode, line1):
    pc = (postcode or "").strip().upper().replace(" ", "")
    l1 = (line1 or "").strip().upper()
    return f"{pc}|{l1}" if pc else ""


def make_person_id(data):
    ne = data.get("name_elements", {})
    dob = data.get("date_of_birth", {})
    parts = [
        ne.get("forename", ""),
        ne.get("middle_name", ""),
        ne.get("surname", ""),
        str(dob.get("month", "")),
        str(dob.get("year", "")),
    ]
    return "|".join(p.strip().upper() for p in parts)


def pad_company_number(num):
    """Zero-pad purely numeric company numbers to 8 digits."""
    num = num.strip()
    if num.isdigit():
        return num.zfill(8)
    return num.upper()


def make_entity_id(data):
    ident = data.get("identification", {})
    reg = ident.get("registration_number", "")
    name = data.get("name", "")
    if reg:
        return f"REG|{pad_company_number(reg)}"
    return f"NAME|{name.strip().upper()}"


# ---------------------------------------------------------------------------
# Phase 1a: Process Company CSV
# ---------------------------------------------------------------------------
def process_companies(company_csv, out_dir):
    print(f"\n=== Processing companies from {company_csv} ===")
    start = time.time()

    seen_addresses = set()
    seen_sic = {}  # code -> description

    company_f = open(os.path.join(out_dir, "companies.csv"), "w", newline="", encoding="utf-8")
    address_f = open(os.path.join(out_dir, "addresses.csv"), "w", newline="", encoding="utf-8")
    reg_at_f = open(os.path.join(out_dir, "rel_registered_at.csv"), "w", newline="", encoding="utf-8")
    has_sic_f = open(os.path.join(out_dir, "rel_has_sic.csv"), "w", newline="", encoding="utf-8")

    # Headers — neo4j-admin import format
    company_w = csv.writer(company_f)
    company_w.writerow([
        "companyNumber:ID(Company)", "name", "category", "status",
        "countryOfOrigin", "incorporationDate", "dissolutionDate", "uri",
        "careOf", "poBox", "postcode", "addressLine1", "addressLine2", "postTown", "county", "country",
        "accountCategory", "accountRefDay:int", "accountRefMonth:int",
        "accountsNextDueDate", "accountsLastMadeUpDate",
        "returnsNextDueDate", "returnsLastMadeUpDate",
        "numMortCharges:int", "numMortOutstanding:int",
        "numMortPartSatisfied:int", "numMortSatisfied:int",
        "numGenPartners:int", "numLimPartners:int",
        "confStmtNextDueDate", "confStmtLastMadeUpDate",
        "previousNames:string[]",
    ])

    address_w = csv.writer(address_f)
    address_w.writerow([
        "addressId:ID(Address)", "postcode", "addressLine1", "postTown", "county", "country",
    ])

    reg_at_w = csv.writer(reg_at_f)
    reg_at_w.writerow([":START_ID(Company)", ":END_ID(Address)"])

    has_sic_w = csv.writer(has_sic_f)
    has_sic_w.writerow([":START_ID(Company)", ":END_ID(SICCode)"])

    total = 0
    with open(company_csv, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {k.strip(): (v.strip() if v else "") for k, v in raw.items()}
            cn = row.get("CompanyNumber", "").strip()
            if not cn:
                continue

            postcode = row.get("RegAddress.PostCode", "")
            line1 = row.get("RegAddress.AddressLine1", "")
            post_town = row.get("RegAddress.PostTown", "")
            county = row.get("RegAddress.County", "")
            country = row.get("RegAddress.Country", "")

            # SIC codes
            for i in range(1, 5):
                parsed = parse_sic(row.get(f"SICCode.SicText_{i}", ""))
                if parsed:
                    code, desc = parsed
                    seen_sic[code] = desc
                    has_sic_w.writerow([cn, code])

            # Previous names (semicolon-delimited array)
            prev_names = []
            for i in range(1, 11):
                pname = row.get(f"PreviousName_{i}.CompanyName", "").strip()
                if pname:
                    prev_names.append(pname)


            company_w.writerow([
                cn,
                row.get("CompanyName", ""),
                row.get("CompanyCategory", ""),
                row.get("CompanyStatus", ""),
                row.get("CountryOfOrigin", ""),
                row.get("IncorporationDate", ""),
                row.get("DissolutionDate", ""),
                row.get("URI", ""),
                row.get("RegAddress.CareOf", ""),
                row.get("RegAddress.POBox", ""),
                postcode, line1,
                row.get("RegAddress.AddressLine2", ""),
                post_town, county, country,
                row.get("Accounts.AccountCategory", ""),
                row.get("Accounts.AccountRefDay", "") or "",
                row.get("Accounts.AccountRefMonth", "") or "",
                row.get("Accounts.NextDueDate", ""),
                row.get("Accounts.LastMadeUpDate", ""),
                row.get("Returns.NextDueDate", ""),
                row.get("Returns.LastMadeUpDate", ""),
                row.get("Mortgages.NumMortCharges", "") or "",
                row.get("Mortgages.NumMortOutstanding", "") or "",
                row.get("Mortgages.NumMortPartSatisfied", "") or "",
                row.get("Mortgages.NumMortSatisfied", "") or "",
                row.get("LimitedPartnerships.NumGenPartners", "") or "",
                row.get("LimitedPartnerships.NumLimPartners", "") or "",
                row.get("ConfStmtNextDueDate", ""),
                row.get("ConfStmtLastMadeUpDate", ""),
                ";".join(prev_names),
            ])

            # Address node + relationship
            addr_id = make_address_id(postcode, line1)
            if addr_id and addr_id not in seen_addresses:
                seen_addresses.add(addr_id)
                address_w.writerow([addr_id, postcode, line1, post_town, county, country])
            if addr_id:
                reg_at_w.writerow([cn, addr_id])

            total += 1
            if total % 200000 == 0:
                elapsed = time.time() - start
                print(f"  Companies: {total:,} ({total/elapsed:,.0f}/sec)", end="\r")

    company_f.close()
    address_f.close()
    reg_at_f.close()
    has_sic_f.close()

    # Write SIC code nodes
    sic_f = open(os.path.join(out_dir, "sic_codes.csv"), "w", newline="", encoding="utf-8")
    sic_w = csv.writer(sic_f)
    sic_w.writerow(["code:ID(SICCode)", "description"])
    for code, desc in sorted(seen_sic.items()):
        sic_w.writerow([code, desc])
    sic_f.close()

    elapsed = time.time() - start
    print(f"\n  Done: {total:,} companies, {len(seen_addresses):,} addresses, {len(seen_sic):,} SIC codes in {elapsed:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Phase 1b: Process PSC JSON Lines
# ---------------------------------------------------------------------------
def process_psc(psc_jsonl, out_dir):
    print(f"\n=== Processing PSC data from {psc_jsonl} ===")
    start = time.time()

    seen_persons = set()
    seen_corps = set()
    seen_legals = set()
    seen_is_company = set()

    person_f = open(os.path.join(out_dir, "persons.csv"), "w", newline="", encoding="utf-8")
    corp_f = open(os.path.join(out_dir, "corporate_entities.csv"), "w", newline="", encoding="utf-8")
    legal_f = open(os.path.join(out_dir, "legal_persons.csv"), "w", newline="", encoding="utf-8")
    rel_ind_f = open(os.path.join(out_dir, "rel_psc_individual.csv"), "w", newline="", encoding="utf-8")
    rel_corp_f = open(os.path.join(out_dir, "rel_psc_corporate.csv"), "w", newline="", encoding="utf-8")
    rel_legal_f = open(os.path.join(out_dir, "rel_psc_legal.csv"), "w", newline="", encoding="utf-8")
    rel_is_co_f = open(os.path.join(out_dir, "rel_is_company.csv"), "w", newline="", encoding="utf-8")

    person_w = csv.writer(person_f)
    person_w.writerow([
        "personId:ID(Person)", "name", "title", "forename", "middleName", "surname",
        "nationality", "countryOfResidence", "dobMonth:int", "dobYear:int",
        "addressPremises", "addressLine1", "addressLine2",
        "addressLocality", "addressRegion", "addressCountry", "addressPostalCode",
    ])

    corp_w = csv.writer(corp_f)
    corp_w.writerow([
        "entityId:ID(CorporateEntity)", "name", "registrationNumber",
        "legalForm", "legalAuthority", "countryRegistered", "placeRegistered",
        "addressPremises", "addressLine1", "addressLine2",
        "addressLocality", "addressRegion", "addressCountry", "addressPostalCode",
    ])

    legal_w = csv.writer(legal_f)
    legal_w.writerow([
        "entityId:ID(LegalPerson)", "name", "legalForm", "legalAuthority",
        "addressPremises", "addressLine1", "addressLine2",
        "addressLocality", "addressRegion", "addressCountry", "addressPostalCode",
    ])

    rel_ind_w = csv.writer(rel_ind_f)
    rel_ind_w.writerow([":START_ID(Person)", ":END_ID(Company)", "naturesOfControl:string[]", "notifiedOn", "ceasedOn"])

    rel_corp_w = csv.writer(rel_corp_f)
    rel_corp_w.writerow([":START_ID(CorporateEntity)", ":END_ID(Company)", "naturesOfControl:string[]", "notifiedOn", "ceasedOn"])

    rel_legal_w = csv.writer(rel_legal_f)
    rel_legal_w.writerow([":START_ID(LegalPerson)", ":END_ID(Company)", "naturesOfControl:string[]", "notifiedOn", "ceasedOn"])

    rel_is_co_w = csv.writer(rel_is_co_f)
    rel_is_co_w.writerow([":START_ID(CorporateEntity)", ":END_ID(Company)"])

    total = 0
    skipped = 0

    with open(psc_jsonl, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            company_number = record.get("company_number", "")
            data = record.get("data", {})
            kind = data.get("kind", "")
            noc = ";".join(x for x in data.get("natures_of_control", []) if x)
            notified = data.get("notified_on", "")
            ceased = data.get("ceased_on", "")
            addr = data.get("address", {})
            addr_fields = [
                addr.get("premises", ""),
                addr.get("address_line_1", ""),
                addr.get("address_line_2", ""),
                addr.get("locality", ""),
                addr.get("region", ""),
                addr.get("country", ""),
                addr.get("postal_code", ""),
            ]

            if kind == "individual-person-with-significant-control":
                pid = make_person_id(data)
                ne = data.get("name_elements", {})
                dob = data.get("date_of_birth", {})

                if pid not in seen_persons:
                    seen_persons.add(pid)
                    person_w.writerow([
                        pid,
                        data.get("name", ""),
                        ne.get("title", ""),
                        ne.get("forename", ""),
                        ne.get("middle_name", ""),
                        ne.get("surname", ""),
                        data.get("nationality", ""),
                        data.get("country_of_residence", ""),
                        dob.get("month", ""),
                        dob.get("year", ""),
                    ] + addr_fields)

                rel_ind_w.writerow([pid, company_number, noc, notified, ceased])

            elif kind == "corporate-entity-person-with-significant-control":
                eid = make_entity_id(data)
                ident = data.get("identification", {})
                reg_num = ident.get("registration_number", "")

                if eid not in seen_corps:
                    seen_corps.add(eid)
                    corp_w.writerow([
                        eid,
                        data.get("name", ""),
                        reg_num,
                        ident.get("legal_form", ""),
                        ident.get("legal_authority", ""),
                        ident.get("country_registered", ""),
                        ident.get("place_registered", ""),
                    ] + addr_fields)

                rel_corp_w.writerow([eid, company_number, noc, notified, ceased])

                # Cross-reference: corporate PSC -> Company node (deduplicated)
                padded_reg = pad_company_number(reg_num) if reg_num else ""
                if padded_reg and eid not in seen_is_company:
                    seen_is_company.add(eid)
                    rel_is_co_w.writerow([eid, padded_reg])

            elif kind == "legal-person-person-with-significant-control":
                eid = make_entity_id(data)
                ident = data.get("identification", {})

                if eid not in seen_legals:
                    seen_legals.add(eid)
                    legal_w.writerow([
                        eid,
                        data.get("name", ""),
                        ident.get("legal_form", ""),
                        ident.get("legal_authority", ""),
                    ] + addr_fields)

                rel_legal_w.writerow([eid, company_number, noc, notified, ceased])

            else:
                skipped += 1
                continue

            total += 1
            if total % 500000 == 0:
                elapsed = time.time() - start
                print(f"  PSC: {total:,} ({total/elapsed:,.0f}/sec)", end="\r")

    for fh in [person_f, corp_f, legal_f, rel_ind_f, rel_corp_f, rel_legal_f, rel_is_co_f]:
        fh.close()

    elapsed = time.time() - start
    print(f"\n  Done: {total:,} PSC records in {elapsed:.1f}s")
    print(f"  Persons: {len(seen_persons):,}, Corporate: {len(seen_corps):,}, Legal: {len(seen_legals):,}")
    print(f"  Skipped: {skipped:,}")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not COMPANY_CSV:
        sys.exit("Error: No data/BasicCompanyDataAsOneFile-*.csv found. Place the Companies House CSV in data/")
    if not PSC_JSONL:
        sys.exit("Error: No data/persons-with-significant-control-snapshot-*.txt found. Place the PSC file in data/")

    print("=" * 60)
    print("Companies House -> Neo4j Bulk Import CSV Generator")
    print("=" * 60)
    print(f"  Company CSV: {COMPANY_CSV}")
    print(f"  PSC JSONL:   {PSC_JSONL}")

    from concurrent.futures import ProcessPoolExecutor, as_completed
    with ProcessPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(process_companies, COMPANY_CSV, OUTPUT_DIR): "Companies",
            pool.submit(process_psc, PSC_JSONL, OUTPUT_DIR): "PSC",
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"\nError in {name}: {e}")
                sys.exit(1)

    # Print the neo4j-admin import command
    print("\n" + "=" * 60)
    print("CSV generation complete! Now run the import:")
    print("=" * 60)
    print("""
# 1. Stop neo4j (from your project directory):
docker compose down

# 2. Run the bulk import inside a temporary container:
docker run --rm \\
  -v $(pwd)/data/import:/import \\
  -v chsearch_neo4j_data:/data \\
  neo4j:5-community \\
  neo4j-admin database import full \\
    --overwrite-destination \\
    --nodes=Company=/import/companies.csv \\
    --nodes=Address=/import/addresses.csv \\
    --nodes=SICCode=/import/sic_codes.csv \\
    --nodes=Person=/import/persons.csv \\
    --nodes=CorporateEntity=/import/corporate_entities.csv \\
    --nodes=LegalPerson=/import/legal_persons.csv \\
    --relationships=REGISTERED_AT=/import/rel_registered_at.csv \\
    --relationships=HAS_SIC=/import/rel_has_sic.csv \\
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_individual.csv \\
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_corporate.csv \\
    --relationships=HAS_SIGNIFICANT_CONTROL=/import/rel_psc_legal.csv \\
    --relationships=IS_COMPANY=/import/rel_is_company.csv \\
    --skip-bad-relationships \\
    --skip-duplicate-nodes \\
    --bad-tolerance=10000000 \\
    --array-delimiter=";" \\
    --trim-strings=true \\
    neo4j

# 3. Start neo4j back up:
docker compose up -d

# 4. Create indexes (run once in the Neo4j browser at http://localhost:7474):
#    CREATE INDEX company_name FOR (c:Company) ON (c.name);
#    CREATE INDEX company_status FOR (c:Company) ON (c.status);
#    CREATE INDEX company_postcode FOR (c:Company) ON (c.postcode);
#    CREATE INDEX person_name FOR (p:Person) ON (p.name);
#    CREATE INDEX person_surname FOR (p:Person) ON (p.surname);
#    CREATE INDEX corp_entity_name FOR (ce:CorporateEntity) ON (ce.name);
#    CREATE INDEX corp_entity_reg FOR (ce:CorporateEntity) ON (ce.registrationNumber);
#    CREATE INDEX address_postcode FOR (a:Address) ON (a.postcode);
""")


if __name__ == "__main__":
    main()
