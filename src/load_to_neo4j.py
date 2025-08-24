# src/load_to_neo4j.py
# Load the preprocessed CSV into Neo4j.
# - creates constraints (unique id) for all node types we use
# - batch MERGEs Users, Merchants, Devices, Locations, Phones, Emails
# - creates relationships (TRANSACTS_WITH, USES_DEVICE, LOCATED_IN, HAS_PHONE, HAS_EMAIL)
# - writes tx properties: amount, timestamp, label
from pathlib import Path
import os, time
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Paths & .env (this file lives in src/)
ROOT = Path(__file__).resolve().parents[1]     # go to project root
CSV  = ROOT / "data" / "raw" / "sampled_momo_transactions.csv"

load_dotenv(ROOT / ".env")
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD", "neo4j")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

# small batches keep the UI responsive; you can increase if ingestion is slow
BATCH_SIZE = 500

# Constraint setup (id uniqueness per label)
def setup_constraints(tx):
    # one id per node; this prevents duplicates when we MERGE
    tx.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User)     REQUIRE u.id IS UNIQUE")
    tx.run("CREATE CONSTRAINT merch_id IF NOT EXISTS FOR (m:Merchant) REQUIRE m.id IS UNIQUE")
    tx.run("CREATE CONSTRAINT device_id IF NOT EXISTS FOR (d:Device)  REQUIRE d.id IS UNIQUE")
    tx.run("CREATE CONSTRAINT loc_id IF NOT EXISTS FOR (l:Location)   REQUIRE l.id IS UNIQUE")
    tx.run("CREATE CONSTRAINT phone_id IF NOT EXISTS FOR (p:Phone)    REQUIRE p.id IS UNIQUE")
    tx.run("CREATE CONSTRAINT email_id IF NOT EXISTS FOR (e:Email)    REQUIRE e.id IS UNIQUE")

def await_indexes(tx):
    # wait for constraints to be online before ingesting
    tx.run("CALL db.awaitIndexes()")

# One batch insert (UNWIND)
def load_batch(tx, rows):
    # Note: we MERGE a receiver node without a fixed label first,
    # then flip between :User and :Merchant based on r.receiver_type.
    tx.run(
        """
        UNWIND $rows AS r

        // sender
        MERGE (s:User {id: r.sender_id})

        // receiver can be a User or a Merchant (we switch labels once)
        MERGE (t {id: r.receiver_id})
          ON CREATE SET t:User
        FOREACH (_ IN CASE WHEN r.receiver_type = 'merchant' THEN [1] ELSE [] END |
            SET t:Merchant REMOVE t:User
        )

        // device + edge
        MERGE (d:Device {id: r.sender_device_id})
        MERGE (s)-[:USES_DEVICE]->(d)

        // location + edge
        MERGE (l:Location {id: r.sender_location})
        MERGE (s)-[:LOCATED_IN]->(l)

        // phone + edge
        MERGE (p:Phone {id: r.sender_phone})
        MERGE (s)-[:HAS_PHONE]->(p)

        // email + edge
        MERGE (e:Email {id: r.sender_email})
        MERGE (s)-[:HAS_EMAIL]->(e)

        // transaction (use txid to keep a single edge per transaction)
        MERGE (s)-[x:TRANSACTS_WITH {txid: r.transaction_id}]->(t)
          ON CREATE SET
            x.amount    = toFloat(r.amount),
            x.timestamp = r.timestamp,
            x.label     = toInteger(r.label)
        """,
        rows=rows,
    )

# Main
def main():
    t0 = time.time()
    if not CSV.exists():
        raise FileNotFoundError(f"Missing CSV: {CSV}\nRun src/preprocess_data.py first.")

    # read as strings where appropriate to avoid NaN/None surprises
    print(f"Reading {CSV} ...")
    df = pd.read_csv(CSV, dtype={
        "transaction_id": "string",
        "sender_id": "string",
        "receiver_id": "string",
        "receiver_type": "string",
        "sender_device_id": "string",
        "sender_location": "string",
        "sender_phone": "string",
        "sender_email": "string",
        "timestamp": "string",
    })
    # keep only rows with sender/receiver
    df = df.dropna(subset=["sender_id", "receiver_id"])
    rows = df.to_dict("records")
    print(f"Loading {len(rows):,} rows into Neo4j (batch={BATCH_SIZE})")

    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session(database=DB) as s:
        s.execute_write(setup_constraints)
        s.execute_read(await_indexes)

        total = len(rows)
        batches = (total // BATCH_SIZE) + (1 if total % BATCH_SIZE else 0)
        for i in range(0, total, BATCH_SIZE):
            bno = (i // BATCH_SIZE) + 1
            print(f"Batch {bno}/{batches} ...")
            s.execute_write(load_batch, rows[i:i+BATCH_SIZE])

    driver.close()
    print(f"Done. Elapsed: {time.time() - t0:,.1f}s")

if __name__ == "__main__":
    main()
