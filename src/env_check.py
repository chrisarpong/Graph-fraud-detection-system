# src/env_check.py
from pathlib import Path
import os, sys
from neo4j import GraphDatabase
from dotenv import load_dotenv

# go up one level from src/ to project root where .env lives
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
pwd  = os.getenv("NEO4J_PASSWORD")
db   = os.getenv("NEO4J_DATABASE", "neo4j")

print("Python:", sys.version)
print("Loaded from .env ->", {"NEO4J_URI": uri, "NEO4J_DATABASE": db})

if not uri or not user or not pwd:
    raise SystemExit("Missing NEO4J_* env vars. Check your .env is at project root and filled in.")

driver = GraphDatabase.driver(uri, auth=(user, pwd))
with driver.session(database=db) as s:
    ok = s.run("RETURN 1 AS ok").single()["ok"]
    print("Neo4j connection ok:", ok == 1)

# Optional: check GDS
# Optional: check GDS
try:
    with driver.session(database=db) as s:
        ver = s.run("RETURN gds.version() AS version").single()["version"]
        print("GDS version:", ver)
except Exception as e:
    print("GDS check failed (plugin not installed or DB not started):", e)

