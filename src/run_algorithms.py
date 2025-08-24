# src/run_algorithms.py
# Recreates the GDS projections, runs the algorithms, applies the risk rules,
# and exports a CSV of suspicious/high-risk users for reporting.

from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pandas as pd

# -------------------------
# Setup: paths + .env
# -------------------------
# this file lives in src/, project root is one folder up
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD", "neo4j")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

driver = GraphDatabase.driver(URI, auth=(USER, PWD))

# -------------------------
# Cypher helpers
# -------------------------
def run(session, q, msg=None):
    """Run a Cypher query with an optional log message."""
    if msg:
        print(msg)
    return session.run(q)

# -------------------------
# Main pipeline
# -------------------------
if __name__ == "__main__":
    with driver.session(database=DB) as s:
        # 0) Start from a clean slate (ok if the graphs don't exist)
        run(s, "CALL gds.graph.drop('momo-dir',   false)", "Dropping momo-dir (if any) ...")
        run(s, "CALL gds.graph.drop('momo-undir', false)", "Dropping momo-undir (if any) ...")

        # 1) Projections: undirected for degree/triangles/Louvain; directed for PageRank
        run(s, """
        CALL gds.graph.project(
          'momo-undir',
          'User',
          { TRANSACTS_WITH: { type:'TRANSACTS_WITH', orientation:'UNDIRECTED', aggregation:'SUM', properties:'amount' } }
        )
        """, "Projecting momo-undir (UNDIRECTED) ...")

        run(s, """
        CALL gds.graph.project(
          'momo-dir',
          'User',
          { TRANSACTS_WITH: { type:'TRANSACTS_WITH', orientation:'NATURAL', properties:'amount' } }
        )
        """, "Projecting momo-dir (DIRECTED) ...")

        # 2) Algorithms (match Step 5 you ran manually)
        run(s, "CALL gds.degree.write('momo-undir', {writeProperty:'degree'})",
              "Writing degree (undirected) ...")

        run(s, "CALL gds.triangleCount.write('momo-undir', {writeProperty:'triangles'})",
              "Writing triangleCount (undirected) ...")

        run(s, "CALL gds.louvain.write('momo-undir', {writeProperty:'community'})",
              "Writing louvain (undirected) ...")

        run(s, "CALL gds.pageRank.write('momo-dir', {writeProperty:'pr', maxIterations:20, dampingFactor:0.85})",
              "Writing pageRank (directed) ...")

        # 3) Extra feature used by our rules: device co-users
        run(s, """
        MATCH (u:User)-[:USES_DEVICE]->(d:Device)<-[:USES_DEVICE]-(v:User)
        WHERE u <> v
        WITH u, count(DISTINCT v) AS coUsers
        SET u.coUsers = coUsers
        """, "Computing device co-users ...")

        # 4) Rules: suspicious + highRisk (transparent, thesis-ready)
        run(s, """
        MATCH (u:User)
        WITH avg(u.pr) AS meanPR, stDev(u.pr) AS sdPR
        MATCH (u:User)
        SET u.suspicious =
          (u.pr > meanPR + 3*sdPR) OR
          (coalesce(u.coUsers,0) >= 2) OR
          (coalesce(u.triangles,0) >= 2)
        """, "Setting u.suspicious ...")

        run(s, """
        MATCH (u:User)
        WITH avg(u.pr) AS meanPR, stDev(u.pr) AS sdPR
        MATCH (u:User)
        WHERE u.suspicious = true
          AND (coalesce(u.coUsers,0) >= 4 OR u.pr > meanPR + 2*sdPR)
        SET u.highRisk = true
        """, "Setting u.highRisk ...")

        # 5) Export: collect the top flagged users for reporting
        print("Exporting risk table ...")
        rows = s.run("""
        MATCH (u:User)
        WHERE u.suspicious = true OR u.highRisk = true
        RETURN
          u.id                                  AS user,
          coalesce(u.highRisk,false)            AS highRisk,
          coalesce(u.coUsers,0)                 AS coUsers,
          round(coalesce(u.pr,0.0),6)           AS pr,
          coalesce(u.degree,0)                  AS degree,
          coalesce(u.triangles,0)               AS triangles,
          coalesce(u.community,-1)              AS community
        ORDER BY
          highRisk DESC,
          coUsers DESC,
          pr DESC
        LIMIT 1000
        """).data()

    driver.close()

    # 6) Write CSV
    out = ROOT / "data" / "processed" / "risk_users.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Saved: {out}")
