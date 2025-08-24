# quick_graph_check.py
# Build a small transaction graph in memory and compute PageRank + Louvain

from pathlib import Path
import pandas as pd
import networkx as nx
from community import community_louvain  # Louvain

ROOT = Path(__file__).resolve().parents
CSV = ROOT / "data" / "raw" / "momo_transactions.csv"

def main():
    print(f"Loading: {CSV}")
    df = pd.read_csv(CSV)

    # keep only P2P-like transfers (optional; PaySim 'type' was dropped; we just use all rows)
    # If file is big, take a small slice for speed
    if len(df) > 30000:
        df = df.sample(n=30000, random_state=42)

    # Build a directed multigraph of sender -> receiver, weighted by count
    G = nx.DiGraph()
    for row in df.itertuples(index=False):
        s = row.sender_id
        r = row.receiver_id
        amt = float(row.amount)
        # add or update edge weight
        if G.has_edge(s, r):
            G[s][r]["count"] += 1
            G[s][r]["amount_sum"] += amt
        else:
            G.add_edge(s, r, count=1, amount_sum=amt)

    print(f"Nodes: {G.number_of_nodes():,}  Edges: {G.number_of_edges():,}")

    # PageRank (who is unusually "important" as receiver/sender)
    pr = nx.pagerank(G, alpha=0.85, max_iter=50)
    top_pr = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 PageRank accounts:")
    for acc, score in top_pr:
        print(f"{acc:>14}  {score:.6f}")

    # Louvain needs an undirected graph (use edge count as weight)
    UG = nx.Graph()
    for u, v, d in G.edges(data=True):
        w = d.get("count", 1)
        if UG.has_edge(u, v):
            UG[u][v]["weight"] += w
        else:
            UG.add_edge(u, v, weight=w)

    parts = community_louvain.best_partition(UG, weight="weight", random_state=42)
    # count community sizes
    from collections import Counter
    sizes = Counter(parts.values())
    biggest = sizes.most_common(5)
    print("\nLargest communities (id -> size):")
    for cid, sz in biggest:
        print(f"{cid} -> {sz}")

    print("\nSanity check done. You can cite PageRank & Louvain outputs in your defense.")

if __name__ == "__main__":
    main()
