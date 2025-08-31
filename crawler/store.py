import os
import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://crawler:secret@localhost:5432/crawlerdb"
)

_seen_ids = set()


def get_conn():
    return psycopg.connect(DATABASE_URL)


def insert_repositories(repos):
    """Insert repositories into Postgres using executemany."""
    if not repos:
        return 0

    rows = []
    for r in repos:
        rid = r["id"]
        if rid not in _seen_ids:
            _seen_ids.add(rid)
            rows.append((
                rid,
                r["name"],
                r.get("description"),
                r.get("stargazerCount", 0),
                r["url"],
            ))

    if not rows:
        return 0

    inserted = len(rows)

    with get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO repositories (id, name, description, stars, url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()
            # removed per-shard print, let ingest.py handle logging
            return inserted
        except Exception as e:
            conn.rollback()
            print(f"❌ Insert failed: {e}")
            return 0

