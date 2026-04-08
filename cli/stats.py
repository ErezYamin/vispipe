#!/usr/bin/env python3
"""
Pipeline health dashboard.
Usage: python cli/stats.py
Requires DATABASE_URL env var or uses default localhost connection.
"""
import os
import sys

import psycopg2
from tabulate import tabulate

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://vispipe:vispipe@localhost:5432/vispipe",
)


def fetch_stats(db) -> list:
    with db.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM images GROUP BY status ORDER BY status")
        return cur.fetchall()


def fetch_recent(db, limit: int = 10) -> list:
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT image_id, status, width, height, file_size_bytes, processed_at
            FROM images
            ORDER BY COALESCE(processed_at, created_at) DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def main() -> None:
    try:
        db = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Could not connect to database: {e}")
        sys.exit(1)

    try:
        print("\n=== VisPipe Pipeline Stats ===\n")

        stats = fetch_stats(db)
        if not stats:
            print("No images in the pipeline yet.")
        else:
            print(tabulate(stats, headers=["Status", "Count"], tablefmt="rounded_outline"))

        print("\n--- Recent Images ---\n")
        recent = fetch_recent(db)
        if not recent:
            print("No images processed yet.")
        else:
            print(tabulate(
                recent,
                headers=["Image ID", "Status", "Width", "Height", "Size (bytes)", "Processed At"],
                tablefmt="rounded_outline",
            ))
    finally:
        db.close()


if __name__ == "__main__":
    main()
