"""
One-time migration: copy bf_categories and bf_mapping from SQL Server to D1.

Run while connected to the SQL Server network:
    python migrate_sql_to_d1.py
"""

import os
from dotenv import load_dotenv
import pyodbc

from d1_client import get_d1_client

load_dotenv()

SQL_SERVER = os.environ.get("SQL_SERVER", "")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "")
SQL_USER = os.environ.get("SQL_USER", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")


def main():
    # Connect to SQL Server
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    print("Connecting to SQL Server...")
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()

    d1 = get_d1_client()

    # --- Migrate bf_categories ---
    print("\nReading utb_BFCategories...")
    cursor.execute("SELECT CategoryID, CategoryPath, FieldName FROM utb_BFCategories WHERE CategoryID != ''")
    cat_rows = cursor.fetchall()
    print(f"  Found {len(cat_rows)} rows with CategoryID")

    if cat_rows:
        for i, row in enumerate(cat_rows):
            d1.execute(
                "INSERT OR REPLACE INTO bf_categories (category_id, category_path, field_name) VALUES (?, ?, ?)",
                [str(row.CategoryID), row.CategoryPath, row.FieldName],
            )
            if (i + 1) % 50 == 0:
                print(f"    {i + 1}/{len(cat_rows)} inserted...")
        print(f"  Inserted {len(cat_rows)} rows into D1 bf_categories")

    # --- Migrate bf_mapping ---
    print("\nReading utb_BFMapping...")
    cursor.execute("SELECT FieldName, SHValue, BFValue FROM utb_BFMapping")
    map_rows = cursor.fetchall()
    print(f"  Found {len(map_rows)} rows")

    if map_rows:
        for i, row in enumerate(map_rows):
            d1.execute(
                "INSERT OR REPLACE INTO bf_mapping (field_name, sh_value, bf_value) VALUES (?, ?, ?)",
                [str(row.FieldName), str(row.SHValue), str(row.BFValue)],
            )
            if (i + 1) % 50 == 0:
                print(f"    {i + 1}/{len(map_rows)} inserted...")
        print(f"  Inserted {len(map_rows)} rows into D1 bf_mapping")

    # --- Verify ---
    print("\nVerifying D1 data...")
    d1_cats = d1.execute("SELECT COUNT(*) as cnt FROM bf_categories")
    d1_maps = d1.execute("SELECT COUNT(*) as cnt FROM bf_mapping")
    print(f"  D1 bf_categories: {d1_cats[0]['cnt']} rows")
    print(f"  D1 bf_mapping:    {d1_maps[0]['cnt']} rows")

    cursor.close()
    conn.close()
    print("\nDone! SQL Server data migrated to D1.")


if __name__ == "__main__":
    main()
