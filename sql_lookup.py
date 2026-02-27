"""
SQL Server lookup for Bluefly category-specific field mappings.

Queries utb_BFCategories and utb_BFMapping to get the Bluefly field names
and values for a given Shopify category + variant size.
"""

import pyodbc


class BlueflyDBLookup:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
    ):
        self.conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        self._conn = None

    def connect(self):
        """Open a connection to SQL Server."""
        self._conn = pyodbc.connect(self.conn_str, timeout=10)
        print(f"  SQL Server connected: {self._conn.getinfo(pyodbc.SQL_SERVER_NAME)}")

    def close(self):
        """Close the SQL Server connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def lookup_category_fields(
        self, category_id: str, variant_title: str
    ) -> dict[str, str]:
        """
        Look up Bluefly field mappings for a category + variant size.

        Executes:
            SELECT m.FieldName, m.BFValue
            FROM utb_BFCategories c
            INNER JOIN utb_BFMapping m ON c.FieldName = m.FieldName
            WHERE c.CategoryID = ?
            AND m.SHValue = ?

        Args:
            category_id: Bluefly category ID from metafield (e.g. "634")
            variant_title: Shopify variant title / size value (e.g. "9", "41")

        Returns:
            Dict of {FieldName: BFValue} for the matching category + size.
            Empty dict if no matches found.
        """
        if not self._conn:
            raise RuntimeError("Not connected. Use 'with BlueflyDBLookup(...) as db:'")

        cursor = self._conn.cursor()
        try:
            cursor.execute(
                """
                SELECT m.FieldName, m.BFValue
                FROM utb_BFCategories c
                INNER JOIN utb_BFMapping m ON c.FieldName = m.FieldName
                WHERE c.CategoryID = ?
                AND m.SHValue = ?
                """,
                (category_id, variant_title),
            )
            rows = cursor.fetchall()
            results = {}
            for row in rows:
                results[row.FieldName] = row.BFValue
            print(f"    [SQL] cat={category_id} variant='{variant_title}' → {len(rows)} rows")
            return results
        except pyodbc.ProgrammingError as e:
            # Permission denied or table not found -- non-fatal
            print(f"    SQL lookup warning: {e}")
            return {}
        finally:
            cursor.close()

    def test_connection(self) -> bool:
        """Quick connectivity check."""
        if not self._conn:
            return False
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False
