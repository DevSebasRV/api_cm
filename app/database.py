import pyodbc
from app.config import SAP_SERVER, SAP_USER, SAP_PASSWORD


def get_connection(database: str):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SAP_SERVER};"
        f"DATABASE={database};"
        f"UID={SAP_USER};"
        f"PWD={SAP_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_warehouse_stock(cursor, item_code: str) -> list:
    cursor.execute(
        """
        SELECT   OITW.WhsCode,
                 OWHS.WhsName,
                 OITW.OnHand
        FROM     OITW
        JOIN     OWHS ON OWHS.WhsCode = OITW.WhsCode
        WHERE    OITW.ItemCode = ?
          AND    OITW.OnHand   > 0
        ORDER BY OWHS.WhsName
        """,
        [item_code]
    )
    return [
        {"WhsCode": row[0], "WhsName": row[1], "OnHand": float(row[2])}
        for row in cursor.fetchall()
    ]
