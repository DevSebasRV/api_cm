from fastapi import APIRouter, Header, Query
from fastapi.responses import JSONResponse
from typing import Optional
import pyodbc

from app.config import EMPRESAS
from app.database import get_connection

router = APIRouter(tags=["Business Partners"])


# Mapeo del campo OCRD.CardType (1 char) al enum de Service Layer
_TYPE_MAP = {"C": "cCustomer", "S": "cSupplier", "L": "cLid"}


_SELECT = """
    SELECT  CardCode,
            CardName,
            LicTradNum,
            Phone1,
            Cellular,
            CardType,
            Balance
    FROM    OCRD
"""


def build_bp(row) -> dict:
    return {
        "CardCode":              row[0],
        "CardName":              row[1],
        "FederalTaxID":          row[2],
        "Phone1":                row[3],
        "Cellular":              row[4],
        "CardType":              _TYPE_MAP.get(row[5], row[5]),
        "CurrentAccountBalance": float(row[6]) if row[6] is not None else 0.0,
    }


def _keyword_conditions(keyword: str):
    """Cada palabra busca en CardCode, CardName y LicTradNum."""
    words  = keyword.split()
    clause = " ".join(
        "AND (CardCode LIKE ? OR CardName LIKE ? OR LicTradNum LIKE ?)"
        for _ in words
    )
    params = [p for w in words for p in (f"%{w}%", f"%{w}%", f"%{w}%")]
    return clause, params


def _get_total(cursor, keyword: Optional[str] = None) -> int:
    if keyword:
        kw_clause, kw_params = _keyword_conditions(keyword)
        cursor.execute(
            f"SELECT COUNT(*) FROM OCRD WHERE CardType = 'C' {kw_clause}",
            kw_params,
        )
    else:
        cursor.execute("SELECT COUNT(*) FROM OCRD WHERE CardType = 'C'")
    return cursor.fetchone()[0]


def fetch_bps(cursor, page: int, page_size: int, keyword: Optional[str] = None):
    total  = _get_total(cursor, keyword)
    offset = (page - 1) * page_size

    if keyword:
        kw_clause, kw_params = _keyword_conditions(keyword)
        cursor.execute(
            _SELECT + f" WHERE CardType = 'C' {kw_clause}"
                      " ORDER BY CreateDate DESC, CardCode DESC"
                      " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            kw_params + [offset, page_size],
        )
    else:
        cursor.execute(
            _SELECT + " WHERE CardType = 'C'"
                      " ORDER BY CardCode"
                      " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            [offset, page_size],
        )

    return [build_bp(row) for row in cursor.fetchall()], total


def err(status: int, message: str):
    return JSONResponse(status_code=status, content={
        "success": False,
        "message": message,
        "data":    None,
    })


@router.get(
    "/businessPartners",
    summary="Socios de negocio SAP B1 (lectura rápida vía ODBC)",
)
def get_business_partners(
    empresa: Optional[str] = Query(
        default=None,
        description="Base: fn | cp. Si se omite se toma del header X-SAP-DB.",
    ),
    cardCode: Optional[str] = Query(
        default=None,
        description="Código exacto del socio (OCRD.CardCode).",
    ),
    keyword: Optional[str] = Query(
        default=None,
        description="Texto libre — busca en CardCode, CardName y RFC.",
    ),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=500, ge=1, le=5000),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    # Resolución de empresa: query string > header > default
    db_key = (empresa or x_sap_db or "fn").lower()
    if db_key not in EMPRESAS:
        return err(400, f"empresa '{db_key}' no válida. Usa: {list(EMPRESAS.keys())}.")

    try:
        conn   = get_connection(EMPRESAS[db_key])
        cursor = conn.cursor()
        try:
            # 1) Búsqueda por código exacto
            if cardCode:
                cursor.execute(_SELECT + " WHERE CardCode = ? AND CardType = 'C'", [cardCode])
                row = cursor.fetchone()
                if not row:
                    return err(404, f"Socio '{cardCode}' no encontrado en {db_key}.")
                return {
                    "success": True,
                    "message": None,
                    "businessPartners": [build_bp(row)],
                }

            # 2) Listado paginado (con o sin keyword)
            bps, total  = fetch_bps(cursor, page, pageSize, keyword or None)
            total_pages = max(1, (total + pageSize - 1) // pageSize)

            return {
                "success": True,
                "message": None,
                "pagination": {
                    "page":       page,
                    "pageSize":   pageSize,
                    "total":      total,
                    "totalPages": total_pages,
                },
                "businessPartners": bps,
            }
        finally:
            cursor.close()
            conn.close()

    except pyodbc.Error as db_err:
        return err(500, f"Error de conexión a SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
