"""
Endpoints para integración con Shopify.

Todos los endpoints aceptan el header `X-SAP-DB` con valores fn | cp
para seleccionar la base SAP B1 contra la que se consulta.
"""

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import pyodbc

from app.config import (
    EMPRESAS,
    SHOPIFY_VARIANT_PRICE_LIST,
    SHOPIFY_COMPARE_AT_PRICE_LIST,
)
from app.database import get_connection

router = APIRouter(prefix="/shopify", tags=["Shopify"])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers comunes
# ─────────────────────────────────────────────────────────────────────────────

def resolve_db(x_sap_db: Optional[str]) -> str:
    """Resuelve la base SAP B1 a usar a partir del header X-SAP-DB."""
    if not x_sap_db:
        raise HTTPException(
            status_code=400,
            detail="Falta el header 'X-SAP-DB'. Valores válidos: " + ", ".join(EMPRESAS.keys()),
        )
    key = x_sap_db.lower()
    if key not in EMPRESAS:
        raise HTTPException(
            status_code=400,
            detail=f"X-SAP-DB '{x_sap_db}' no válida. Usa: {list(EMPRESAS.keys())}.",
        )
    return EMPRESAS[key]


def err(status: int, message: str):
    return JSONResponse(
        status_code=status,
        content={"success": False, "message": message, "data": None},
    )


def _pagination(page: int, page_size: int, total: int) -> Dict[str, Any]:
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "page":       page,
        "pageSize":   page_size,
        "total":      total,
        "totalPages": total_pages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1) GET /shopify/articles
#    Datos maestros de artículo + UDFs para Shopify
# ─────────────────────────────────────────────────────────────────────────────

_ARTICLES_SELECT = """
    SELECT
        OITM.ItemCode,
        OMRC.FirmName       AS Vendor,
        OITB.ItmsGrpNam     AS ProductType,
        OITM.U_OPT1_NAME    AS Opt1Name,
        OITM.U_OPT1_VALUE   AS Opt1Value,
        OITM.U_OPT2_NAME    AS Opt2Name,
        OITM.U_OPT2_VALUE   AS Opt2Value,
        OITM.U_OPT3_NAME    AS Opt3Name,
        OITM.U_OPT3_VALUE   AS Opt3Value
    FROM   OITM
    LEFT   JOIN OMRC ON OMRC.FirmCode    = OITM.FirmCode
    LEFT   JOIN OITB ON OITB.ItmsGrpCod  = OITM.ItmsGrpCod
    WHERE  OITM.Canceled = 'N'
"""


def _build_article(row) -> Dict[str, Any]:
    return {
        "Vendor":        row.Vendor,
        "Type":          row.ProductType,
        "Option1 Name":  row.Opt1Name,
        "Option1 Value": row.Opt1Value,
        "Option2 Name":  row.Opt2Name,
        "Option2 Value": row.Opt2Value,
        "Option3 Name":  row.Opt3Name,
        "Option3 Value": row.Opt3Value,
    }


@router.get(
    "/articles",
    summary="Datos maestros de artículo + UDFs para Shopify",
)
def get_articles(
    itemCode: Optional[str] = Query(default=None, description="Código exacto (OITM.ItemCode)."),
    page:     int           = Query(default=1, ge=1),
    pageSize: int           = Query(default=100, ge=1, le=2000),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            if itemCode:
                cursor.execute(_ARTICLES_SELECT + " AND OITM.ItemCode = ?", [itemCode])
                row = cursor.fetchone()
                if not row:
                    return err(404, f"ItemCode '{itemCode}' no encontrado.")
                return {
                    "success":  True,
                    "message":  None,
                    "articles": { row.ItemCode: _build_article(row) },
                }

            # ── Caso 2: listado paginado ─────────────────────────────────────
            cursor.execute("SELECT COUNT(*) FROM OITM WHERE Canceled = 'N'")
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                _ARTICLES_SELECT
                + " ORDER BY OITM.ItemCode OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                [offset, pageSize],
            )
            articles = { row.ItemCode: _build_article(row) for row in cursor.fetchall() }

            return {
                "success":    True,
                "message":    None,
                "pagination": _pagination(page, pageSize, total),
                "articles":   articles,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de conexión a SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 2) GET /shopify/stock
#    Stock por almacén, agrupado por ItemCode
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/stock",
    summary="Stock por almacén, agrupado por artículo",
)
def get_stock(
    itemCode: Optional[str] = Query(default=None, description="Código exacto (OITW.ItemCode)."),
    page:     int           = Query(default=1, ge=1),
    pageSize: int           = Query(default=100, ge=1, le=2000),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            stock: Dict[str, Dict[str, int]] = {}

            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            if itemCode:
                cursor.execute(
                    """
                    SELECT   OITW.ItemCode, OWHS.WhsName, OITW.OnHand
                    FROM     OITW
                    JOIN     OWHS ON OWHS.WhsCode = OITW.WhsCode
                    WHERE    OITW.ItemCode = ?
                    ORDER BY OWHS.WhsName
                    """,
                    [itemCode],
                )
                rows = cursor.fetchall()
                if not rows:
                    return err(404, f"ItemCode '{itemCode}' no tiene registros de stock.")
                stock[itemCode] = {
                    r.WhsName.strip().upper(): int(r.OnHand or 0) for r in rows
                }
                return {"success": True, "message": None, "stock": stock}

            # ── Caso 2: listado paginado de ItemCodes ───────────────────────
            #   Para evitar joins enormes, paginamos ItemCodes y luego traemos
            #   sus filas de OITW en una segunda consulta.
            cursor.execute("SELECT COUNT(*) FROM OITM WHERE Canceled = 'N'")
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                """
                SELECT   ItemCode
                FROM     OITM
                WHERE    Canceled = 'N'
                ORDER BY ItemCode
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
                """,
                [offset, pageSize],
            )
            codes = [r.ItemCode for r in cursor.fetchall()]
            if not codes:
                return {
                    "success":    True,
                    "message":    None,
                    "pagination": _pagination(page, pageSize, total),
                    "stock":      {},
                }

            placeholders = ",".join("?" * len(codes))
            cursor.execute(
                f"""
                SELECT   OITW.ItemCode, OWHS.WhsName, OITW.OnHand
                FROM     OITW
                JOIN     OWHS ON OWHS.WhsCode = OITW.WhsCode
                WHERE    OITW.ItemCode IN ({placeholders})
                ORDER BY OITW.ItemCode, OWHS.WhsName
                """,
                codes,
            )

            # Inicializa el dict con todos los códigos vacíos
            for c in codes:
                stock[c] = {}
            for r in cursor.fetchall():
                stock[r.ItemCode][r.WhsName.strip().upper()] = int(r.OnHand or 0)

            return {
                "success":    True,
                "message":    None,
                "pagination": _pagination(page, pageSize, total),
                "stock":      stock,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de conexión a SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 3) GET /shopify/prices
#    Variant Price (precio descuento) + Variant Compare At Price (lista 01)
# ─────────────────────────────────────────────────────────────────────────────

_PRICES_SELECT = """
    SELECT
        OITM.ItemCode,
        P1.Price AS VariantPrice,
        P2.Price AS CompareAtPrice
    FROM   OITM
    LEFT   JOIN ITM1 P1
        ON  P1.ItemCode  = OITM.ItemCode
        AND P1.PriceList = ?
    LEFT   JOIN ITM1 P2
        ON  P2.ItemCode  = OITM.ItemCode
        AND P2.PriceList = ?
    WHERE  OITM.Canceled = 'N'
"""


def _build_prices(row) -> Dict[str, Any]:
    return {
        "Variant Price":            float(row.VariantPrice)   if row.VariantPrice   is not None else None,
        "Variant Compare At Price": float(row.CompareAtPrice) if row.CompareAtPrice is not None else None,
    }


@router.get(
    "/prices",
    summary="Precios de Shopify (Variant Price + Compare At Price)",
)
def get_prices(
    itemCode: Optional[str] = Query(default=None, description="Código exacto (OITM.ItemCode)."),
    page:     int           = Query(default=1, ge=1),
    pageSize: int           = Query(default=100, ge=1, le=2000),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    database = resolve_db(x_sap_db)

    base_params = [SHOPIFY_VARIANT_PRICE_LIST, SHOPIFY_COMPARE_AT_PRICE_LIST]

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            if itemCode:
                cursor.execute(
                    _PRICES_SELECT + " AND OITM.ItemCode = ?",
                    base_params + [itemCode],
                )
                row = cursor.fetchone()
                if not row:
                    return err(404, f"ItemCode '{itemCode}' no encontrado.")
                return {
                    "success": True,
                    "message": None,
                    "prices":  { row.ItemCode: _build_prices(row) },
                }

            # ── Caso 2: listado paginado ─────────────────────────────────────
            cursor.execute("SELECT COUNT(*) FROM OITM WHERE Canceled = 'N'")
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                _PRICES_SELECT
                + " ORDER BY OITM.ItemCode OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                base_params + [offset, pageSize],
            )
            prices = { row.ItemCode: _build_prices(row) for row in cursor.fetchall() }

            return {
                "success":    True,
                "message":    None,
                "pagination": _pagination(page, pageSize, total),
                "prices":     prices,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de conexión a SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
