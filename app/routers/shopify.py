"""
Endpoints para integración con Shopify.

Todos los endpoints aceptan el header `X-SAP-DB` con valores fn | cp
para seleccionar la base SAP B1 contra la que se consulta.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import pyodbc

from app.config import (
    EMPRESAS,
    SHOPIFY_COMPARE_AT_PRICE_LIST,
    # SHOPIFY_VARIANT_PRICE_LIST queda en config por si se reactiva,
    # pero por ahora no se usa: Variant Price devuelve 0.0 fijo.
)
from app.database import get_connection
from app.security import require_api_key

# `dependencies=[Depends(require_api_key)]` aplica la auth a TODOS los endpoints
# del router. Si alguien llama sin un X-API-Key válido recibe 401 / 503.
router = APIRouter(
    prefix="/shopify",
    tags=["Shopify"],
    dependencies=[Depends(require_api_key)],
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers comunes
# ─────────────────────────────────────────────────────────────────────────────

# Si el cliente no manda X-SAP-DB usamos la base de TEST por seguridad
# (jamás escribimos por accidente sobre producción).
DEFAULT_DB_KEY = "test"


def resolve_db(x_sap_db: Optional[str]) -> str:
    """Resuelve la base SAP B1 a usar a partir del header X-SAP-DB.
    Si no se manda el header, cae al default (`test`)."""
    key = (x_sap_db or DEFAULT_DB_KEY).lower()
    if key not in EMPRESAS:
        raise HTTPException(
            status_code=400,
            detail=f"X-SAP-DB '{x_sap_db}' no válida. Usa: {list(EMPRESAS.keys())}.",
        )
    database = EMPRESAS[key]
    if not database:
        raise HTTPException(
            status_code=500,
            detail=f"La base '{key}' no está configurada en .env "
                   f"(falta SAP_DATABASE_{key.upper()}).",
        )
    return database


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


# Filtro central: solo artículos publicables a Shopify.
# - U_SHOPIFY = 'Y'  → habilitado
# - U_SHOPIFY = 'N' o NULL → bloqueado
SHOPIFY_FLAG_WHERE = "OITM.U_SHOPIFY = 'Y'"


def check_shopify_enabled(cursor, item_code: str):
    """
    Verifica que el artículo:
      1. Exista en OITM
      2. Tenga U_SHOPIFY = 'Y'

    Devuelve None si todo OK, o un JSONResponse de error
    (404 si no existe, 403 si existe pero no está habilitado).
    """
    cursor.execute(
        "SELECT U_SHOPIFY FROM OITM WHERE ItemCode = ?",
        [item_code],
    )
    row = cursor.fetchone()
    if not row:
        return err(404, f"ItemCode '{item_code}' no existe en OITM.")
    flag = (row.U_SHOPIFY or "").strip().upper()
    if flag != "Y":
        return err(
            403,
            f"ItemCode '{item_code}' no está habilitado para Shopify "
            f"(U_SHOPIFY='{flag or 'NULL'}'). Márquelo como 'Y' en SAP.",
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 1) GET /shopify/articles
#    Datos maestros de artículo + UDFs para Shopify
# ─────────────────────────────────────────────────────────────────────────────

_ARTICLES_SELECT = f"""
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
      AND  {SHOPIFY_FLAG_WHERE}
"""


def _build_article(row) -> Dict[str, Any]:
    """
    Estructura original tipo Shopify:
      - Vendor  ← OMRC.FirmName  (fabricante en SAP)
      - Type    ← OITB.ItmsGrpNam (grupo de artículos en SAP)
      - Option{1,2,3} Name/Value ← UDFs U_OPT{1,2,3}_{NAME,VALUE}
    """
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
                blocked = check_shopify_enabled(cursor, itemCode)
                if blocked:
                    return blocked
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
            cursor.execute(
                f"SELECT COUNT(*) FROM OITM WHERE Canceled = 'N' AND {SHOPIFY_FLAG_WHERE}"
            )
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

# Stock agrupado por LOCATION (sucursal/agencia), no por almacén.
# Cada OWHS pertenece a una OLCT (Locations). Sumamos el OnHand de todos
# los almacenes que viven en la misma localidad.
_STOCK_SELECT_SINGLE = """
    SELECT   OITW.ItemCode,
             COALESCE(OLCT.Location, 'SIN LOCALIDAD') AS LocationName,
             SUM(OITW.OnHand)                          AS Stock
    FROM     OITW
    JOIN     OWHS ON OWHS.WhsCode = OITW.WhsCode
    LEFT     JOIN OLCT ON OLCT.Code = OWHS.Location
    WHERE    OITW.ItemCode = ?
    GROUP BY OITW.ItemCode, COALESCE(OLCT.Location, 'SIN LOCALIDAD')
    ORDER BY LocationName
"""

_STOCK_SELECT_MANY = """
    SELECT   OITW.ItemCode,
             COALESCE(OLCT.Location, 'SIN LOCALIDAD') AS LocationName,
             SUM(OITW.OnHand)                          AS Stock
    FROM     OITW
    JOIN     OWHS ON OWHS.WhsCode = OITW.WhsCode
    LEFT     JOIN OLCT ON OLCT.Code = OWHS.Location
    WHERE    OITW.ItemCode IN ({placeholders})
    GROUP BY OITW.ItemCode, COALESCE(OLCT.Location, 'SIN LOCALIDAD')
    ORDER BY OITW.ItemCode, LocationName
"""


@router.get(
    "/stock",
    summary="Stock agrupado por localidad/sucursal (OLCT.Location)",
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
                blocked = check_shopify_enabled(cursor, itemCode)
                if blocked:
                    return blocked
                cursor.execute(_STOCK_SELECT_SINGLE, [itemCode])
                rows = cursor.fetchall()
                if not rows:
                    return err(404, f"ItemCode '{itemCode}' no tiene registros de stock.")
                stock[itemCode] = {
                    r.LocationName.strip().upper(): int(r.Stock or 0) for r in rows
                }
                return {"success": True, "message": None, "stock": stock}

            # ── Caso 2: listado paginado de ItemCodes ───────────────────────
            #   Para evitar joins enormes, paginamos ItemCodes y luego traemos
            #   sus filas de OITW en una segunda consulta.
            cursor.execute(
                f"SELECT COUNT(*) FROM OITM WHERE Canceled = 'N' AND {SHOPIFY_FLAG_WHERE}"
            )
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                f"""
                SELECT   ItemCode
                FROM     OITM
                WHERE    Canceled = 'N'
                  AND    {SHOPIFY_FLAG_WHERE}
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
                _STOCK_SELECT_MANY.format(placeholders=placeholders),
                codes,
            )

            # Inicializa el dict con todos los códigos vacíos para que
            # también aparezcan los items sin stock en ninguna localidad.
            for c in codes:
                stock[c] = {}
            for r in cursor.fetchall():
                stock[r.ItemCode][r.LocationName.strip().upper()] = int(r.Stock or 0)

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

_PRICES_SELECT = f"""
    SELECT
        OITM.ItemCode,
        P2.Price AS CompareAtPrice
    FROM   OITM
    LEFT   JOIN ITM1 P2
        ON  P2.ItemCode  = OITM.ItemCode
        AND P2.PriceList = ?
    WHERE  OITM.Canceled = 'N'
      AND  {SHOPIFY_FLAG_WHERE}
"""


# IVA mexicano que se aplica al "Variant Compare At Price" antes de mandarlo
# a Shopify. SAP guarda el precio SIN IVA y Shopify lo muestra CON IVA.
IVA_RATE = 0.16


def _with_iva(price) -> Optional[float]:
    """Aplica 16% de IVA y redondea a 2 decimales. Devuelve None si el precio es NULL."""
    if price is None:
        return None
    return round(float(price) * (1 + IVA_RATE), 2)


def _build_prices(row) -> Dict[str, Any]:
    """
    - Variant Price: queda en 0.0 hasta que definan la fuente real.
    - Variant Compare At Price: lista configurada en SHOPIFY_COMPARE_AT_PRICE_LIST
      multiplicada por 1.16 (IVA 16%).
    """
    return {
        "Variant Price":            0.0,
        "Variant Compare At Price": _with_iva(row.CompareAtPrice),
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

    base_params = [SHOPIFY_COMPARE_AT_PRICE_LIST]

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            if itemCode:
                blocked = check_shopify_enabled(cursor, itemCode)
                if blocked:
                    return blocked
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
            cursor.execute(
                f"SELECT COUNT(*) FROM OITM WHERE Canceled = 'N' AND {SHOPIFY_FLAG_WHERE}"
            )
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
