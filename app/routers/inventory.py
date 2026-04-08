from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
import pyodbc

from app.config import EMPRESAS, PRICE_LIST_CODE
from app.database import get_connection, get_warehouse_stock

router = APIRouter(tags=["Inventory Items"])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de respuesta
# ─────────────────────────────────────────────────────────────────────────────

def ok_part(part: dict):
    return JSONResponse(status_code=200, content={
        "success": True,
        "message": None,
        "part":    part,
    })

def ok_parts(parts: list, page: int = None, page_size: int = None, total: int = None):
    body = {"success": True, "message": None}
    if page is not None:
        total_pages = max(1, (total + page_size - 1) // page_size)
        body["pagination"] = {
            "page":       page,
            "pageSize":   page_size,
            "total":      total,
            "totalPages": total_pages,
        }
    body["parts"] = parts
    return JSONResponse(status_code=200, content=body)

def err(status: int, message: str):
    return JSONResponse(status_code=status, content={
        "success": False,
        "message": message,
        "data":    None,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del objeto part
# ─────────────────────────────────────────────────────────────────────────────

def build_part(row, warehouse_stock: list, empresa: str) -> dict:
    item_code = row[0]
    item_name = row[1]
    on_hand   = int(row[2]) if row[2] is not None else 0
    price     = float(row[3]) if row[3] is not None else 0.0

    if warehouse_stock:
        comments = [
            f"{w['WhsName']} ({w['WhsCode']}): {int(w['OnHand'])}"
            for w in warehouse_stock
        ]
    else:
        comments = []

    return {
        "partName":         item_name,
        "partId":           item_code,
        "quantity":         1,
        "partUnitPrice":    price,
        "availability":     on_hand,
        "laborHours":       None,
        "laborHourPrice":   None,
        "discount":         0.0,
        "comments":         comments,
        #"_empresa":         empresa,
        #"_warehouseDetail": warehouse_stock,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Consultas a la base de datos
# ─────────────────────────────────────────────────────────────────────────────

_SELECT = """
    SELECT
        OITM.ItemCode,
        OITM.ItemName,
        OITM.OnHand,
        ITM1.Price
    FROM   OITM
    LEFT   JOIN ITM1
        ON  ITM1.ItemCode  = OITM.ItemCode
        AND ITM1.PriceList = ?
    WHERE  OITM.Canceled = 'N'
"""

def fetch_single_item(cursor, item_code: str, empresa: str):
    cursor.execute(
        _SELECT + " AND OITM.ItemCode = ?",
        [PRICE_LIST_CODE, item_code]
    )
    row = cursor.fetchone()
    if not row:
        return None
    whs = get_warehouse_stock(cursor, item_code)
    return build_part(row, whs, empresa)


def fetch_single_item_ambas(item_code: str) -> list:
    results = []
    for emp, database in EMPRESAS.items():
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            part = fetch_single_item(cursor, item_code, emp)
            if part:
                results.append(part)
        finally:
            cursor.close()
            conn.close()
    return results


def _fetch_slice(cursor, offset: int, limit: int, empresa: str, keyword: str = None) -> list:
    if limit <= 0:
        return []

    if keyword:
        pattern = f"%{keyword}%"
        cursor.execute(
            _SELECT + " AND (OITM.ItemCode LIKE ? OR OITM.ItemName LIKE ?)"
                      " ORDER BY OITM.ItemCode"
                      " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            [PRICE_LIST_CODE, pattern, pattern, offset, limit]
        )
    else:
        cursor.execute(
            _SELECT + " ORDER BY OITM.ItemCode"
                      " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            [PRICE_LIST_CODE, offset, limit]
        )

    parts = []
    for row in cursor.fetchall():
        whs = get_warehouse_stock(cursor, row[0])
        parts.append(build_part(row, whs, empresa))
    return parts


def _get_total(cursor, keyword: str = None) -> int:
    if keyword:
        pattern = f"%{keyword}%"
        cursor.execute(
            "SELECT COUNT(*) FROM OITM"
            " WHERE Canceled = 'N' AND (ItemCode LIKE ? OR ItemName LIKE ?)",
            [pattern, pattern]
        )
    else:
        cursor.execute("SELECT COUNT(*) FROM OITM WHERE Canceled = 'N'")
    return cursor.fetchone()[0]


def fetch_items(cursor, page: int, page_size: int, empresa: str, keyword: str = None):
    """Devuelve (lista_de_parts, total) para una sola empresa."""
    total  = _get_total(cursor, keyword)
    offset = (page - 1) * page_size
    parts  = _fetch_slice(cursor, offset, page_size, empresa, keyword)
    return parts, total


def fetch_items_ambas(page: int, page_size: int, keyword: str = None):
    """Devuelve (lista_de_parts, total_combinado) para fn + cp."""
    conn_fn = get_connection(EMPRESAS["fn"])
    conn_cp = get_connection(EMPRESAS["cp"])
    cur_fn  = conn_fn.cursor()
    cur_cp  = conn_cp.cursor()

    try:
        total_fn = _get_total(cur_fn, keyword)
        total_cp = _get_total(cur_cp, keyword)
        total    = total_fn + total_cp

        offset_global = (page - 1) * page_size
        parts = []

        if offset_global < total_fn:
            cant_fn = min(page_size, total_fn - offset_global)
            parts  += _fetch_slice(cur_fn, offset_global, cant_fn, "fn", keyword)

        restante = page_size - len(parts)
        if restante > 0:
            offset_cp = max(0, offset_global - total_fn)
            parts    += _fetch_slice(cur_cp, offset_cp, restante, "cp", keyword)

    finally:
        cur_fn.close(); conn_fn.close()
        cur_cp.close(); conn_cp.close()

    return parts, total


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/inventoryItems",
    summary="Artículos SAP B1 en formato ClearMechanic",
)
def get_inventory_items(
    itemCode: Optional[str] = Query(
        default=None,
        description="Código exacto del artículo (OITM.ItemCode).",
    ),
    keyword: Optional[str] = Query(
        default=None,
        description="Texto libre — busca en ItemCode e ItemName (paginado).",
    ),
    empresa: str = Query(
        default="fn",
        description="Base de datos: fn | cp | ambas.",
    ),
    page: int = Query(default=1, ge=1, description="Número de página."),
    pageSize: int = Query(default=50, ge=1, le=500, description="Artículos por página (máx 500)."),
):
    empresa = empresa.lower()
    if empresa not in ("fn", "cp", "ambas"):
        return err(400, "El parámetro 'empresa' debe ser fn, cp o ambas.")

    try:
        # 1. Búsqueda por código exacto 
        if itemCode:
            if empresa == "ambas":
                results = fetch_single_item_ambas(itemCode)
                if not results:
                    return err(404, f"Item '{itemCode}' no encontrado en ninguna empresa.")
                return ok_parts(results) 

            conn   = get_connection(EMPRESAS[empresa])
            cursor = conn.cursor()
            part   = fetch_single_item(cursor, itemCode, empresa)
            cursor.close(); conn.close()

            if part is None:
                return err(404, f"Item '{itemCode}' no encontrado en {empresa}.")
            return ok_part(part) 

        # 2. Búsqueda por keyword / listado paginado
        if empresa == "ambas":
            parts, total = fetch_items_ambas(page, pageSize, keyword or None)
        else:
            conn   = get_connection(EMPRESAS[empresa])
            cursor = conn.cursor()
            parts, total = fetch_items(cursor, page, pageSize, empresa, keyword or None)
            cursor.close(); conn.close()

        return ok_parts(parts, page=page, page_size=pageSize, total=total)

    except pyodbc.Error as db_err:
        return err(500, f"Error de conexión a SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
