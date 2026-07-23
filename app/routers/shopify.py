"""
Endpoints para integración con Shopify.

Todos los endpoints aceptan el header `X-SAP-DB` con valores fn | cp
para seleccionar la base SAP B1 contra la que se consulta.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, List
import pyodbc

# Variant Price se responde fijo en 0.0 (no hay lista de precios para él).
from app.config import EMPRESAS, SHOPIFY_COMPARE_AT_PRICE_LIST
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
# Helpers comunes → viven en app/routers/common.py (compartidos por routers)
# ─────────────────────────────────────────────────────────────────────────────

from app.routers.common import DEFAULT_DB_KEY, resolve_db, err, _pagination  # noqa: F401


# ─────────────────────────────────────────────────────────────────────────────
# Mapeo SAP OLCT.Location → nombre Shopify
# ─────────────────────────────────────────────────────────────────────────────
# Shopify exige nombres exactos: Coapa, Patriotismo, Roma, Satélite.
# Como las sucursales en SAP difieren entre bases (FERBEL guarda "Satélite"
# con acento, PROSHOP "Satelite" sin acento, etc.), el mapeo es por base.
#
# Si la misma SAP-location aparece varias veces en SQL (poco probable porque
# ya está agregado por COALESCE/GROUP BY), el helper las suma automáticamente.
SHOPIFY_LOCATION_MAP: Dict[str, Dict[str, str]] = {
    "cp": {  # PROSHOP-2023
        "Patriotismo":      "Patriotismo",
        "Satelite":         "Satélite",
        "Sur (Miramontes)": "Coapa",
        # ⚠️ PROSHOP no tiene sucursal "Tonala" — Roma siempre devuelve 0.
        # El almacén TONBOUT está mal asignado a Satelite en SAP — reportar.
    },
    "fn": {  # FERBEL-2023
        "Patriotismo":      "Patriotismo",
        "Satélite":         "Satélite",
        "Sur (Miramontes)": "Coapa",
        "Tonala":           "Roma",
        "Zona Esmeralda":   "ZonaEsmeralda",
    },
    "test": {  # PROSHOP-TEST
        "Patriotismo":      "Patriotismo",
        "Satelite":         "Satélite",
        "Sur (Miramontes)": "Coapa",
    },
}

# Llaves que SIEMPRE deben aparecer en el JSON (aunque tengan 0 en stock).
# Shopify espera estos 4 nombres fijos; FERBEL agrega ZonaEsmeralda como 5ª.
SHOPIFY_REQUIRED_LOCATIONS: Dict[str, list] = {
    "cp":   ["Coapa", "Patriotismo", "Roma", "Satélite"],
    "fn":   ["Coapa", "Patriotismo", "Roma", "Satélite", "ZonaEsmeralda"],
    "test": ["Coapa", "Patriotismo", "Roma", "Satélite"],
}


def _build_stock_for_item(rows, db_key: str) -> Dict[str, int]:
    """
    Convierte las filas crudas de SQL (LocationName + Stock) al dict que
    devuelve la API, aplicando el mapeo SAP→Shopify y garantizando que las
    llaves requeridas estén presentes (con 0 si no hay stock).
    """
    location_map  = SHOPIFY_LOCATION_MAP.get(db_key, {})
    required_keys = SHOPIFY_REQUIRED_LOCATIONS.get(db_key, [])

    # 1. Inicializa con todas las llaves requeridas en 0
    result: Dict[str, int] = {k: 0 for k in required_keys}

    # 2. Suma stock por mapeo (SAP location → Shopify name)
    for r in rows:
        sap_location = (r.LocationName or "").strip()
        shopify_name = location_map.get(sap_location)
        if not shopify_name:
            # SAP location no mapeada → se ignora silenciosamente
            continue
        result[shopify_name] = result.get(shopify_name, 0) + int(r.Stock or 0)

    return result




# Filtro central: existencia en la UDT @SHOPIFY_ARTICLE con U_Activo='Y'.
# Los artículos NO viven en OITM filtrados por una bandera — viven en una
# tabla aparte, gestionable desde Retool/SQL.
SHOPIFY_GATE_WHERE = "ISNULL(U_Activo, 'Y') = 'Y'"


def check_shopify_enabled(cursor, item_code: str):
    """
    Verifica que el artículo:
      1. Exista en [@SHOPIFY_ARTICLE]
      2. Tenga U_Activo distinto de 'N'

    Devuelve None si todo OK, o un JSONResponse de error
    (403 si no está publicable).
    """
    cursor.execute(
        f"SELECT 1 FROM [@SHOPIFY_ARTICLE] "
        f"WHERE Code = ? AND {SHOPIFY_GATE_WHERE}",
        [item_code],
    )
    if not cursor.fetchone():
        return err(
            403,
            f"ItemCode '{item_code}' no es publicable en Shopify "
            f"(no existe en @SHOPIFY_ARTICLE o U_Activo='N').",
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 1) GET /shopify/articles
#    Datos maestros de artículo + UDFs para Shopify
# ─────────────────────────────────────────────────────────────────────────────

# Columna (UDF) donde se guardan las URLs de imágenes, una por línea.
# El campo es opcional: el código detecta si existe y degrada con gracia
# (no rompe la API antes de que se cree el UDF en SAP).
IMAGES_COL = "U_Imagenes"

# Cache de existencia de la columna por base de datos (se llena en runtime).
_IMAGES_COL_CACHE: Dict[str, bool] = {}


def _has_images_col(cursor, database: str) -> bool:
    """¿Existe la columna U_Imagenes en [@SHOPIFY_ARTICLE] de esta base?"""
    if database in _IMAGES_COL_CACHE:
        return _IMAGES_COL_CACHE[database]
    try:
        cursor.execute(
            "SELECT 1 FROM sys.columns "
            "WHERE object_id = OBJECT_ID('[@SHOPIFY_ARTICLE]') AND name = ?",
            [IMAGES_COL],
        )
        exists = cursor.fetchone() is not None
    except pyodbc.Error:
        exists = False
    _IMAGES_COL_CACHE[database] = exists
    return exists


BODY_COL = "U_Body"

# Cache de existencia de la columna U_Body por base (igual que U_Imagenes).
_BODY_COL_CACHE: Dict[str, bool] = {}


def _has_body_col(cursor, database: str) -> bool:
    """¿Existe la columna U_Body (HTML del producto) en [@SHOPIFY_ARTICLE]?"""
    if database in _BODY_COL_CACHE:
        return _BODY_COL_CACHE[database]
    try:
        cursor.execute(
            "SELECT 1 FROM sys.columns "
            "WHERE object_id = OBJECT_ID('[@SHOPIFY_ARTICLE]') AND name = ?",
            [BODY_COL],
        )
        exists = cursor.fetchone() is not None
    except pyodbc.Error:
        exists = False
    _BODY_COL_CACHE[database] = exists
    return exists


def _parse_images(raw: Optional[str]) -> list:
    """Texto del UDF (una URL por línea) → lista de URLs limpias (sin vacías)."""
    if not raw:
        return []
    return [u.strip() for u in raw.replace("\r\n", "\n").split("\n") if u.strip()]


_ARTICLES_BASE_COLS = """
        Code         AS ItemCode,
        Name         AS ItemName,
        U_Activo     AS Activo,
        U_Vendor     AS Vendor,
        U_Type       AS ProductType,
        U_Opt1Name   AS Opt1Name,
        U_Opt1Value  AS Opt1Value,
        U_Opt2Name   AS Opt2Name,
        U_Opt2Value  AS Opt2Value,
        U_Opt3Name   AS Opt3Name,
        U_Opt3Value  AS Opt3Value
"""


def _articles_select(has_img: bool, has_body: bool = False) -> str:
    """SELECT de artículos; incluye imágenes/body solo si sus columnas existen."""
    img  = ",\n        U_Imagenes AS ImagesRaw" if has_img else ""
    body = ",\n        U_Body     AS BodyRaw" if has_body else ""
    return f"SELECT {_ARTICLES_BASE_COLS}{img}{body}\n    FROM [@SHOPIFY_ARTICLE]"


def _activo_to_status(flag: Optional[str]) -> str:
    """Convierte el flag U_Activo de SAP (Y/N/NULL) al string visible Activa/Inactiva."""
    return "Activa" if (flag or "Y").strip().upper() == "Y" else "Inactiva"


def _build_article(row, has_img: bool, has_body: bool = False) -> Dict[str, Any]:
    """
    Toda la data sale de la UDT @SHOPIFY_ARTICLE.
    Las llaves van sin espacios (Option1Name en vez de "Option1 Name") para
    facilitar el consumo desde código (acceso por atributo / destructuring).

    Imágenes: SIEMPRE se devuelve la llave "Images" como ARRAY:
      - 0 imágenes (o el UDF no existe en esta base) → "Images": []
      - 1 o más                                      → "Images": ["url1", ...]
    """
    art = {
        "Name":          row.ItemName,
        "Status":        _activo_to_status(row.Activo),
        "Vendor":        row.Vendor,
        "Type":          row.ProductType,
        "Option1Name":   row.Opt1Name,
        "Option1Value":  row.Opt1Value,
        "Option2Name":   row.Opt2Name,
        "Option2Value":  row.Opt2Value,
        "Option3Name":   row.Opt3Name,
        "Option3Value":  row.Opt3Value,
        # Siempre array: vacío si la base no tiene el UDF o el artículo no tiene imágenes.
        "Images":        _parse_images(row.ImagesRaw) if has_img else [],
        # HTML del producto (U_Body, tipo Memo). Se guarda y devuelve TAL CUAL.
        "Body":          (row.BodyRaw if has_body else None),
    }
    return art


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
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            has_img  = _has_images_col(cursor, database)
            has_body = _has_body_col(cursor, database)
            select   = _articles_select(has_img, has_body)

            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            # Devolvemos el item EXISTA O NO sea activo — el campo Status
            # indica al consumer si está Activa/Inactiva.
            if itemCode:
                cursor.execute(select + " WHERE Code = ?", [itemCode])
                row = cursor.fetchone()
                if not row:
                    return err(
                        404,
                        f"ItemCode '{itemCode}' no existe en @SHOPIFY_ARTICLE.",
                    )
                return {
                    "success":  True,
                    "message":  None,
                    "articles": { row.ItemCode: _build_article(row, has_img) },
                }

            # ── Caso 2: listado paginado (activos + inactivos) ─────────────
            cursor.execute("SELECT COUNT(*) FROM [@SHOPIFY_ARTICLE]")
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                select
                + " ORDER BY Code OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                [offset, pageSize],
            )
            articles = {
                row.ItemCode: _build_article(row, has_img)
                for row in cursor.fetchall()
            }

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
# 1b) POST / PATCH / DELETE /shopify/articles  — escritura a la UDT
#     SAP B1 Service Layer NO expone las UDTs como servicio por defecto
#     (requiere registrarlas como UDO). Para evitar ese paso, escribimos
#     SQL directo con pyodbc. Misma transacción, misma BD.
# ─────────────────────────────────────────────────────────────────────────────

from pydantic import BaseModel, Field as PydField  # noqa: E402


class ArticleIn(BaseModel):
    """
    Payload de create/update.
    - `code` (PK) requerido en POST, opcional en PATCH (va en la URL)
    - El resto opcional — cualquier campo `None` no se toca en UPDATE
    """
    code:       Optional[str] = PydField(None, max_length=50)
    name:       Optional[str] = PydField(None, max_length=100)
    vendor:     Optional[str] = PydField(None, max_length=100)
    type:       Optional[str] = PydField(None, max_length=100)
    opt1Name:   Optional[str] = PydField(None, max_length=50)
    opt1Value:  Optional[str] = PydField(None, max_length=50)
    opt2Name:   Optional[str] = PydField(None, max_length=50)
    opt2Value:  Optional[str] = PydField(None, max_length=50)
    opt3Name:   Optional[str] = PydField(None, max_length=50)
    opt3Value:  Optional[str] = PydField(None, max_length=50)
    activo:     Optional[str] = PydField(None, pattern=r"^[YN]$")
    # URLs de imágenes. None = no tocar (en update); [] = limpiar.
    imagenes:   Optional[List[str]] = PydField(None)
    # HTML del producto (descripción larga). Sin límite de largo (UDF Memo).
    # None = no tocar (en update); "" = limpiar.
    body:       Optional[str] = PydField(None)


def _join_images(urls: Optional[List[str]]) -> Optional[str]:
    """Lista de URLs → texto con una URL por línea (o None si vacía)."""
    if not urls:
        return None
    clean = [u.strip() for u in urls if u and u.strip()]
    return "\n".join(clean) if clean else None


@router.post(
    "/articles",
    summary="Crea un artículo en la UDT @SHOPIFY_ARTICLE",
)
def create_article(
    payload: ArticleIn,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    if not payload.code:
        return err(400, "El campo 'code' (SKU) es requerido.")
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Verificar que no exista
            cursor.execute("SELECT 1 FROM [@SHOPIFY_ARTICLE] WHERE Code = ?", [payload.code])
            if cursor.fetchone():
                return err(409, f"Ya existe un artículo con SKU '{payload.code}'.")

            cols = ["Code", "Name", "U_Vendor", "U_Type",
                    "U_Opt1Name", "U_Opt1Value",
                    "U_Opt2Name", "U_Opt2Value",
                    "U_Opt3Name", "U_Opt3Value", "U_Activo"]
            vals = [
                payload.code,
                payload.name      or None,
                payload.vendor    or None,
                payload.type      or None,
                payload.opt1Name  or None,
                payload.opt1Value or None,
                payload.opt2Name  or None,
                payload.opt2Value or None,
                payload.opt3Name  or None,
                payload.opt3Value or None,
                (payload.activo or "Y").upper(),
            ]
            # Imágenes: solo si el UDF existe en esta base
            if _has_images_col(cursor, database):
                cols.append(IMAGES_COL)
                vals.append(_join_images(payload.imagenes))

            # Body (HTML): tal cual, sin transformar — solo si el UDF existe
            if _has_body_col(cursor, database):
                cols.append(BODY_COL)
                vals.append(payload.body or None)

            placeholders = ", ".join("?" * len(cols))
            cursor.execute(
                f"INSERT INTO [@SHOPIFY_ARTICLE] ({', '.join(cols)}) VALUES ({placeholders})",
                vals,
            )
            conn.commit()
            return {"success": True, "message": None, "code": payload.code}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.patch(
    "/articles/{code}",
    summary="Actualiza un artículo (solo los campos enviados)",
)
def update_article(
    code:    str,
    payload: ArticleIn,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM [@SHOPIFY_ARTICLE] WHERE Code = ?", [code])
            if not cursor.fetchone():
                return err(404, f"No existe artículo con SKU '{code}'.")

            # Construir SET dinámico: solo campos NO-None
            sets:   list[str] = []
            params: list[Any] = []
            mapping = [
                ("Name",        payload.name),
                ("U_Vendor",    payload.vendor),
                ("U_Type",      payload.type),
                ("U_Opt1Name",  payload.opt1Name),
                ("U_Opt1Value", payload.opt1Value),
                ("U_Opt2Name",  payload.opt2Name),
                ("U_Opt2Value", payload.opt2Value),
                ("U_Opt3Name",  payload.opt3Name),
                ("U_Opt3Value", payload.opt3Value),
                ("U_Activo",    payload.activo.upper() if payload.activo else None),
            ]
            for col, val in mapping:
                if val is not None:
                    sets.append(f"{col} = ?")
                    # Strings vacíos → NULL para no ensuciar la tabla
                    params.append(val if val != "" else None)

            # Imágenes: solo si el UDF existe y el payload las trae (None = no tocar)
            if payload.imagenes is not None and _has_images_col(cursor, database):
                sets.append(f"{IMAGES_COL} = ?")
                params.append(_join_images(payload.imagenes))

            # Body (HTML): None = no tocar; "" = limpiar; texto = guardar tal cual
            if payload.body is not None and _has_body_col(cursor, database):
                sets.append(f"{BODY_COL} = ?")
                params.append(payload.body if payload.body != "" else None)

            if not sets:
                return err(400, "No se mandó ningún campo para actualizar.")

            params.append(code)
            cursor.execute(
                f"UPDATE [@SHOPIFY_ARTICLE] SET {', '.join(sets)} WHERE Code = ?",
                params,
            )
            conn.commit()
            return {"success": True, "message": None, "code": code}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.delete(
    "/articles/{code}",
    summary="Elimina un artículo (hard delete). Para soft-delete usar PATCH con activo=N.",
)
def delete_article(
    code: str,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM [@SHOPIFY_ARTICLE] WHERE Code = ?", [code])
            if cursor.rowcount == 0:
                return err(404, f"No existe artículo con SKU '{code}'.")
            conn.commit()
            return {"success": True, "message": None, "code": code}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
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
    db_key, database = resolve_db(x_sap_db)

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
                # Aunque no haya filas, devolvemos las 4 llaves requeridas en 0
                # (el item ya pasó check_shopify_enabled, así que es válido).
                stock[itemCode] = _build_stock_for_item(rows, db_key)
                return {"success": True, "message": None, "stock": stock}

            # ── Caso 2: listado paginado de ItemCodes ───────────────────────
            #   Para evitar joins enormes, paginamos ItemCodes desde la UDT y
            #   luego traemos sus filas de OITW en una segunda consulta.
            cursor.execute(
                f"SELECT COUNT(*) FROM [@SHOPIFY_ARTICLE] WHERE {SHOPIFY_GATE_WHERE}"
            )
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                f"""
                SELECT   Code AS ItemCode
                FROM     [@SHOPIFY_ARTICLE]
                WHERE    {SHOPIFY_GATE_WHERE}
                ORDER BY Code
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

            # Agrupar las filas de stock por ItemCode
            rows_by_item: Dict[str, list] = {c: [] for c in codes}
            for r in cursor.fetchall():
                rows_by_item.setdefault(r.ItemCode, []).append(r)

            # Construir el dict final: todos los códigos llevan las llaves
            # requeridas (con 0 si no tienen stock en esa sucursal).
            stock = {
                code: _build_stock_for_item(rows_by_item.get(code, []), db_key)
                for code in codes
            }

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
        SA.Code  AS ItemCode,
        P2.Price AS CompareAtPrice
    FROM   [@SHOPIFY_ARTICLE] SA
    LEFT   JOIN ITM1 P2
        ON  P2.ItemCode  = SA.Code
        AND P2.PriceList = ?
    WHERE  {SHOPIFY_GATE_WHERE.replace("U_Activo", "SA.U_Activo")}
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
    - VariantPrice: queda en 0.0 hasta que definan la fuente real.
    - VariantCompareAtPrice: lista configurada en SHOPIFY_COMPARE_AT_PRICE_LIST
      multiplicada por 1.16 (IVA 16%).
    Las llaves van sin espacios (PascalCase) para facilitar el consumo.
    """
    return {
        "VariantPrice":            0.0,
        "VariantCompareAtPrice":   _with_iva(row.CompareAtPrice),
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
    _, database = resolve_db(x_sap_db)

    base_params = [SHOPIFY_COMPARE_AT_PRICE_LIST]

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # ── Caso 1: un solo ItemCode ─────────────────────────────────────
            if itemCode:
                cursor.execute(
                    _PRICES_SELECT + " AND SA.Code = ?",
                    base_params + [itemCode],
                )
                row = cursor.fetchone()
                if not row:
                    return err(
                        404,
                        f"ItemCode '{itemCode}' no existe en @SHOPIFY_ARTICLE "
                        f"o tiene U_Activo='N'.",
                    )
                return {
                    "success": True,
                    "message": None,
                    "prices":  { row.ItemCode: _build_prices(row) },
                }

            # ── Caso 2: listado paginado ─────────────────────────────────────
            cursor.execute(
                f"SELECT COUNT(*) FROM [@SHOPIFY_ARTICLE] WHERE {SHOPIFY_GATE_WHERE}"
            )
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                _PRICES_SELECT
                + " ORDER BY SA.Code OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
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
