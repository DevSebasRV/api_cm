"""
Integración con ClearMechanic (openapi.somosclear.com).

Replica el flujo del script del jefe (middleware*.py), pero como endpoint:
  1. SP_POST_CM(folio)  → datos del ODS desde SQL Server (vía ODBC)
  2. login en CM        → accessToken
  3. POST /cm/orders?repairShopId=XXXX  con el JSON armado

El `repairShopId` se asocia al USUARIO en el portal; el portal lo manda en el
body. Las credenciales viven en el .env del servidor (config.py).

Usa urllib (stdlib) para no agregar dependencias (no hay `requests`/`httpx`).
"""

from fastapi import APIRouter, Header, Body
from fastapi.responses import JSONResponse
from typing import Optional, Any
from decimal import Decimal
import datetime
import json
import urllib.request
import urllib.error
import pyodbc

from app.config import EMPRESAS, CM_LOGIN_URL, CM_ORDERS_URL, CM_USER, CM_PASSWORD
from app.database import get_connection

router = APIRouter(prefix="/clearmechanic", tags=["ClearMechanic"])


# Mapeo de status SAP → phase de CM (igual que el script del jefe: 21 → 18807)
_PHASE_MAP = {"21": "18807"}


def err(status: int, message: str, extra: Optional[dict] = None):
    body = {"success": False, "message": message, "data": None}
    if extra:
        body.update(extra)
    return JSONResponse(status_code=status, content=body)


def _jsonable(v: Any) -> Any:
    """Convierte valores de pyodbc a tipos serializables por JSON."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def _http_post_json(url: str, payload: dict, headers: Optional[dict] = None):
    """POST JSON con urllib. Devuelve (status_code, texto_respuesta)."""
    data = json.dumps(payload).encode("utf-8")
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        return 0, str(e)


def _cm_login() -> Optional[str]:
    """Autentica en CM y devuelve el accessToken (o None)."""
    status, body = _http_post_json(
        CM_LOGIN_URL, {"email": CM_USER, "password": CM_PASSWORD}
    )
    if status == 200:
        try:
            return json.loads(body).get("accessToken")
        except Exception:
            return None
    return None


def _build_order_json(row) -> dict:
    """
    Arma el JSON de la orden a partir del resultado de SP_POST_CM.
    El orden de las columnas es EXACTO al del script del jefe (índices 0..17).
    """
    status_raw = str(row[15]) if row[15] is not None else ""
    phase = _PHASE_MAP.get(status_raw, status_raw)
    return {
        "orderNumber":  _jsonable(row[0]),
        "firstName":    _jsonable(row[1]),
        "lastName":     _jsonable(row[2]),
        "email":        _jsonable(row[3]),
        "mobile":       _jsonable(row[4]),
        "mainPhone":    _jsonable(row[5]),
        "brand":        _jsonable(row[6]),
        "model":        _jsonable(row[7]),
        "year":         _jsonable(row[8]),
        "kilometers":   _jsonable(row[9]),
        "vin":          _jsonable(row[10]),
        "licensePlate": _jsonable(row[11]),
        "towerNumber":  _jsonable(row[12]),
        "utsSold":      _jsonable(row[13]),
        "orderType":    _jsonable(row[14]),
        "phase":        phase,
        "total":        _jsonable(row[16]),
        "serviceType":  _jsonable(row[17]),
    }


@router.post(
    "/orders",
    summary="Crea la orden en ClearMechanic a partir de un folio de SAP (SP_POST_CM)",
)
def create_cm_order(
    folio:        str           = Body(..., embed=True, description="Número de ODS / folio en SAP"),
    repairShopId: int           = Body(..., embed=True, description="ID del taller en CM (asociado al usuario)"),
    x_sap_db:     Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    db_key = (x_sap_db or "fn").lower()
    if db_key not in EMPRESAS:
        return err(400, f"X-SAP-DB '{x_sap_db}' no válida. Usa: {list(EMPRESAS.keys())}.")

    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado (faltan CM_USER / CM_PASSWORD en .env).")

    # ── 1. Datos del ODS vía SP_POST_CM ──────────────────────────────────────
    try:
        conn   = get_connection(EMPRESAS[db_key])
        cursor = conn.cursor()
        try:
            cursor.execute("{CALL SP_POST_CM(?)}", folio)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error al ejecutar SP_POST_CM: {db_err}")

    if not rows:
        return err(404, f"SP_POST_CM no devolvió datos para el folio '{folio}'.")

    payload = _build_order_json(rows[0])

    # ── 2. Login en CM ───────────────────────────────────────────────────────
    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic (revisa CM_USER/CM_PASSWORD).")

    # ── 3. POST de la orden ──────────────────────────────────────────────────
    url = f"{CM_ORDERS_URL}?repairShopId={repairShopId}"
    status, body = _http_post_json(
        url, payload, {"Authorization": f"Bearer {token}"}
    )

    if status == 200:
        # CM normalmente devuelve algo con un id; lo pasamos tal cual si viene.
        cm_data = None
        try:
            cm_data = json.loads(body)
        except Exception:
            pass
        return {
            "success":      True,
            "message":      None,
            "repairShopId": repairShopId,
            "orderNumber":  payload["orderNumber"],
            "cmResponse":   cm_data,
            "sentPayload":  payload,
        }

    # Error desde CM → lo devolvemos con detalle para depurar
    detail = body
    try:
        j = json.loads(body)
        detail = j.get("message", body)
    except Exception:
        pass
    return err(
        502,
        f"ClearMechanic rechazó la orden (HTTP {status}): {detail}",
        {"repairShopId": repairShopId, "sentPayload": payload},
    )
