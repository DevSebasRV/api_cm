"""
Integración con ClearMechanic (openapi.somosclear.com).

Replica el flujo del script del jefe (middleware*.py), pero como endpoint:
  1. Consulta la ODS por folio en SQL Server (vía ODBC). NOTA: NO usamos el SP
     SP_POST_CM porque quedó a medio editar (parámetro y WHERE comentados). En
     su lugar replicamos su mapeo de columnas en _ODS_SELECT, parametrizado.
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
import urllib.parse
import pyodbc

from app.config import EMPRESAS, CM_LOGIN_URL, CM_ORDERS_URL, CM_USER, CM_PASSWORD
from app.database import get_connection

router = APIRouter(prefix="/clearmechanic", tags=["ClearMechanic"])


# Fase inicial de CM por TALLER. OJO: el phaseId es DISTINTO en cada taller,
# aunque la fase se llame igual ("Esperando Rampa"). Mapea repairShopId → {status
# SAP : phaseId de CM}. Para agregar un taller nuevo, saca sus IDs con:
#   GET /api/cm/phases?workshopId=<GUID del taller>
# (el GUID aparece en el mensaje de error si mandas un phaseId inválido).
_PHASE_BY_SHOP = {
    4105: {"21": "22212"},   # Roma — status SAP 21 = "Esperando Rampa"
}

# Fase de ENTRADA por taller. Una orden recién creada en el portal entra en esta
# fase sin importar su status SAP (el form crea con status -3 "Abierto" por
# defecto, que no tiene override). El phaseId es distinto en cada taller.
#   Roma (4105) → 22212 "Esperando Rampa".
# Saca los phaseId de un taller con GET /api/cm/phases?workshopId=<GUID>.
_DEFAULT_PHASE_BY_SHOP = {
    4105: "22212",   # Roma — "Esperando Rampa"
}

# workshopId (GUID) por taller. El POST/PATCH de inspectionItems exige el GUID en
# query (NO el repairShopId numérico). Para agregar un taller, saca su GUID con
# GET /api/cm/phases?workshopId=<GUID> (aparece en el error si mandas fase inválida).
_WORKSHOP_GUID_BY_SHOP = {
    4105: "5950971e-41c4-4202-bf54-8b4514768163",   # Roma
}

# Color del portal → priority de CM. Rojo=Urgent, Amarillo=Med, Verde=Low.
_PRIORITY_VALUES = {"Low", "Med", "Urgent"}


def _resolve_phase(repair_shop_id: int, status_raw: str) -> Optional[str]:
    """phaseId de CM para (taller, status SAP).
    1) override explícito por status en _PHASE_BY_SHOP, si existe;
    2) si no, la fase de ENTRADA del taller (_DEFAULT_PHASE_BY_SHOP);
    3) None solo si el taller no tiene NINGUNA fase configurada."""
    sid = int(repair_shop_id)
    override = _PHASE_BY_SHOP.get(sid, {}).get(str(status_raw))
    if override is not None:
        return override
    return _DEFAULT_PHASE_BY_SHOP.get(sid)


def _to_int_or_none(v: Any) -> Optional[int]:
    """CM exige 'year' entero. Convierte; si no es numérico (ej. 'TEST'), None."""
    try:
        return int(str(v).strip())
    except (ValueError, TypeError, AttributeError):
        return None


# Consulta de datos de la ODS para armar el JSON de CM.
#
# Reemplaza a `{CALL SP_POST_CM(?)}`: ese SP del jefe quedó a medio editar
# (el parámetro @DocNum y el WHERE están comentados → "has no parameters" y sin
# filtro devuelve TODO el historial). Aquí replicamos EXACTAMENTE su mapeo de
# columnas 0..17, pero:
#   - filtramos por callID = ? (la ODS específica),
#   - usamos LEFT JOIN (no INNER) para que una orden sin tarjeta de equipo
#     (OINS), sin técnico (OHEM) o sin tipo de problema (OSCP) NO se caiga,
#   - protegemos el concat de firstName contra NULL.
# Las columnas de vehículo (brand/model/year/vin/placa) salen de la tarjeta de
# equipo; si la orden no tiene equipo, llegan vacías a CM.
_ODS_SELECT = """
    SELECT
        T0.callID                                       AS orderNumber,   -- 00
        T0.custmrName + ISNULL(' ' + T5.ExtEmpNo, '')   AS firstName,     -- 01
        T0.custmrName                                   AS lastName,      -- 02
        T1.IntrntSite                                   AS email,         -- 03
        T1.Cellular                                     AS mobile,        -- 04
        T1.Cellular                                     AS mainPhone,     -- 05
        T3.street                                       AS brand,         -- 06
        T3.StreetNo                                     AS model,         -- 07
        T3.city                                         AS [year],        -- 08
        T0.U_KM                                         AS kilometers,    -- 09
        T0.internalSN                                   AS vin,           -- 10
        T3.county                                       AS licensePlate,  -- 11
        ''                                              AS towerNumber,   -- 12
        ''                                              AS utsSold,       -- 13
        T4.[Name]                                       AS orderType,     -- 14
        T0.status                                       AS [status],      -- 15
        ''                                              AS total,         -- 16
        T4.[Name]                                       AS serviceType    -- 17
    FROM OSCL T0
        LEFT JOIN OCRD T1 ON T0.customer   = T1.CardCode
        LEFT JOIN OINS T3 ON T0.insID      = T3.insID
        LEFT JOIN OSCP T4 ON T0.problemTyp = T4.prblmTypID
        LEFT JOIN OHEM T5 ON T0.technician = T5.empID
    WHERE T0.callID = ?
"""


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


def _http_get_json(url: str, headers: Optional[dict] = None):
    """GET con urllib. Devuelve (status_code, texto_respuesta)."""
    h = dict(headers or {})
    req = urllib.request.Request(url, headers=h, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        return 0, str(e)


def _http_patch_json(url: str, payload: dict, headers: Optional[dict] = None):
    """PATCH JSON con urllib. Devuelve (status_code, texto_respuesta)."""
    data = json.dumps(payload).encode("utf-8")
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method="PATCH")
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


def _build_order_json(row, phase: str) -> dict:
    """
    Arma el JSON de la orden. El orden de las columnas es EXACTO al del script
    del jefe (índices 0..17). `phase` ya viene resuelto por taller.
    """
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
    # Número de cita de ClearMechanic: liga la orden a una cita existente en CM.
    appointmentNumber: Optional[str] = Body(default=None, embed=True, description="Número de cita de CM a ligar"),
    # Datos del vehículo que captura el portal. En SAP no se guardan de forma
    # confiable (campos no escribibles vía SL), por eso viajan directo a CM.
    brand:        Optional[str] = Body(default=None, embed=True, description="Marca de la moto"),
    model:        Optional[str] = Body(default=None, embed=True, description="Modelo"),
    year:         Optional[str] = Body(default=None, embed=True, description="Año"),
    licensePlate: Optional[str] = Body(default=None, embed=True, description="Placa"),
    x_sap_db:     Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    db_key = (x_sap_db or "fn").lower()
    if db_key not in EMPRESAS:
        return err(400, f"X-SAP-DB '{x_sap_db}' no válida. Usa: {list(EMPRESAS.keys())}.")

    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado (faltan CM_USER / CM_PASSWORD en .env).")

    # ── 1. Datos del ODS (consulta propia, NO el SP roto del jefe) ────────────
    try:
        conn   = get_connection(EMPRESAS[db_key])
        cursor = conn.cursor()
        try:
            cursor.execute(_ODS_SELECT, folio)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error al consultar la ODS para CM: {db_err}")

    if not rows:
        return err(404, f"No se encontró la ODS con folio '{folio}' en la base.")

    # Fase de CM según el taller. El phaseId es distinto por taller.
    status_raw = str(rows[0][15]) if rows[0][15] is not None else ""
    phase = _resolve_phase(repairShopId, status_raw)
    if phase is None:
        return err(
            400,
            f"El taller {repairShopId} no tiene fase de CM configurada. "
            f"Agrégalo en _DEFAULT_PHASE_BY_SHOP "
            f"(saca el phaseId con GET /api/cm/phases?workshopId=<GUID>).",
        )

    payload = _build_order_json(rows[0], phase)

    # Los datos de vehículo del portal tienen prioridad (en SAP no se guardan
    # de forma confiable). El VIN sí sale del SQL (OSCL.internalSN de la ODS).
    if brand:        payload["brand"]        = brand
    if model:        payload["model"]        = model
    if year:         payload["year"]         = year
    if licensePlate: payload["licensePlate"] = licensePlate

    # Número de cita de CM → liga la orden a la cita existente en ClearMechanic.
    if appointmentNumber:
        payload["appointmentNumber"] = str(appointmentNumber).strip()

    # CM exige 'year' entero. Si viene algo no numérico (del form o del SQL),
    # lo mandamos como None en vez de romper toda la orden.
    payload["year"] = _to_int_or_none(payload.get("year"))

    # ── 2. Login en CM ───────────────────────────────────────────────────────
    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic (revisa CM_USER/CM_PASSWORD).")

    # ── 3. POST de la orden ──────────────────────────────────────────────────
    url = f"{CM_ORDERS_URL}?repairShopId={repairShopId}"
    status, body = _http_post_json(
        url, payload, {"Authorization": f"Bearer {token}"}
    )

    if status in (200, 201):
        # CM responde 201 Created en éxito. Devuelve algo con un id si viene.
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


@router.get(
    "/orders/{folio}/inspection",
    summary="Puntos de inspección (inspectionItems) de una orden en ClearMechanic",
)
def get_cm_inspection(folio: str, repairShopId: int):
    """Consulta GET /cm/orders/{folio}?repairShopId=X en CM y devuelve sus
    puntos de inspección + contadores de color. El orderNumber de CM = CallID SAP.
    Si la orden aún no existe en CM (o sin inspección), devuelve items vacío."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado (faltan CM_USER / CM_PASSWORD en .env).")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    url = f"{CM_ORDERS_URL}/{urllib.parse.quote(str(folio))}?repairShopId={int(repairShopId)}"
    status, body = _http_get_json(url, {"Authorization": f"Bearer {token}"})

    # La orden todavía no está en CM (no sincronizada / sin inspección hecha).
    if status == 404:
        return {
            "success": True, "message": None,
            "data": {"orderNumber": str(folio), "notInCm": True,
                     "inspectionFormStatus": None,
                     "counts": {"green": 0, "yellow": 0, "red": 0}, "items": []},
        }

    if status not in (200, 201):
        detail = body
        try:
            detail = json.loads(body).get("message", body)
        except Exception:
            pass
        return err(502, f"ClearMechanic rechazó la consulta (HTTP {status}): {detail}")

    try:
        data = json.loads(body).get("data", {}) or {}
    except Exception:
        return err(502, "Respuesta inválida de ClearMechanic.")

    items = []
    for it in (data.get("inspectionItems") or []):
        if not isinstance(it, dict):
            continue
        items.append({
            "id":             it.get("cmosInspectionItemId"),
            "name":           it.get("inspectionItemName"),
            "priority":       it.get("priority"),          # Low | Med | Urgent → color en el front
            "approvalStatus": it.get("approvalStatus"),    # Pending | Approved | Rejected
            "comments":       it.get("comments") or it.get("inspectionItemComments") or "",
            "quantity":       it.get("quantity"),
            "partUnitPrice":  it.get("partUnitPrice"),
            "laborHours":     it.get("laborHours"),
            "laborHourPrice": it.get("laborHourPrice"),
            "parts":          it.get("parts") or [],
            "labors":         it.get("labors") or [],
        })

    return {
        "success": True, "message": None,
        "data": {
            "orderNumber":          str(data.get("orderNumber") or folio),
            "inspectionFormStatus": data.get("inspectionFormStatus"),
            "notInCm":              False,
            "counts": {
                "green":  data.get("greenItemsCount") or 0,
                "yellow": data.get("yellowItemsCount") or 0,
                "red":    data.get("redItemsCount") or 0,
            },
            "items": items,
        },
    }


def _estimates_for_cm(estimates) -> list:
    """Mapea los artículos/kits del portal al formato estimates[] de CM."""
    out = []
    for e in (estimates or []):
        if not isinstance(e, dict):
            continue
        price = e.get("unitPrice")
        if price is None:
            price = e.get("UnitPrice", 0)
        out.append({
            "estimateName": e.get("estimateName") or e.get("name") or e.get("itemCode") or "",
            "partId":       e.get("partId") or e.get("itemCode") or "",
            "quantity":     int(e.get("quantity") or 1),
            "UnitPrice":    float(price or 0),
        })
    return out


@router.post(
    "/orders/{folio}/inspection",
    summary="Crea un punto de inspección (inspectionItem) en ClearMechanic",
)
def create_inspection_item(
    folio:          str,
    repairShopId:   int           = Body(..., embed=True, description="ID numérico del taller"),
    name:           str           = Body(..., embed=True, description="Nombre del punto de inspección"),
    priority:       str           = Body(..., embed=True, description="Low | Med | Urgent (color)"),
    approvalStatus: str           = Body(default="Pending", embed=True),
    comments:       Optional[str] = Body(default=None, embed=True),
    estimates:      list          = Body(default=[], embed=True, description="Artículos/kits {itemCode,name,quantity,unitPrice}"),
):
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado (faltan CM_USER / CM_PASSWORD).")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado en _WORKSHOP_GUID_BY_SHOP.")
    if priority not in _PRIORITY_VALUES:
        return err(400, f"priority inválido '{priority}'. Usa: Low | Med | Urgent.")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    body = {
        "inspectionItemName": str(name),
        "priority":           priority,
        "approvalStatus":     approvalStatus or "Pending",
        "isVisible":          True,
        "estimates":          _estimates_for_cm(estimates),
    }
    if comments:
        body["comments"] = [str(comments)]

    url = f"{CM_ORDERS_URL}/{urllib.parse.quote(str(folio))}/inspectionItems?workshopId={guid}"
    status, resp = _http_post_json(url, body, {"Authorization": f"Bearer {token}"})
    if status in (200, 201):
        data = None
        try:
            data = json.loads(resp).get("data")
        except Exception:
            pass
        return {"success": True, "message": None, "data": data}

    detail = resp
    try:
        detail = json.loads(resp).get("message", resp)
    except Exception:
        pass
    return err(502, f"ClearMechanic rechazó el punto de inspección (HTTP {status}): {detail}")


@router.patch(
    "/orders/{folio}/inspection/{item_id}",
    summary="Edita un punto de inspección (inspectionItem) en ClearMechanic",
)
def patch_inspection_item(
    folio:          str,
    item_id:        int,
    repairShopId:   int           = Body(..., embed=True),
    name:           Optional[str] = Body(default=None, embed=True),
    priority:       Optional[str] = Body(default=None, embed=True),
    approvalStatus: Optional[str] = Body(default=None, embed=True),
    comments:       Optional[str] = Body(default=None, embed=True),
):
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")
    if priority is not None and priority not in _PRIORITY_VALUES:
        return err(400, f"priority inválido '{priority}'. Usa: Low | Med | Urgent.")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    body: dict = {}
    if name is not None:           body["inspectionItemName"] = str(name)
    if priority is not None:       body["priority"] = priority
    if approvalStatus is not None: body["approvalStatus"] = approvalStatus
    if comments is not None:       body["comments"] = [str(comments)]
    if not body:
        return err(400, "No hay cambios que actualizar.")

    url = f"{CM_ORDERS_URL}/{urllib.parse.quote(str(folio))}/inspectionItems/{int(item_id)}?workshopId={guid}"
    status, resp = _http_patch_json(url, body, {"Authorization": f"Bearer {token}"})
    if status in (200, 201, 204):
        return {"success": True, "message": None}

    detail = resp
    try:
        detail = json.loads(resp).get("message", resp)
    except Exception:
        pass
    return err(502, f"ClearMechanic rechazó la edición (HTTP {status}): {detail}")
