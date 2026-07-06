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

# Base del API de CM (".../api/cm"), derivada de CM_ORDERS_URL para respetar el
# override del .env. De aquí cuelgan appointments, v2/appointments, users, etc.
CM_API_BASE = CM_ORDERS_URL.rsplit("/orders", 1)[0]


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

    def _num(x):
        try:
            return float(x) if x is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _line(d, is_labor=False):
        """Normaliza un estimate (part o labor) de CM a {sku,name,quantity,unitPrice,total}.
        El SKU confiable es `partNumber` (el `partId` a veces viene null)."""
        sku  = (d.get("partNumber") or d.get("partId") or "") or ""
        name = d.get("partName") or d.get("laborName") or ""
        if is_labor:
            qty   = _num(d.get("laborHours") or d.get("quantity") or 1)
            price = _num(d.get("laborHourPrice") or d.get("partUnitPrice"))
        else:
            qty   = _num(d.get("quantity") or 1)
            price = _num(d.get("partUnitPrice"))
        return {
            "sku":       str(sku),
            "name":      name,
            "quantity":  qty,
            "unitPrice": round(price, 2),
            "total":     round(qty * price, 2),
        }

    items = []
    for it in (data.get("inspectionItems") or []):
        if not isinstance(it, dict):
            continue
        parts  = it.get("parts")  or []
        labors = it.get("labors") or []
        # Líneas normalizadas (para el desglose por sección en el portal).
        lines = [_line(p, False) for p in parts  if isinstance(p, dict)]
        lines += [_line(l, True)  for l in labors if isinstance(l, dict)]
        # Total del punto = suma de sus estimates (parts + labors): qty × precio.
        total = _num(it.get("quantity")) * _num(it.get("partUnitPrice"))
        total += _num(it.get("laborHours")) * _num(it.get("laborHourPrice"))
        for p in parts:
            if isinstance(p, dict):
                total += _num(p.get("quantity") or 1) * _num(p.get("partUnitPrice"))
                total += _num(p.get("laborHours")) * _num(p.get("laborHourPrice"))
        for l in labors:
            if isinstance(l, dict):
                total += _num(l.get("laborHours") or l.get("quantity") or 1) * _num(l.get("laborHourPrice"))
        items.append({
            "id":             it.get("cmosInspectionItemId"),
            "name":           it.get("inspectionItemName"),
            "priority":       it.get("priority"),          # Low | Med | Urgent → color en el front
            "approvalStatus": it.get("approvalStatus"),    # Pending | Approved | Rejected
            "comments":       it.get("comments") or it.get("inspectionItemComments") or "",
            "total":          round(total, 2),             # suma de los artículos del punto
            "lines":          lines,                       # [{sku,name,quantity,unitPrice,total}]
            "parts":          parts,
            "labors":         labors,
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


@router.post(
    "/orders/{folio}/inspection/{item_id}/estimates",
    summary="Agrega artículos (estimates) a un punto de inspección existente en CM",
)
def add_inspection_estimates(
    folio:        str,
    item_id:      int,
    repairShopId: int  = Body(..., embed=True),
    estimates:    list = Body(default=[], embed=True, description="Artículos/kits {itemCode,name,quantity,unitPrice}"),
):
    """Empuja los artículos de una oferta a un punto de inspección que YA existe en CM.
    CM sólo acepta un estimate por llamada (postInspectionItemEstimate), así que
    iteramos: 1 POST por artículo. Se usa al 'Cotizar' un punto traído de CM."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")

    items = _estimates_for_cm(estimates)
    if not items:
        return err(400, "No hay artículos que agregar al punto de inspección.")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    base_url = (
        f"{CM_ORDERS_URL}/{urllib.parse.quote(str(folio))}"
        f"/inspectionItems/{int(item_id)}/estimates?workshopId={guid}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    added = 0
    errors: list = []
    for e in items:
        status, resp = _http_post_json(base_url, e, headers)
        if status in (200, 201):
            added += 1
        else:
            detail = resp
            try:
                detail = json.loads(resp).get("message", resp)
            except Exception:
                pass
            errors.append(f"{e.get('estimateName') or e.get('partId')}: HTTP {status} {detail}")

    if added == 0:
        return err(502, "ClearMechanic rechazó los artículos: " + " | ".join(errors[:3]))
    return {"success": True, "message": None, "added": added, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# CITAS (appointments) de ClearMechanic
#   - GET  /appointments        → lista citas del taller (por rango de fecha)
#   - POST /appointments        → crea una cita (API v2: cliente + vehículo inline)
#   - GET  /serviceAdvisors     → asesores del taller (para el selector del form)
# El workshopId (GUID) se resuelve del repairShopId numérico, igual que inspección.
# ─────────────────────────────────────────────────────────────────────────────

def _appointment_view(a: dict) -> dict:
    """Curado de los campos útiles de una cita de CM para el portal."""
    return {
        "appointmentNumber": a.get("appointmentNumber"),
        "status":            a.get("status"),
        "startDate":         a.get("startDate"),
        "deliveryDate":      a.get("deliveryDate"),
        "duration":          a.get("duration"),
        "customerId":        a.get("customerId"),
        "firstName":         a.get("firstName"),
        "lastName":          a.get("lastName"),
        "email":             a.get("email"),
        "mobile":            a.get("mobile"),
        "vehicleId":         a.get("vehicleId"),
        "brand":             a.get("brand"),
        "model":             a.get("model"),
        "year":              a.get("year"),
        "vin":               a.get("vin"),
        "licensePlate":      a.get("licensePlate"),
        "color":             a.get("color"),
        "serviceAdvisor":    a.get("serviceAdvisor"),
        "serviceAdvisorId":  a.get("serviceAdvisorId"),
        "observations":      a.get("observations") or "",
        "orderNumber":       a.get("orderNumber") or "",
        "creationSource":    a.get("creationSource"),
    }


@router.get(
    "/appointments",
    summary="Lista las citas (appointments) de un taller en ClearMechanic",
)
def list_cm_appointments(
    repairShopId: int,
    dateFrom:     str,
    dateTo:       Optional[str] = None,
    status:       Optional[str] = None,
    vin:          Optional[str] = None,
    licensePlate: Optional[str] = None,
    page:         int = 1,
    pageSize:     int = 50,
):
    """GET /cm/appointments?workshopId=<GUID>&dateFrom=&dateTo=… (dateFrom obligatorio)."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")
    if not dateFrom:
        return err(400, "dateFrom es obligatorio (formato YYYY-MM-DD).")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    params = {"workshopId": guid, "dateFrom": dateFrom, "page": page, "pageSize": pageSize}
    if dateTo:       params["dateTo"] = dateTo
    if status:       params["status"] = status
    if vin:          params["vin"] = vin
    if licensePlate: params["licensePlate"] = licensePlate

    url = f"{CM_API_BASE}/appointments?{urllib.parse.urlencode(params)}"
    st, body = _http_get_json(url, {"Authorization": f"Bearer {token}"})
    if st not in (200, 201):
        detail = body
        try:
            detail = json.loads(body).get("message", body)
        except Exception:
            pass
        return err(502, f"ClearMechanic rechazó la consulta de citas (HTTP {st}): {detail}")

    try:
        data = json.loads(body).get("data", {}) or {}
    except Exception:
        return err(502, "Respuesta inválida de ClearMechanic (citas).")

    raw = data.get("appointments") or data.get("data") or []
    appts = [_appointment_view(a) for a in raw if isinstance(a, dict)]
    return {
        "success": True, "message": None,
        "data": {
            "page":         data.get("page", page),
            "pageSize":     data.get("pageSize", pageSize),
            "total":        data.get("totalNumberOfRecords", len(appts)),
            "appointments": appts,
        },
    }


@router.post(
    "/appointments",
    summary="Crea una cita (appointment) en ClearMechanic vía API v2",
)
def create_cm_appointment(
    repairShopId:     int           = Body(..., embed=True),
    startDate:        str           = Body(..., embed=True, description="ISO 8601, ej. 2026-07-10T15:30:00Z"),
    customer:         dict          = Body(..., embed=True, description="{firstName, mobile, lastName?, email?}"),
    vehicle:          dict          = Body(default={}, embed=True, description="{brand,model,year,vin,licensePlate,color}"),
    customReasons:    list          = Body(default=[], embed=True,
                                           description="[{customReasonId, customReasonDetailId?}] — Roma exige al menos uno"),
    duration:         Optional[int] = Body(default=None, embed=True, description="Minutos"),
    observations:     Optional[str] = Body(default=None, embed=True),
    serviceAdvisorId: Optional[str] = Body(default=None, embed=True),
    sendReminder:     bool          = Body(default=False, embed=True),
    orderNumber:      Optional[str] = Body(default=None, embed=True,
                                           description="ODS SAP; V2 no liga por campo, se estampa en observations"),
):
    """POST /cm/v2/appointments?workshopId=<GUID>. V2 acepta cliente y vehículo
    inline (no hay que pre-crear customer/vehicle). serviceAdvisorId y motivos son
    opcionales. Si viene orderNumber se estampa como 'ODS #<n>' en observations
    (V2 no tiene campo orderNumber para ligar a la orden SAP)."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")

    cust = customer or {}
    first  = (cust.get("firstName") or "").strip()
    mobile = (cust.get("mobile") or "").strip()
    if not first or not mobile:
        return err(400, "La cita requiere al menos el nombre y el celular del cliente.")
    if not startDate:
        return err(400, "startDate es obligatorio (ISO 8601).")

    # El vehículo necesita VIN o (placa + marca + modelo). Lo valida también CM,
    # pero avisamos claro antes de gastar el POST.
    veh = vehicle or {}
    _vin = str(veh.get("vin") or "").strip()
    _plate, _brand, _model = (str(veh.get(k) or "").strip() for k in ("licensePlate", "brand", "model"))
    if not _vin and not (_plate and _brand and _model):
        return err(400, "El vehículo requiere VIN, o bien placa + marca + modelo.")

    # customReasons: CM (Roma) exige al menos un motivo. Normalizamos al formato
    # PLANO que espera el POST: {customReasonId, customReasonDetailId?}.
    reasons = []
    for r in (customReasons or []):
        if not isinstance(r, dict):
            continue
        rid = r.get("customReasonId") or r.get("reasonId")
        if not rid:
            continue
        entry = {"customReasonId": str(rid)}
        did = r.get("customReasonDetailId") or r.get("detailId")
        if did:
            entry["customReasonDetailId"] = str(did)
        reasons.append(entry)
    if not reasons:
        return err(400, "La cita requiere al menos un motivo (customReasons).")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    obs = (observations or "").strip()
    if orderNumber:
        tag = f"ODS #{str(orderNumber).strip()}"
        obs = f"{obs} · {tag}" if obs else tag

    payload = {
        "customer": {
            "firstName": first,
            "lastName":  str(cust.get("lastName") or ""),
            "mobile":    mobile,
            "email":     str(cust.get("email") or ""),
        },
        "vehicle": {
            "vin":          _vin,
            "licensePlate": _plate,
            "brand":        _brand,
            "model":        _model,
            "year":         str(veh.get("year") or ""),
            "color":        str(veh.get("color") or ""),
        },
        "startDate":     str(startDate),
        "sendReminder":  bool(sendReminder),
        "customReasons": reasons,
    }
    if duration:
        payload["duration"] = int(duration)
    if obs:
        payload["observations"] = obs
    if serviceAdvisorId:
        payload["serviceAdvisorId"]  = str(serviceAdvisorId)
        payload["userWhoScheduleId"] = str(serviceAdvisorId)

    url = f"{CM_API_BASE}/v2/appointments?workshopId={guid}"
    st, resp = _http_post_json(url, payload, {"Authorization": f"Bearer {token}"})
    if st in (200, 201):
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
    return err(502, f"ClearMechanic rechazó la cita (HTTP {st}): {detail}", {"sentPayload": payload})


@router.get(
    "/serviceAdvisors",
    summary="Asesores de servicio (users role ServiceAdvisor) de un taller",
)
def list_cm_service_advisors(repairShopId: int):
    """GET /cm/users?workshopId=<GUID>&role=ServiceAdvisor. Para el selector de asesor."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    params = {"workshopId": guid, "role": "ServiceAdvisor", "pageSize": 200}
    url = f"{CM_API_BASE}/users?{urllib.parse.urlencode(params)}"
    st, body = _http_get_json(url, {"Authorization": f"Bearer {token}"})
    if st not in (200, 201):
        detail = body
        try:
            detail = json.loads(body).get("message", body)
        except Exception:
            pass
        return err(502, f"ClearMechanic rechazó la consulta de asesores (HTTP {st}): {detail}")

    try:
        data = json.loads(body).get("data", {}) or {}
    except Exception:
        return err(502, "Respuesta inválida de ClearMechanic (asesores).")

    raw = data.get("data") or data.get("users") or []
    advisors = [{
        "userId":   u.get("userId"),
        "userName": u.get("userName"),
        "email":    u.get("email"),
        "isActive": u.get("isActive"),
    } for u in raw if isinstance(u, dict) and u.get("userId")]
    return {"success": True, "message": None, "data": {"advisors": advisors}}


@router.get(
    "/customReasons",
    summary="Catálogo de motivos de cita (customReasons) de un taller",
)
def list_cm_custom_reasons(repairShopId: int):
    """GET /cm/customReasons?workshopId=<GUID>. Motivos con sus detalles, para el
    selector del formulario de cita. Roma exige elegir al menos uno al crear."""
    if not CM_USER or not CM_PASSWORD:
        return err(500, "ClearMechanic no está configurado.")
    guid = _WORKSHOP_GUID_BY_SHOP.get(int(repairShopId))
    if not guid:
        return err(400, f"El taller {repairShopId} no tiene workshopId (GUID) configurado.")

    token = _cm_login()
    if not token:
        return err(502, "No se pudo autenticar en ClearMechanic.")

    url = f"{CM_API_BASE}/customReasons?workshopId={guid}"
    st, body = _http_get_json(url, {"Authorization": f"Bearer {token}"})
    if st not in (200, 201):
        detail = body
        try:
            detail = json.loads(body).get("message", body)
        except Exception:
            pass
        return err(502, f"ClearMechanic rechazó la consulta de motivos (HTTP {st}): {detail}")

    try:
        parsed = json.loads(body)
    except Exception:
        return err(502, "Respuesta inválida de ClearMechanic (motivos).")

    # /cm/customReasons devuelve la lista directamente en `data` (no anidada).
    raw = parsed.get("data") if isinstance(parsed, dict) else parsed
    if not isinstance(raw, list):
        raw = (raw or {}).get("data") if isinstance(raw, dict) else []
    reasons = []
    for r in (raw or []):
        if not isinstance(r, dict):
            continue
        reasons.append({
            "reasonId":    r.get("reasonId"),
            "description": r.get("description"),
            "details": [
                {"detailId": d.get("detailId"), "description": d.get("description")}
                for d in (r.get("details") or []) if isinstance(d, dict)
            ],
        })
    return {"success": True, "message": None, "data": {"reasons": reasons}}
