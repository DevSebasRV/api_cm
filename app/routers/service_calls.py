"""
Endpoints para Órdenes de Servicio (Service Calls) de SAP B1.

Tablas principales:
  - OSCL  : cabecera de la llamada de servicio
  - SCL1  : actividades programadas/realizadas
  - SCL3  : refacciones (gastos) y documentos relacionados
  - OINS  : Tarjetas de Equipo del cliente (motos, etc.)
  - OCRD  : socio de negocios (cliente)
  - AOSL  : catálogo de estados
  - OPRL  : tipos de problema
  - OACL  : orígenes
  - OHEM  : técnicos / empleados

Y para los documentos vinculados (vía SCL3.ObjType + DocEntry):
  - OQUT/QUT1  : Ofertas de venta   (ObjType=23)
  - ORDR/RDR1  : Pedidos            (ObjType=17)
  - ODLN/DLN1  : Entregas           (ObjType=15)
  - OINV/INV1  : Facturas           (ObjType=13)
"""

from fastapi import APIRouter, Header, Query
from typing import Optional, Dict, Any, List
import datetime
import re
import unicodedata
import pyodbc

from app.config import PRICE_LIST_CODE
from app.database import get_connection
from app.routers.common import resolve_db, err, _pagination

router = APIRouter(tags=["Service Calls"])


# ─────────────────────────────────────────────────────────────────────────────
# Mapas auxiliares
# ─────────────────────────────────────────────────────────────────────────────

OBJ_TYPE_MAP = {
    13: "Factura",
    15: "Entrega",
    17: "Pedido",
    23: "Oferta",
}

PRIORITY_MAP = {
    "L": "Baja",
    "M": "Media",
    "H": "Alta",
}

LINE_STATUS_MAP = {
    "O": "Abierta",
    "C": "Cerrada",
}

# Estados de Service Call — fallback si OSCS no devuelve nombre.
# IMPORTANTE: los códigos por defecto en SAP B1 varían entre instalaciones.
# En Ferbel/Proshop (verificado en OSCS):
#   -3 = Abierto, -2 = Pendiente, -1 = Cerrado
# Esta tabla solo se usa cuando OSCS está vacío para ese statusID.
STATUS_MAP = {
    -3: "Abierto",
    -2: "Pendiente",
    -1: "Cerrado",
}


def _status_label(code) -> str:
    if code is None:
        return ""
    try:
        c = int(code)
    except (ValueError, TypeError):
        return str(code)
    return STATUS_MAP.get(c, f"Estado {c}")


# ─────────────────────────────────────────────────────────────────────────────
# 1) GET /serviceCalls — lista paginada con filtros
# ─────────────────────────────────────────────────────────────────────────────

_LIST_SELECT = """
    SELECT  OSCL.CallID,
            OSCL.Subject,
            OSCL.customer       AS CardCode,
            OCRD.CardName       AS CustomerName,
            OSCL.itemCode,
            OITM.ItemName       AS ItemName,
            OSCL.status,
            OSCS.Name           AS StatusName,
            OSCL.priority,
            OSCL.createDate,
            OSCL.createTime,
            OSCL.closeDate,
            -- OSCL.assignee es un USUARIO (OUSR.USERID) = el MECÁNICO real.
            -- (Verificado contra el formulario de SAP: campo "Mecánico".)
            OUSR.U_NAME         AS Tecnico,
            OINS.U_Ps_Marca     AS MotoMarca,
            OINS.U_Ps_SubMarca  AS MotoSubMarca,
            OINS.U_Ps_Modelo    AS MotoModelo,
            OINS.U_Ps_Placa     AS MotoPlaca
    FROM    OSCL
    LEFT    JOIN OCRD ON OCRD.CardCode  = OSCL.customer
    LEFT    JOIN OITM ON OITM.ItemCode  = OSCL.itemCode
    LEFT    JOIN OSCS ON OSCS.statusID  = OSCL.status
    LEFT    JOIN OUSR ON OUSR.USERID    = OSCL.assignee
    LEFT    JOIN OINS ON OINS.insID     = OSCL.insID
"""


def _build_list_row(r) -> Dict[str, Any]:
    status_code = int(r.status) if r.status is not None else None
    return {
        "CallID":        int(r.CallID),
        "Subject":       r.Subject,
        "CardCode":      r.CardCode,
        "CustomerName":  r.CustomerName,
        "ItemCode":      r.itemCode,
        "ItemName":      r.ItemName,
        "Status":        status_code,
        # Si OSCS no devuelve un nombre (estado custom sin descripción), cae al map estándar
        "StatusName":    r.StatusName or _status_label(status_code),
        "Priority":      r.priority,
        "PriorityLabel": PRIORITY_MAP.get(r.priority, r.priority or ""),
        "CreateDate":    r.createDate.isoformat() if r.createDate else None,
        "CreateTime":    int(r.createTime) if r.createTime is not None else None,
        "CloseDate":     r.closeDate.isoformat() if r.closeDate else None,
        "Tecnico":       (r.Tecnico or "").strip() or None,
        # Datos de la moto según la TARJETA DE EQUIPO (OINS vía OSCL.insID).
        # U_Ps_Modelo guarda el AÑO (así lo usa el cliente: "Modelo (Año)").
        "MotoMarca":     (r.MotoMarca or "").strip() or None,
        "MotoSubMarca":  (r.MotoSubMarca or "").strip() or None,
        "MotoModelo":    str(r.MotoModelo) if r.MotoModelo is not None else None,
        "MotoPlaca":     (r.MotoPlaca or "").strip() or None,
    }


@router.get(
    "/serviceCalls",
    summary="Lista paginada de órdenes de servicio",
)
def list_service_calls(
    cardCode: Optional[str] = Query(default=None, description="Filtra por CardCode exacto"),
    status:   Optional[int] = Query(default=None, description="Filtra por statusID (-3=Open, -2=Closed)"),
    keyword:  Optional[str] = Query(default=None, description="Búsqueda libre en Subject / CustomerName / ItemCode / ItemName"),
    sucursal: Optional[str] = Query(default=None,
        description="Limita a órdenes cuyo asesor (assignee) pertenece a esa sucursal (OUBR.Name)"),
    page:     int           = Query(default=1, ge=1),
    pageSize: int           = Query(default=20, ge=1, le=200),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)

    where_parts: List[str] = ["1=1"]
    params:      List[Any] = []

    if cardCode:
        where_parts.append("OSCL.customer = ?")
        params.append(cardCode)
    if status is not None:
        where_parts.append("OSCL.status = ?")
        params.append(status)
    if keyword:
        words = keyword.split()
        for w in words:
            like = f"%{w}%"
            # Búsqueda libre: asunto, cliente (nombre en la ODS y nombre actual en
            # OCRD), SKU/descripción del artículo y PLACA de la tarjeta de equipo.
            # Si la palabra es numérica también matchea el número de ODS (CallID) —
            # sin esto, buscar "82095" no encontraba la orden ("fallas al localizar
            # registros en páginas distintas").
            clause = (
                "(OSCL.subject LIKE ? OR OSCL.custmrName LIKE ? "
                "OR OCRD.CardName LIKE ? "
                "OR OSCL.itemCode LIKE ? OR OSCL.itemName LIKE ? "
                "OR OINS.U_Ps_Placa LIKE ?"
            )
            params_w = [like, like, like, like, like, like]
            if w.isdigit():
                clause += " OR OSCL.callID = ? OR CAST(OSCL.callID AS VARCHAR(20)) LIKE ?"
                params_w += [int(w), like]
            clause += ")"
            where_parts.append(clause)
            params += params_w

    # Sucursal: la ODS no trae sucursal propia; se usa la sucursal del ASESOR
    # DE SERVICIO, que en este SAP vive en OSCL.technician (empleado OHEM).
    # (OSCL.assignee es el MECÁNICO como usuario OUSR — no sirve para sucursal.)
    if sucursal and sucursal.strip():
        where_parts.append(
            "OSCL.technician IN (SELECT h.empID FROM OHEM h "
            "JOIN OUBR b ON b.Code = h.branch WHERE b.Name = ?)"
        )
        params.append(sucursal.strip())

    where_clause = " AND ".join(where_parts)

    # El WHERE ahora referencia OCRD/OINS, así que el COUNT usa los mismos JOINs.
    _COUNT_FROM = """
        FROM    OSCL
        LEFT    JOIN OCRD ON OCRD.CardCode  = OSCL.customer
        LEFT    JOIN OINS ON OINS.insID     = OSCL.insID
    """

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) {_COUNT_FROM} WHERE {where_clause}", params)
            total = cursor.fetchone()[0]

            offset = (page - 1) * pageSize
            cursor.execute(
                f"{_LIST_SELECT} WHERE {where_clause} "
                f"ORDER BY OSCL.CallID DESC "
                f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
                params + [offset, pageSize],
            )
            rows = [_build_list_row(r) for r in cursor.fetchall()]

            return {
                "success":      True,
                "message":      None,
                "pagination":   _pagination(page, pageSize, total),
                "serviceCalls": rows,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 1b) GET /serviceCallStatuses — catálogo de estatus (OSCS) con conteo de ODS
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/serviceCallStatuses",
    summary="Catálogo de estatus de ODS (OSCS) con conteo de órdenes por estatus",
)
def list_service_call_statuses(
    sucursal: Optional[str] = Query(default=None,
        description="Cuenta solo órdenes cuyo asesor pertenece a esa sucursal (OUBR.Name)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Los estatus de ODS son configurables en SAP (tabla OSCS; el cliente creó
    los suyos espejo de las fases de CM, ej. '02-Esperando Rampa'). Devuelve TODOS
    los estatus con su conteo en OSCL (0 si ninguno) para pintar filtros dinámicos."""
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Con sucursal, el conteo solo incluye órdenes de asesores de esa
            # sucursal (mismo criterio que el listado). El JOIN extra vive en el
            # ON para que TODOS los estatus sigan apareciendo (con 0 si aplica).
            join_suc = ""
            qparams: List[Any] = []
            if sucursal and sucursal.strip():
                join_suc = (" AND OSCL.technician IN (SELECT h.empID FROM OHEM h "
                            "JOIN OUBR b ON b.Code = h.branch WHERE b.Name = ?)")
                qparams = [sucursal.strip()]
            cursor.execute(
                f"""
                SELECT  OSCS.statusID,
                        OSCS.Name,
                        COUNT(OSCL.callID) AS Cnt
                FROM    OSCS
                LEFT    JOIN OSCL ON OSCL.status = OSCS.statusID{join_suc}
                GROUP   BY OSCS.statusID, OSCS.Name
                ORDER   BY OSCS.statusID
                """,
                qparams,
            )
            statuses = [
                {"statusID": int(r.statusID), "name": r.Name, "count": int(r.Cnt)}
                for r in cursor.fetchall()
            ]
            total = sum(s["count"] for s in statuses)
            return {"success": True, "message": None, "statuses": statuses, "total": total}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


# ─────────────────────────────────────────────────────────────────────────────
# 2) GET /serviceCalls/{call_id} — detalle completo
# ─────────────────────────────────────────────────────────────────────────────

_DETAIL_HEADER = """
    SELECT  OSCL.callID         AS CallID,
            OSCL.subject         AS Subject,
            OSCL.customer        AS CardCode,
            OSCL.BPContact       AS ContactName,
            OSCL.Telephone,
            OSCL.manufSN         AS ManufSN,
            OSCL.internalSN,
            OSCL.insID,
            OSCL.itemCode,
            OSCL.contractID,
            OSCL.status,
            OSCL.priority,
            OSCL.origin,
            OSCL.problemTyp,
            OSCL.callType,
            OSCL.assignee,
            OSCL.resolution,
            OSCL.descrption,
            OSCL.createDate,
            OSCL.createTime,
            OSCL.closeDate,
            OSCL.resolDate,
            OSCL.respByDate      AS ResponseDate,
            OSCL.respByTime,
            OCRD.CardName        AS CustomerCardName,
            OCRD.Phone1          AS CustomerPhone,
            OCRD.E_Mail          AS CustomerEmail,
            OSCS.Name            AS StatusName,
            OSCO.Name            AS OrigenName,
            OSCP.Name            AS ProblemName,
            OUSR.U_NAME          AS TecnicoName,
            LTRIM(RTRIM(ISNULL(EMP.firstName,'') + ' ' + ISNULL(EMP.lastName,''))) AS AsesorName,
            OINS.manufSN         AS EquipManufSN,
            OINS.internalSN      AS EquipInternalSN,
            OINS.U_Ps_Marca      AS EquipMarca,
            OINS.U_Ps_SubMarca   AS EquipSubMarca,
            OINS.U_Ps_Modelo     AS EquipModelo,
            OINS.U_Ps_Placa      AS EquipPlaca,
            OINS.U_Ps_Color      AS EquipColor,
            OITM.ItemName        AS ItemFullName
    FROM    OSCL
    LEFT    JOIN OCRD ON OCRD.CardCode    = OSCL.customer
    LEFT    JOIN OSCS ON OSCS.statusID    = OSCL.status
    LEFT    JOIN OSCO ON OSCO.originID    = OSCL.origin
    LEFT    JOIN OSCP ON OSCP.prblmTypID  = OSCL.problemTyp
    LEFT    JOIN OUSR ON OUSR.USERID      = OSCL.assignee
    LEFT    JOIN OHEM EMP ON EMP.empID    = OSCL.technician
    LEFT    JOIN OINS ON OINS.insID       = OSCL.insID
    LEFT    JOIN OITM ON OITM.ItemCode    = OSCL.itemCode
    WHERE   OSCL.callID = ?
"""


def _build_header(r) -> Dict[str, Any]:
    status_code = int(r.status) if r.status is not None else None
    return {
        "CallID":           int(r.CallID),
        "Subject":          r.Subject,
        "Description":      r.descrption,
        "Resolution":       r.resolution,
        "Customer": {
            "CardCode":     r.CardCode,
            "CardName":     r.CustomerCardName,
            "Phone":        r.CustomerPhone,
            "Email":        r.CustomerEmail,
            "ContactName":  r.ContactName,
            "ContactPhone": r.Telephone,
        },
        "Equipment": {
            "InsID":        int(r.insID) if r.insID else None,
            "ItemCode":     r.itemCode,
            "ItemName":     r.ItemFullName,
            "ManufSN":      r.EquipManufSN or r.ManufSN,
            "InternalSN":   r.EquipInternalSN or r.internalSN,
            # Datos de la moto según la TARJETA DE EQUIPO (U_Ps_* de OINS).
            # Modelo = AÑO (el cliente lo maneja como "Modelo (Año)").
            "Marca":        (r.EquipMarca or "").strip() or None,
            "SubMarca":     (r.EquipSubMarca or "").strip() or None,
            "Modelo":       str(r.EquipModelo) if r.EquipModelo is not None else None,
            "Placa":        (r.EquipPlaca or "").strip() or None,
            "Color":        (r.EquipColor or "").strip() or None,
        },
        "Status": {
            "Code":         status_code,
            "Label":        r.StatusName or _status_label(status_code),
        },
        "Priority":         r.priority,
        "PriorityLabel":    PRIORITY_MAP.get(r.priority, r.priority or ""),
        "Origin":           r.OrigenName,
        "ProblemType":      r.ProblemName,
        "ContractID":       int(r.contractID) if r.contractID else None,
        "Tecnico":          (r.TecnicoName or "").strip() or None,
        # Asesor de servicio = OSCL.technician (empleado OHEM), editable como el técnico.
        "Asesor":           (r.AsesorName or "").strip() or None,
        "CreateDate":       r.createDate.isoformat() if r.createDate else None,
        "CreateTime":       int(r.createTime) if r.createTime is not None else None,
        "CloseDate":        r.closeDate.isoformat() if r.closeDate else None,
        "ResolutionDate":   r.resolDate.isoformat() if r.resolDate else None,
        "ResponseDate":     r.ResponseDate.isoformat() if r.ResponseDate else None,
        "ResponseByTime":   int(r.respByTime) if r.respByTime is not None else None,
    }


def _fetch_solutions(cursor, call_id: int) -> List[Dict[str, Any]]:
    """SCL1 = Soluciones aplicadas (NO actividades). Solo tiene FK a OSCT (knowledge base)."""
    cursor.execute(
        """
        SELECT  SCL1.line       AS LineID,
                SCL1.solutionID,
                SCL1.createDate
        FROM    SCL1
        WHERE   SCL1.srvcCallID = ?
        ORDER BY SCL1.line
        """,
        [call_id],
    )
    return [
        {
            "LineID":     int(r.LineID),
            "SolutionID": int(r.solutionID) if r.solutionID is not None else None,
            "CreateDate": r.createDate.isoformat() if r.createDate else None,
        }
        for r in cursor.fetchall()
    ]


def _fetch_refacciones(cursor, call_id: int) -> List[Dict[str, Any]]:
    """
    SCL3 en esta BD no tiene Price/WhsCode/DocEntry/ObjType — solo ItemCode,
    cantidades y horas. Es básicamente "lo que se pidió" para la llamada.
    Los datos de facturación/almacén/precio vienen de los DOCUMENTOS vinculados.
    """
    cursor.execute(
        """
        SELECT  SCL3.Line        AS LineID,
                SCL3.ItemCode,
                SCL3.ItemName,
                SCL3.Quantity,
                SCL3.QtyToBill,
                SCL3.QtyToInv,
                SCL3.Bill,
                SCL3.HourFrom,
                SCL3.HourTo,
                SCL3.SaleUnits
        FROM    SCL3
        WHERE   SCL3.SrcvCallID = ?
        ORDER BY SCL3.Line
        """,
        [call_id],
    )
    return [
        {
            "LineID":     int(r.LineID),
            "ItemCode":   r.ItemCode,
            "ItemName":   r.ItemName,
            "Quantity":   float(r.Quantity)  if r.Quantity  is not None else 0.0,
            "QtyToBill":  float(r.QtyToBill) if r.QtyToBill is not None else 0.0,
            "QtyToInv":   float(r.QtyToInv)  if r.QtyToInv  is not None else 0.0,
            "Bill":       r.Bill,
            "HourFrom":   int(r.HourFrom) if r.HourFrom is not None else None,
            "HourTo":     int(r.HourTo)   if r.HourTo   is not None else None,
            "SaleUnits":  r.SaleUnits,
        }
        for r in cursor.fetchall()
    ]


# ── Helpers para traer cabecera + líneas de cada tipo de documento ──────────

DOC_TABLES = {
    23: ("OQUT", "QUT1", "Oferta"),     # Quotation
    17: ("ORDR", "RDR1", "Pedido"),     # Sales Order
    15: ("ODLN", "DLN1", "Entrega"),    # Delivery
    13: ("OINV", "INV1", "Factura"),    # Invoice
}


def _fetch_document(cursor, obj_type: int, doc_entry: int) -> Optional[Dict[str, Any]]:
    if obj_type not in DOC_TABLES:
        return None
    o_table, l_table, label = DOC_TABLES[obj_type]

    cursor.execute(
        f"""
        SELECT  DocEntry, DocNum, DocDate, DocStatus,
                DocTotal, DocCur, Comments, CardCode, CardName, SlpCode,
                (SELECT SlpName FROM OSLP WHERE OSLP.SlpCode = {o_table}.SlpCode) AS SlpName
        FROM    {o_table}
        WHERE   DocEntry = ?
        """,
        [doc_entry],
    )
    h = cursor.fetchone()
    if not h:
        return None

    # Solo ENTREGAS: el UDF U_PREFAC (Sí/-/No) que decide si entra al ticket
    # de prefactura. El portal lo muestra y lo puede alternar.
    prefactura = None
    if obj_type == 15:
        try:
            cursor.execute(
                "SELECT ISNULL(U_PREFAC, '-') AS p FROM ODLN WHERE DocEntry = ?",
                [doc_entry],
            )
            row = cursor.fetchone()
            prefactura = (row.p or "-").strip() if row else None
        except pyodbc.Error:
            prefactura = None

    cursor.execute(
        f"""
        SELECT  L.LineNum, L.ItemCode, L.Dscription, L.Quantity, L.Price, L.LineTotal,
                L.VatSum, L.VatPrcnt, L.PriceAfVAT, L.GTotal,
                L.LineStatus, L.WhsCode, L.TargetType, L.TrgetEntry,
                ISNULL(I.InvntItem, 'Y') AS InvntItem
        FROM    {l_table} L
                LEFT JOIN OITM I ON I.ItemCode = L.ItemCode
        WHERE   L.DocEntry = ?
        ORDER BY L.LineNum
        """,
        [doc_entry],
    )
    lines = [
        {
            "LineNum":     int(l.LineNum),
            "ItemCode":    l.ItemCode,
            "Description": l.Dscription,
            "Quantity":    float(l.Quantity)  if l.Quantity  is not None else 0.0,
            "Price":       float(l.Price)     if l.Price     is not None else 0.0,
            # LineTotal = Subtotal SIN IVA  (Qty × Price)
            "LineTotal":   float(l.LineTotal) if l.LineTotal is not None else 0.0,
            "VatSum":      float(l.VatSum)    if l.VatSum    is not None else 0.0,
            "VatPrcnt":    float(l.VatPrcnt)  if l.VatPrcnt  is not None else 0.0,
            "PriceAfVAT":  float(l.PriceAfVAT) if l.PriceAfVAT is not None else 0.0,
            # GTotal = Total CON IVA (LineTotal + VatSum). Es lo que SAP llama "Importe" en la UI.
            "GTotal":      float(l.GTotal)    if l.GTotal    is not None else 0.0,
            "LineStatus":      l.LineStatus,
            "LineStatusLabel": LINE_STATUS_MAP.get(l.LineStatus, l.LineStatus or ""),
            "WhsCode":     l.WhsCode,
            # OITM.InvntItem='N' → servicio/mano de obra: se entrega sin stock.
            "NonInventory": (l.InvntItem or "Y") != "Y",
            "StockHere":   0.0,   # Se llena después con _enrich_lines_with_stock
            "StockOther":  0.0,   # Se llena después
            "TargetType":  int(l.TargetType) if l.TargetType is not None else None,
            "TargetLabel": OBJ_TYPE_MAP.get(int(l.TargetType), None) if l.TargetType else None,
            "TargetEntry": int(l.TrgetEntry) if l.TrgetEntry is not None else None,
        }
        for l in cursor.fetchall()
    ]

    return {
        "Type":       label,
        "ObjType":    obj_type,
        "DocEntry":   int(h.DocEntry),
        "DocNum":     int(h.DocNum),
        "DocDate":    h.DocDate.isoformat() if h.DocDate else None,
        "DocStatus":  h.DocStatus,            # 'O' (abierto) | 'C' (cerrado)
        "DocStatusLabel": LINE_STATUS_MAP.get(h.DocStatus, h.DocStatus or ""),
        # Ligado DIRECTAMENTE a la orden de servicio, o solo alcanzable por la
        # cadena de documentos. Lo decide _fetch_related_documents.
        "LinkedDirect": False,
        "DocTotal":   float(h.DocTotal) if h.DocTotal is not None else 0.0,
        "DocCurrency": h.DocCur,
        "CardCode":   h.CardCode,
        "CardName":   h.CardName,
        "Comments":   h.Comments,
        "SalesPersonCode": int(h.SlpCode) if h.SlpCode is not None and int(h.SlpCode) > 0 else None,
        "SalesPersonName": ((h.SlpName or "").strip() or None) if (h.SlpCode is not None and int(h.SlpCode) > 0) else None,
        "Prefactura": prefactura,   # solo entregas; None en los demás tipos
        "Lines":      lines,
    }


SERVICE_CALL_OBJTYPE = 191    # ObjType de ServiceCalls en SAP B1


def _fetch_related_documents(
    cursor,
    call_id:     int,
    card_code:   Optional[str],
    create_date: Optional[Any],
    close_date:  Optional[Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Busca documentos REALMENTE ligados a la orden de servicio, combinando:

    1.  Linkage estándar SAP B1 — líneas en QUT1/RDR1/DLN1/INV1 con
        BaseType=191 y BaseEntry=CallID (docs creados desde la pestaña
        "Registr y Refacciones" de SAP).

    2.  SCL4 — la grilla de documentos de "Registr y Refacciones"
        (SrcvCallID → Object + DocAbs). La llena el portal vía
        ServiceCallInventoryExpenses y también SAP al ligar a mano.

    3.  Marcador del portal "ODS #<callId>" en Comments (respaldo para docs
        del portal cuya liga haya fallado o anteriores a la liga).

    NOTA: existió una "heurística por cliente + fechas" que listaba TODOS los
    documentos del cliente en el rango de la orden — con clientes de mucho
    movimiento (p.ej. intercompañía) inventaba decenas de relaciones falsas.
    Se eliminó a propósito: aquí solo entran ligas verificables.
    (card_code y close_date ya no se usan; se conservan por compatibilidad.)

    Los documentos se devuelven sin duplicados (vía set de (obj_type, doc_entry)).
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {
        "Oferta":  [],
        "Pedido":  [],
        "Entrega": [],
        "Factura": [],
    }
    seen: set = set()

    # (obj_type, tabla líneas, tabla cabecera, label)
    doc_specs = [
        (23, "QUT1", "OQUT", "Oferta"),
        (17, "RDR1", "ORDR", "Pedido"),
        (15, "DLN1", "ODLN", "Entrega"),
        (13, "INV1", "OINV", "Factura"),
    ]

    # ── Mecanismo 1: BaseType=191 en líneas (linkage estándar SAP) ──────────
    for obj_type, line_table, _, type_label in doc_specs:
        try:
            cursor.execute(
                f"""
                SELECT DISTINCT DocEntry
                FROM   {line_table}
                WHERE  BaseType = ? AND BaseEntry = ?
                """,
                [SERVICE_CALL_OBJTYPE, call_id],
            )
            for r in cursor.fetchall():
                key = (obj_type, int(r.DocEntry))
                if key in seen:
                    continue
                doc = _fetch_document(cursor, obj_type, int(r.DocEntry))
                if doc:
                    doc["LinkedDirect"] = True     # apunta a la ODS en sus líneas
                    grouped[type_label].append(doc)
                    seen.add(key)
        except pyodbc.Error:
            pass

    # ── Mecanismo 2: SCL4 — documentos ligados a la llamada ─────────────────
    # (La grilla de "Registr y Refacciones": Object = tipo, DocAbs = DocEntry.)
    _label_by_obj = {23: "Oferta", 17: "Pedido", 15: "Entrega", 13: "Factura"}
    try:
        cursor.execute(
            "SELECT DISTINCT Object, DocAbs FROM SCL4 "
            "WHERE SrcvCallID = ? AND DocAbs IS NOT NULL",
            [call_id],
        )
        for r in cursor.fetchall():
            try:
                obj_type = int(str(r.Object).strip())
            except (TypeError, ValueError):
                continue
            type_label = _label_by_obj.get(obj_type)
            if not type_label:
                continue
            key = (obj_type, int(r.DocAbs))
            if key in seen:
                continue
            doc = _fetch_document(cursor, obj_type, int(r.DocAbs))
            if doc:
                doc["LinkedDirect"] = True    # está en la grilla de la ODS en SAP
                grouped[type_label].append(doc)
                seen.add(key)
    except pyodbc.Error:
        pass

    # ── Mecanismo 3: marcador del portal en Comments ("ODS #<callId>") ───────
    # Determinístico para documentos creados desde el portal. NO depende de
    # fechas, así que liga las ofertas aunque la orden ya tenga closeDate y el
    # documento se haya creado DESPUÉS de esa fecha. Lo estampan las acciones
    # del portal (crear oferta, convertir a entrega) en Comments.
    #
    # OJO: esto NO es una liga de SAP. Un documento que solo llega hasta aquí
    # aparece por su comentario o por la cadena (oferta → entrega → factura),
    # pero NO está colgado de la orden de servicio: en SAP, la ODS no lo
    # conoce. Quedan con LinkedDirect=False y el portal los marca en rojo.
    marker_like = f"%ODS #{call_id}%"
    # Evita que, p.ej., la orden 7006 capture documentos de la 70065.
    marker_re = re.compile(rf"ODS\s*#?\s*{call_id}(?:\D|$)")
    for obj_type, _, head_table, type_label in doc_specs:
        try:
            if create_date is not None:
                cursor.execute(
                    f"""
                    SELECT DocEntry, ISNULL(Comments, '') AS Comments
                    FROM   {head_table}
                    WHERE  Comments LIKE ? AND DocDate >= ?
                    """,
                    [marker_like, create_date],
                )
            else:
                cursor.execute(
                    f"""
                    SELECT DocEntry, ISNULL(Comments, '') AS Comments
                    FROM   {head_table}
                    WHERE  Comments LIKE ?
                    """,
                    [marker_like],
                )
            for r in cursor.fetchall():
                key = (obj_type, int(r.DocEntry))
                if key in seen:
                    continue
                if not marker_re.search(r.Comments or ""):
                    continue  # descarta falsos positivos del LIKE (#7006 vs #70065)
                doc = _fetch_document(cursor, obj_type, int(r.DocEntry))
                if doc:
                    grouped[type_label].append(doc)
                    seen.add(key)
        except pyodbc.Error:
            pass

    return grouped


def _enrich_lines_with_stock(cursor, documents: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Llena StockHere y StockOther en cada línea de cada documento.
    Hace UNA sola query a OITW agrupando todos los ItemCodes únicos.

    - StockHere  = OnHand en el WhsCode de la propia línea
    - StockOther = sum(OnHand) de todos los OTROS almacenes para ese item
    """
    item_codes = set()
    for docs in documents.values():
        for doc in docs:
            for line in doc.get("Lines", []):
                if line.get("ItemCode"):
                    item_codes.add(line["ItemCode"])

    if not item_codes:
        return

    placeholders = ",".join("?" * len(item_codes))
    cursor.execute(
        f"SELECT ItemCode, WhsCode, OnHand FROM OITW WHERE ItemCode IN ({placeholders})",
        list(item_codes),
    )

    stock_map: Dict[str, Dict[str, float]] = {}
    for r in cursor.fetchall():
        stock_map.setdefault(r.ItemCode, {})[r.WhsCode] = float(r.OnHand or 0)

    for docs in documents.values():
        for doc in docs:
            for line in doc.get("Lines", []):
                item = line.get("ItemCode")
                whs  = line.get("WhsCode")
                per_whs = stock_map.get(item, {})
                line["StockHere"]  = per_whs.get(whs, 0.0) if whs else 0.0
                line["StockOther"] = sum(v for k, v in per_whs.items() if k != whs)


@router.get(
    "/itemStock",
    summary="Stock detallado de un artículo, agrupado por sucursal y almacén",
)
def get_item_stock(
    code:     str           = Query(..., description="ItemCode del artículo (puede contener /, espacios, etc.)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Recibe el ItemCode como query param para soportar slashes y caracteres especiales."""
    item_code = code
    """
    Devuelve el stock completo de un artículo:
    - Total general
    - Agrupado por sucursal (OLCT.Location)
    - Cada sucursal con sus almacenes (OWHS) y cantidades

    Usado por el modal "Ver más" en las líneas de documentos del detalle
    de Órdenes de Servicio.
    """
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Verificar que el item exista
            cursor.execute("SELECT ItemName FROM OITM WHERE ItemCode = ?", [item_code])
            row = cursor.fetchone()
            if not row:
                return err(404, f"Artículo '{item_code}' no existe en OITM.")
            item_name = row.ItemName

            cursor.execute(
                """
                SELECT  OWHS.WhsCode,
                        OWHS.WhsName,
                        OITW.OnHand,
                        OITW.IsCommited   AS Committed,
                        OITW.OnOrder,
                        COALESCE(OLCT.Location, 'SIN LOCALIDAD') AS LocationName
                FROM    OITW
                JOIN    OWHS ON OWHS.WhsCode = OITW.WhsCode
                LEFT    JOIN OLCT ON OLCT.Code = OWHS.Location
                WHERE   OITW.ItemCode = ?
                ORDER BY LocationName, OWHS.WhsName
                """,
                [item_code],
            )

            by_location: Dict[str, Dict[str, Any]] = {}
            total_onhand   = 0.0
            total_commit   = 0.0
            total_avail    = 0.0

            for r in cursor.fetchall():
                loc_raw = (r.LocationName or "SIN LOCALIDAD").strip()
                loc_key = loc_raw.upper().replace(" ", "")
                if loc_key not in by_location:
                    by_location[loc_key] = {
                        "Location":     loc_raw,
                        "TotalOnHand":  0.0,
                        "TotalAvailable": 0.0,
                        "Warehouses":   [],
                    }
                on_hand   = float(r.OnHand    or 0)
                committed = float(r.Committed or 0)
                on_order  = float(r.OnOrder   or 0)
                available = on_hand - committed

                by_location[loc_key]["Warehouses"].append({
                    "WhsCode":   r.WhsCode,
                    "WhsName":   r.WhsName,
                    "OnHand":    on_hand,
                    "Committed": committed,
                    "OnOrder":   on_order,
                    "Available": available,
                })
                by_location[loc_key]["TotalOnHand"]    += on_hand
                by_location[loc_key]["TotalAvailable"] += available

                total_onhand += on_hand
                total_commit += committed
                total_avail  += available

            return {
                "success":        True,
                "message":        None,
                "ItemCode":       item_code,
                "ItemName":       item_name,
                "TotalOnHand":    total_onhand,
                "TotalCommitted": total_commit,
                "TotalAvailable": total_avail,
                "ByLocation":     list(by_location.values()),
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/serviceCalls/{call_id}",
    summary="Detalle de una orden de servicio + documentos vinculados",
)
def get_service_call(
    call_id:  int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # 1. Cabecera
            cursor.execute(_DETAIL_HEADER, [call_id])
            h = cursor.fetchone()
            if not h:
                return err(404, f"Orden de servicio #{call_id} no encontrada.")
            header = _build_header(h)

            # 2. Soluciones aplicadas (SCL1 — knowledge base, no actividades)
            solutions = _fetch_solutions(cursor, call_id)

            # 3. Refacciones / gastos (SCL3)
            refacciones = _fetch_refacciones(cursor, call_id)

            # 4. Documentos vinculados — usa linkage estándar + heurística
            #    por cliente y rango de fechas (para SAP custom como Ferbel).
            documents = _fetch_related_documents(
                cursor,
                call_id,
                h.CardCode,
                h.createDate,
                h.closeDate,
            )

            # 5. Enriquecer cada línea con stock del propio almacén y otros
            _enrich_lines_with_stock(cursor, documents)

            return {
                "success":     True,
                "message":     None,
                "header":      header,
                "solutions":   solutions,
                "refacciones": refacciones,
                "documents":   documents,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/serviceCalls/{call_id}/prefactura",
    summary="Datos del ticket de PREFACTURA: entregas de la ODS con U_PREFAC='Si'",
)
def get_prefactura(
    call_id: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Replica los datos del ticket térmico "Ticket_Prefactura" de SAP (Crystal):
    emisor (OADM), cliente (OCRD), vehículo (OINS/OSCL: placa=internalSN,
    serie=motor=manufSN — así lo imprime el Crystal), y las ENTREGAS de la ODS
    marcadas con U_PREFAC='Si' (no canceladas NI ya facturadas), con sus
    líneas y totales.
    Las entregas se resuelven con las mismas ligas veraces del detalle:
    SCL4 + BaseType=191 + marcador "ODS #<n>" en Comments.
    """
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT customer, custmrName, manufSN, internalSN, insID "
                "FROM OSCL WHERE callID = ?", [call_id],
            )
            ods = cursor.fetchone()
            if not ods:
                return err(404, f"La orden de servicio {call_id} no existe.")

            # ── Entregas ligadas a la ODS (SCL4 ∪ BaseType 191 ∪ marcador) ──
            entries: set = set()
            cursor.execute(
                "SELECT DISTINCT DocAbs FROM SCL4 "
                "WHERE SrcvCallID = ? AND Object = '15' AND DocAbs IS NOT NULL",
                [call_id],
            )
            entries.update(int(r.DocAbs) for r in cursor.fetchall())
            cursor.execute(
                "SELECT DISTINCT DocEntry FROM DLN1 WHERE BaseType = 191 AND BaseEntry = ?",
                [call_id],
            )
            entries.update(int(r.DocEntry) for r in cursor.fetchall())
            marker_re = re.compile(rf"ODS\s*#?\s*{call_id}(?:\D|$)")
            cursor.execute(
                "SELECT DocEntry, ISNULL(Comments,'') AS c FROM ODLN WHERE Comments LIKE ?",
                [f"%ODS #{call_id}%"],
            )
            entries.update(int(r.DocEntry) for r in cursor.fetchall() if marker_re.search(r.c))

            entregas, lineas = [], []
            subtotal = iva = total = 0.0
            fecha = None
            placa_udf = ""
            if entries:
                marks = ",".join("?" * len(entries))
                # Se excluyen canceladas y las YA FACTURADAS (alguna línea
                # copiada a una Factura, TargetType 13): lo facturado ya se
                # cobró y no debe volver a salir en la prefactura.
                cursor.execute(
                    f"SELECT d.DocEntry, d.DocNum, d.DocDate, d.DocTotal, d.VatSum, "
                    f"       ISNULL(d.U_placas,'') AS placas "
                    f"FROM ODLN d WHERE d.DocEntry IN ({marks}) "
                    f"  AND ISNULL(d.U_PREFAC,'-') = 'Si' AND d.CANCELED = 'N' "
                    f"  AND NOT EXISTS (SELECT 1 FROM DLN1 t "
                    f"                  WHERE t.DocEntry = d.DocEntry AND t.TargetType = 13) "
                    f"ORDER BY d.DocNum",
                    list(entries),
                )
                heads = cursor.fetchall()
                for h in heads:
                    entregas.append(int(h.DocNum))
                    total += float(h.DocTotal or 0)
                    iva   += float(h.VatSum or 0)
                    if h.DocDate and (fecha is None or h.DocDate > fecha):
                        fecha = h.DocDate
                    if not placa_udf and (h.placas or "").strip() not in ("", "-"):
                        placa_udf = h.placas.strip()
                if heads:
                    marks2 = ",".join("?" * len(heads))
                    cursor.execute(
                        f"SELECT DocEntry, ItemCode, Dscription, Quantity, Price, LineTotal "
                        f"FROM DLN1 WHERE DocEntry IN ({marks2}) ORDER BY DocEntry, LineNum",
                        [int(h.DocEntry) for h in heads],
                    )
                    for l in cursor.fetchall():
                        qty   = float(l.Quantity or 0)
                        price = float(l.Price or 0)
                        lt    = float(l.LineTotal or 0)
                        subtotal += lt
                        lineas.append({
                            "itemCode": l.ItemCode,
                            "name":     l.Dscription,
                            "quantity": qty,
                            "price":    round(price, 2),
                            "total":    round(lt, 2),
                        })

            # ── Cliente y emisor ────────────────────────────────────────────
            cursor.execute(
                "SELECT CardName, LicTradNum, Address, Block, City, County, ZipCode "
                "FROM OCRD WHERE CardCode = ?", [(ods.customer or "").strip()],
            )
            c = cursor.fetchone()
            dir_partes = [p.strip() for p in
                          [c.Address if c else "", c.Block if c else "",
                           c.City if c else "", c.County if c else ""] if p and p.strip()]
            direccion = ",".join(dir_partes)
            if c and (c.ZipCode or "").strip():
                direccion = f"{direccion},{c.ZipCode.strip()}" if direccion else c.ZipCode.strip()

            cursor.execute("SELECT CompnyName, CompnyAddr, TaxIdNum FROM OADM")
            adm = cursor.fetchone()
            emisor_lineas = [ln.strip() for ln in str(adm.CompnyAddr or "").replace("\r\n", "\r")
                             .replace("\n", "\r").split("\r") if ln.strip()]

            return {
                "success": True, "message": None,
                "data": {
                    "ods":     call_id,
                    "fecha":   fecha.date().isoformat() if fecha else None,
                    "emisor": {
                        "nombre":    (adm.CompnyName or "").strip(),
                        "direccion": emisor_lineas,
                        "rfc":       (adm.TaxIdNum or "").strip(),
                    },
                    "cliente": {
                        "nombre":    (c.CardName if c else ods.custmrName or "").strip(),
                        "direccion": direccion,
                        "rfc":       ((c.LicTradNum if c else "") or "").strip(),
                    },
                    "vehiculo": {
                        # Mismo mapeo que el Crystal de SAP: serie y motor = manufSN.
                        "placa": placa_udf or (ods.internalSN or "").strip(),
                        "serie": (ods.manufSN or "").strip(),
                        "motor": (ods.manufSN or "").strip(),
                    },
                    "entregas": entregas,
                    "lineas":   lineas,
                    "subtotal": round(subtotal, 2),
                    "iva":      round(iva, 2),
                    "total":    round(total, 2),
                },
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/surtidoPendiente",
    summary="ODS abiertas con ofertas abiertas pendientes de surtir (módulo de Refacciones)",
)
def surtido_pendiente(
    sucursal: Optional[str] = Query(default=None,
        description="Limita a órdenes cuyo asesor pertenece a esa sucursal (OUBR.Name)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Cola de trabajo de Refacciones: ODS ABIERTAS (closeDate IS NULL) que
    tienen ofertas ABIERTAS con líneas por surtir (QUT1.LineStatus='O'),
    agrupadas por orden. La liga oferta↔ODS es la grilla SCL4 (Object='23'),
    la misma fuente veraz que usa el detalle de la orden — ofertas cuya liga
    falló (raro; quedan solo con el marcador en Comments) no aparecen."""
    _, database = resolve_db(x_sap_db)
    filtro_suc = ""
    params: List[Any] = []
    if sucursal and sucursal.strip():
        filtro_suc = (" AND s.technician IN (SELECT h.empID FROM OHEM h "
                      "JOIN OUBR b ON b.Code = h.branch WHERE b.Name = ?)")
        params.append(sucursal.strip())
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT  s.callID, s.custmrName, s.createDate,
                        LTRIM(RTRIM(ISNULL(h.firstName,'') + ' ' + ISNULL(h.lastName,''))) AS AsesorName,
                        q.DocEntry, q.DocNum, q.DocDate, q.DocTotal, q.Comments,
                        (SELECT COUNT(*) FROM QUT1 l
                         WHERE l.DocEntry = q.DocEntry AND l.LineStatus = 'O') AS LineasAbiertas
                FROM    OQUT q
                JOIN    SCL4 x ON x.[Object] = '23' AND x.DocAbs = q.DocEntry
                JOIN    OSCL s ON s.callID = x.SrcvCallID
                LEFT    JOIN OHEM h ON h.empID = s.technician
                WHERE   q.DocStatus = 'O'
                  AND   ISNULL(q.CANCELED, 'N') <> 'Y'
                  AND   s.closeDate IS NULL
                  AND   EXISTS (SELECT 1 FROM QUT1 l
                                WHERE l.DocEntry = q.DocEntry AND l.LineStatus = 'O')
                  {filtro_suc}
                ORDER BY s.callID DESC, q.DocEntry DESC
                """,
                params,
            )
            por_ods: Dict[int, Dict[str, Any]] = {}
            total_ofertas = 0
            for r in cursor.fetchall():
                ods = int(r.callID)
                grupo = por_ods.setdefault(ods, {
                    "ods":      ods,
                    "cliente":  (r.custmrName or "").strip() or None,
                    "fecha":    r.createDate.date().isoformat() if r.createDate else None,
                    "asesor":   (r.AsesorName or "").strip() or None,
                    "ofertas":  [],
                })
                grupo["ofertas"].append({
                    "docEntry":       int(r.DocEntry),
                    "docNum":         int(r.DocNum),
                    "fecha":          r.DocDate.date().isoformat() if r.DocDate else None,
                    "total":          float(r.DocTotal) if r.DocTotal is not None else 0.0,
                    "lineasAbiertas": int(r.LineasAbiertas or 0),
                })
                total_ofertas += 1

            grupos = sorted(por_ods.values(), key=lambda g: g["ods"], reverse=True)
            return {"success": True, "message": None,
                    "data": {"grupos": grupos, "totalOfertas": total_ofertas,
                             "ods": len(grupos)}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 3) GET /equipment/customer/{card_code} — tarjetas de equipo del cliente
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/lastKm",
    summary="Último kilometraje registrado de un equipo (para validar la ODS nueva)",
)
def last_km(
    insId:    Optional[int] = Query(default=None, description="insID de la tarjeta de equipo (OINS)"),
    vin:      Optional[str] = Query(default=None, description="Número de serie interno, si no hay tarjeta"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Kilometraje de la ODS más reciente de ese equipo, para que el portal no
    deje capturar uno menor.

    Se busca por TARJETA DE EQUIPO (OSCL.insID) porque es el identificador
    confiable: los VIN traen comodines históricos compartidos por cientos de
    órdenes ('REVISONDEUNIDADES', '123456789'). Si no hay tarjeta se cae al VIN,
    pero solo si parece uno real (17 caracteres) — con un comodín se devuelve
    vacío para no bloquear trabajos internos.

    `U_KM` es entero en OSCL; se usa TRY_CAST por si alguna fila trae basura."""
    if not insId and not (vin or "").strip():
        return err(400, "Falta insId o vin.")

    _, database = resolve_db(x_sap_db)
    if insId:
        cond, params = "o.insID = ?", [int(insId)]
    else:
        v = (vin or "").strip()
        if len(v) != 17:          # comodín: no hay histórico confiable
            return {"success": True, "data": {"km": None, "callId": None, "fecha": None}}
        cond, params = "LTRIM(RTRIM(ISNULL(o.internalSN,''))) = ?", [v]

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT TOP 1 o.callID, TRY_CAST(o.U_KM AS INT) AS km, o.createDate
                FROM   OSCL o
                WHERE  {cond} AND TRY_CAST(o.U_KM AS INT) IS NOT NULL
                ORDER BY TRY_CAST(o.U_KM AS INT) DESC, o.callID DESC
                """,
                params,
            )
            r = cursor.fetchone()
            if not r:
                return {"success": True, "data": {"km": None, "callId": None, "fecha": None}}
            return {"success": True, "data": {
                "km":     int(r.km),
                "callId": int(r.callID),
                "fecha":  r.createDate.date().isoformat() if r.createDate else None,
            }}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


@router.get(
    "/serialLookup",
    summary="Busca un equipo por número de serie (manufactura, interno, distribución o proveedor)",
)
def serial_lookup(
    serial:   str           = Query(..., min_length=1, description="Texto a buscar en los números de serie"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Busca SOLO tarjetas de equipo (OINS) por EXACTAMENTE 3 criterios:
      1. Últimos 5 dígitos EXACTOS del VIN  ->  RIGHT(OINS.internalSN, 5) = texto
      2. Celular del cliente                ->  OCRD.Cellular LIKE %texto%
      3. Nombre del cliente                 ->  OCRD.CardName LIKE %texto%
    Devuelve hasta 20 coincidencias. NO busca por número de motor ni inventario.
    """
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            vin5 = serial.strip()            # para los últimos 5 dígitos EXACTOS
            like = f"%{serial.strip()}%"     # para nombre y celular del cliente
            cursor.execute(
                """
                SELECT TOP 20
                    OINS.insID          AS SysSerial,
                    OINS.internalSN     AS DistNumber,
                    OINS.manufSN        AS MnfSerial,
                    OINS.internalSN     AS IntrSerial,
                    CAST(NULL AS NVARCHAR(50)) AS SuppSerial,
                    CAST(NULL AS NVARCHAR(50)) AS Lot,
                    OINS.itemCode       AS ItemCode,
                    OINS.itemName       AS ItemName,
                    OITB.ItmsGrpNam     AS ItmsGrpNam,
                    OINS.customer       AS CardCode,
                    OINS.custmrName     AS CustomerName,
                    ISNULL(OCRD.Cellular, OCRD.Phone1) AS CustomerPhone,
                    CAST(NULL AS NVARCHAR(10)) AS WhsCode,
                    CAST(NULL AS NVARCHAR(100)) AS WhsName,
                    CAST(OINS.status AS NVARCHAR(20)) AS Status,
                    OINS.U_Ps_Marca     AS VehBrand,   -- Marca (KTM, Honda…)
                    OINS.U_Ps_SubMarca  AS VehModel,   -- Modelo (DUKE, NINJA 400…)
                    CAST(OINS.U_Ps_Modelo AS NVARCHAR(10)) AS VehYear,  -- Año (2026)
                    OINS.U_Ps_Placa     AS VehPlate,   -- Placa
                    OINS.U_Ps_Color     AS VehColor,   -- Color
                    OCRD.E_Mail         AS CustomerEmail,
                    'Tarjeta de Equipo' AS Notes
                FROM OINS
                LEFT JOIN OITB ON OITB.ItmsGrpCod = OINS.itemGroup
                LEFT JOIN OCRD ON OCRD.CardCode   = OINS.customer
                -- EXACTAMENTE 3 criterios (lo que pidió el usuario):
                WHERE RIGHT(RTRIM(OINS.internalSN), 5) = ?   -- 1) VIN: últimos 5 dígitos exactos
                   OR OCRD.Cellular = ?                       -- 2) celular del cliente (exacto)
                   OR OCRD.CardName LIKE ?                    -- 3) nombre del cliente (parcial)
                ORDER BY OINS.insID DESC
                """,
                # VIN-últimos5 y celular usan el texto tal cual; nombre va con %…%
                [vin5, vin5, like],
            )

            results = [
                {
                    "SysSerial":    int(r.SysSerial) if r.SysSerial is not None else None,
                    "DistNumber":   r.DistNumber,
                    "ManufSN":      r.MnfSerial,
                    "InternalSN":   r.IntrSerial,
                    "SupplierSN":   r.SuppSerial,
                    "Lot":          r.Lot,
                    "ItemCode":     r.ItemCode,
                    "ItemName":     r.ItemName,
                    "ItemGroup":    r.ItmsGrpNam,
                    "CardCode":     r.CardCode,
                    "CustomerName": r.CustomerName,
                    "CustomerPhone": r.CustomerPhone,
                    "WhsCode":      r.WhsCode,
                    "WhsName":      r.WhsName,
                    "Status":       r.Status,
                    "Notes":        r.Notes,
                    # Datos de vehículo desde los UDF de la tarjeta de equipo (OINS)
                    "Brand":        r.VehBrand,
                    "Model":        r.VehModel,
                    "Year":         r.VehYear,
                    "LicensePlate": r.VehPlate,
                    "Color":        r.VehColor,
                    "CustomerEmail": r.CustomerEmail,
                }
                for r in cursor.fetchall()
            ]

            return {
                "success": True,
                "message": None,
                "query":   serial,
                "count":   len(results),
                "results": results,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/serviceCallValidateCodes",
    summary="Valida que los códigos de una orden existan en SAP (para dar errores precisos)",
)
def validate_service_call_codes(
    cardCode:    Optional[str] = Query(default=None),
    itemCode:    Optional[str] = Query(default=None),
    assignee:    Optional[int] = Query(default=None),
    technician:  Optional[int] = Query(default=None),
    origin:      Optional[int] = Query(default=None),
    problemType: Optional[int] = Query(default=None),
    x_sap_db:    Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Revisa cada código contra su tabla en SAP y devuelve los que NO existen,
    para que el portal muestre un error específico en vez del genérico -2028.

    Los nombres de tabla/columna son fijos (no input) → seguro contra inyección.
    """
    _, database = resolve_db(x_sap_db)

    # (campo, valor, tabla, columna, etiqueta legible)
    checks = [
        ("cardCode",    cardCode,    "OCRD", "CardCode",   "Cliente"),
        ("itemCode",    itemCode,    "OITM", "ItemCode",   "Artículo (SKU)"),
        # assignee = usuario OUSR (Mecánico); technician = empleado OHEM (Asesor).
        ("assignee",    assignee,    "OUSR", "USERID",     "Mecánico"),
        ("technician",  technician,  "OHEM", "empID",      "Asesor de servicio"),
        ("origin",      origin,      "OSCO", "originID",   "Origen"),
        ("problemType", problemType, "OSCP", "prblmTypID", "Tipo de problema"),
    ]

    invalid = []
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            for field, value, table, col, label in checks:
                if value is None or value == "":
                    continue
                cursor.execute(f"SELECT 1 FROM {table} WHERE {col} = ?", [value])
                if not cursor.fetchone():
                    invalid.append({"field": field, "value": value, "label": label})
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error al validar códigos: {db_err}")

    return {"success": True, "valid": len(invalid) == 0, "invalid": invalid}


@router.get(
    "/serviceCallCatalogs",
    summary="Catálogos necesarios para crear una orden de servicio (origenes, tipos, técnicos, status, series)",
)
def get_catalogs(
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
    sucursal: Optional[str] = Query(default=None,
        description="Nombre de sucursal (OUBR.Name). Si se manda, Asesor y "
                    "Técnico se limitan a empleados de esa sucursal."),
):
    """
    Devuelve los catálogos que el form de creación necesita:
    - OSCO Origenes
    - OSCP Tipos de problema
    - OSCS Status (estados)
    - OHEM Empleados activos (asesores y técnicos)
    - NNM1 Series (numeración para Service Calls, ObjectCode='191')
    - Prioridades hardcoded (L/M/H)

    Si `sucursal` viene, se filtran asesores/técnicos a esa sucursal
    (OHEM.branch → OUBR.Name). Si la sucursal no existe en esta base o no
    tiene empleados, se cae a la lista completa (para no bloquear el alta).
    """
    _, database = resolve_db(x_sap_db)
    sucursal = (sucursal or "").strip() or None

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Origenes
            cursor.execute("SELECT originID, Name FROM OSCO WHERE ISNULL(Active,'Y')='Y' ORDER BY Name")
            origins = [{"id": int(r.originID), "name": r.Name} for r in cursor.fetchall()]

            # Problem types
            cursor.execute("SELECT prblmTypID, Name FROM OSCP WHERE ISNULL(Active,'Y')='Y' ORDER BY Name")
            problems = [{"id": int(r.prblmTypID), "name": r.Name} for r in cursor.fetchall()]

            # Status — solo activos
            cursor.execute("SELECT statusID, Name FROM OSCS WHERE ISNULL(Active,'Y')='Y' ORDER BY statusID")
            statuses = [{"id": int(r.statusID), "name": r.Name} for r in cursor.fetchall()]

            # Filtro por sucursal (OHEM.branch → OUBR.Name). Resolvemos los
            # códigos de branch de esa sucursal; si no existe en esta base,
            # branch_codes queda vacío y NO se filtra.
            branch_codes: List[int] = []
            if sucursal:
                try:
                    cursor.execute("SELECT Code FROM OUBR WHERE Name = ?", [sucursal])
                    branch_codes = [int(r.Code) for r in cursor.fetchall()]
                except pyodbc.Error:
                    branch_codes = []

            # Posiciones (OHPS) por NOMBRE — el posID cambia entre empresas.
            # El cliente mantiene en OHEM.position: "Asesor de Servicio" / "Tecnico".
            asesor_pos: List[int] = []
            tecnico_pos: List[int] = []
            try:
                cursor.execute("SELECT posID, name FROM OHPS")
                for r in cursor.fetchall():
                    n = (r.name or "").strip().lower()
                    if "asesor" in n:
                        asesor_pos.append(int(r.posID))
                    elif "tecnic" in n or "técnic" in n:
                        tecnico_pos.append(int(r.posID))
            except pyodbc.Error:
                pass

            def _fetch_people(codes: List[int]):
                """Devuelve (employees, technicians) activos, filtrados por los
                códigos de sucursal si `codes` no está vacío."""
                if codes:
                    ph = ",".join("?" * len(codes))
                    where_branch = f" AND h.branch IN ({ph}) "
                    params = list(codes)
                else:
                    where_branch, params = "", []

                def _emp_query(extra_where: str, extra_params: List[Any]):
                    cursor.execute(
                        "SELECT h.empID, h.firstName, h.lastName "
                        "FROM   OHEM h "
                        "WHERE  ISNULL(h.Active,'Y')='Y' " + where_branch + extra_where +
                        "ORDER BY h.firstName, h.lastName",
                        params + extra_params,
                    )
                    return [
                        {"id": int(r.empID),
                         "name": (f"{r.firstName or ''} {r.lastName or ''}").strip() or f"#{r.empID}"}
                        for r in cursor.fetchall()
                    ]

                # Empleados (lista completa, legado)
                try:
                    emps = _emp_query("", [])
                except pyodbc.Error:
                    cursor.execute("SELECT empID, firstName, lastName FROM OHEM ORDER BY firstName, lastName")
                    emps = [
                        {"id": int(r.empID),
                         "name": (f"{r.firstName or ''} {r.lastName or ''}").strip() or f"#{r.empID}"}
                        for r in cursor.fetchall()
                    ]

                # Asesores: por POSICIÓN "Asesor de Servicio" (OHEM.position).
                # Respaldo si nadie la tiene: empleados ya usados como technician
                # (el criterio anterior), para no bloquear el alta.
                techs: List[Dict[str, Any]] = []
                if asesor_pos:
                    try:
                        ph_pos = ",".join("?" * len(asesor_pos))
                        techs = _emp_query(f" AND h.position IN ({ph_pos}) ", list(asesor_pos))
                    except pyodbc.Error:
                        techs = []
                if not techs:
                    try:
                        techs = _emp_query(
                            " AND h.empID IN (SELECT DISTINCT technician FROM OSCL WHERE technician IS NOT NULL) ",
                            [],
                        )
                    except pyodbc.Error:
                        techs = emps   # último recurso: no bloqueamos

                # Mecánicos = USUARIOS (OUSR) con empleado activo — es lo que
                # SAP guarda en OSCL.assignee. Por POSICIÓN "Tecnico"; respaldo
                # sin filtro de posición si nadie la tiene.
                def _mec_query(extra_where: str, extra_params: List[Any]):
                    cursor.execute(
                        "SELECT DISTINCT u.USERID, u.U_NAME "
                        "FROM   OUSR u JOIN OHEM h ON h.userId = u.USERID "
                        "WHERE  ISNULL(h.Active,'Y')='Y' " + where_branch + extra_where +
                        "ORDER BY u.U_NAME",
                        params + extra_params,
                    )
                    return [
                        {"id": int(r.USERID), "name": (r.U_NAME or f"#{r.USERID}").strip()}
                        for r in cursor.fetchall()
                    ]

                mecs: List[Dict[str, Any]] = []
                if tecnico_pos:
                    try:
                        ph_pos = ",".join("?" * len(tecnico_pos))
                        mecs = _mec_query(f" AND h.position IN ({ph_pos}) ", list(tecnico_pos))
                    except pyodbc.Error:
                        mecs = []
                if not mecs:
                    try:
                        mecs = _mec_query("", [])
                    except pyodbc.Error:
                        mecs = []
                return emps, techs, mecs

            employees, technicians, mecanicos = _fetch_people(branch_codes)
            # Si el filtro por sucursal dejó la lista vacía (sucursal sin
            # empleados en esta base), caemos a la lista completa.
            if branch_codes and not employees:
                employees, technicians, mecanicos = _fetch_people([])

            # Series para ServiceCalls (ObjectCode=191)
            try:
                cursor.execute(
                    "SELECT Series, SeriesName "
                    "FROM   NNM1 "
                    "WHERE  ObjectCode = '191' AND ISNULL(Locked,'N') = 'N' "
                    "ORDER BY SeriesName"
                )
                series = [{"id": int(r.Series), "name": r.SeriesName} for r in cursor.fetchall()]
            except pyodbc.Error:
                series = []

            return {
                "success":   True,
                "origins":   origins,
                "problems":  problems,
                "statuses":  statuses,
                # Semántica REAL de este SAP (verificada contra su formulario):
                #   asesores  → OSCL.technician (empleado OHEM, "Asesor de Servicio")
                #   mecanicos → OSCL.assignee   (usuario OUSR,  "Mecánico")
                "asesores":  technicians,
                "mecanicos": mecanicos,
                # Legacy (portal viejo): se mantienen mientras convive el deploy.
                "employees": employees,
                "technicians": technicians,
                "series":    series,
                "priorities": [
                    {"id": "L", "name": "Baja"},
                    {"id": "M", "name": "Media"},
                    {"id": "H", "name": "Alta"},
                ],
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/employeeUserCodes",
    summary="Códigos de usuario de empleados SAP (OHEM.userId → OUSR) con su sucursal",
)
def list_employee_user_codes(
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Lista los empleados ACTIVOS que tienen un "código de usuario" de SAP
    asociado (OHEM.userId → OUSR.USER_CODE), junto con su sucursal
    (OHEM.branch → OUBR.Name). Se usa en Admin para ligar un usuario del
    portal a su empleado SAP y, de ahí, deducir su sucursal.
    """
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT h.empID, "
                "       LTRIM(RTRIM(ISNULL(h.firstName,'') + ' ' + ISNULL(h.lastName,''))) AS nombre, "
                "       u.USER_CODE AS userCode, "
                "       ISNULL(b.Name,'') AS sucursal "
                "FROM   OHEM h "
                "       JOIN OUSR u ON u.USERID = h.userId "
                "       LEFT JOIN OUBR b ON b.Code = h.branch "
                "WHERE  ISNULL(h.Active,'Y')='Y' "
                "  AND  h.userId IS NOT NULL AND h.userId <> -1 "
                "ORDER BY sucursal, nombre"
            )
            empleados = [
                {
                    "empId":    int(r.empID),
                    "userCode": (r.userCode or "").strip(),
                    "name":     (r.nombre or "").strip() or f"#{r.empID}",
                    "sucursal": (r.sucursal or "").strip(),
                }
                for r in cursor.fetchall()
                if (r.userCode or "").strip()
            ]
            return {"success": True, "empleados": empleados}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/equipment/customer/{card_code}",
    summary="Lista las Tarjetas de Equipo (motos) de un cliente",
)
def list_customer_equipment(
    card_code: str,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT  OINS.insID,
                        OINS.itemCode,
                        OINS.itemName,
                        OINS.manufSN,
                        OINS.internalSN,
                        OINS.manufDate,
                        OINS.dlvryDate,
                        OINS.wrrntyStrt,
                        OINS.wrrntyEnd
                FROM    OINS
                WHERE   OINS.customer = ?
                ORDER BY OINS.insID DESC
                """,
                [card_code],
            )
            rows = [
                {
                    "InsID":         int(r.insID),
                    "ItemCode":      r.itemCode,
                    "ItemName":      r.itemName,
                    "ManufSN":       r.manufSN,
                    "InternalSN":    r.internalSN,
                    "ManufDate":     r.manufDate.isoformat()   if r.manufDate   else None,
                    "DeliveryDate":  r.dlvryDate.isoformat()   if r.dlvryDate   else None,
                    "WarrantyStart": r.wrrntyStrt.isoformat()  if r.wrrntyStrt  else None,
                    "WarrantyEnd":   r.wrrntyEnd.isoformat()   if r.wrrntyEnd   else None,
                }
                for r in cursor.fetchall()
            ]
            return {
                "success":   True,
                "message":   None,
                "equipment": rows,
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


# Mano de obra = grupo de artículos "Taller Servicio". El PRECIO de estos NO se
# edita en el portal: es tarifa, no negociación, y de ahí sale el destajo del
# mecánico.
#
# El código del grupo es 125 en las dos empresas, pero el NOMBRE no coincide
# ("Taller Servicio" en Ferbel, "* TALLER SERVICIO" en Proshop), así que se
# compara el nombre sin espacios ni asteriscos y además se acepta el 125. Con
# cualquiera de las dos señales basta.
#
# NO se puede filtrar por el prefijo MO-: de los 5,697 artículos del grupo en
# Ferbel, solo 75 empiezan así. El resto son códigos como .AKIRA o .AN125.
_ES_MANO_DE_OBRA_SQL = """
    CASE WHEN OITM.ItmsGrpCod = 125
           OR REPLACE(REPLACE(UPPER(ISNULL(OITB.ItmsGrpNam,'')), ' ', ''), '*', '')
              = 'TALLERSERVICIO'
         THEN 1 ELSE 0 END
"""


@router.get(
    "/quoteArticleSearch",
    summary="Busca artículos por código o nombre (con precio de lista) para armar ofertas",
)
def quote_article_search(
    keyword:  str           = Query(..., min_length=2, description="Texto: busca en ItemCode e ItemName"),
    whs:      Optional[str] = Query(default=None, description="Almacén del vendedor: agrega OnHandWhs (existencia en ese almacén)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Devuelve hasta 25 artículos vendibles (OITM.SellItem='Y', no cancelados) que
    coincidan con el texto en código o nombre, con su precio de la lista
    PRICE_LIST_CODE (ITM1), el stock total (OITM.OnHand) y, si se manda `whs`,
    el stock en ese almacén (OITW). Cada palabra debe coincidir (AND).
    """
    _, database = resolve_db(x_sap_db)
    words = [w for w in keyword.strip().split() if w]
    if not words:
        return {"success": True, "articles": []}

    clause = " AND ".join("(OITM.ItemCode LIKE ? OR OITM.ItemName LIKE ?)" for _ in words)
    params: list = [PRICE_LIST_CODE, (whs or "").strip()]
    for w in words:
        like = f"%{w}%"
        params += [like, like]

    sql = f"""
        SELECT TOP 25
            OITM.ItemCode,
            OITM.ItemName,
            OITM.OnHand,
            ISNULL(ITM1.Price, 0)  AS Price,
            ISNULL(OITW.OnHand, 0) AS OnHandWhs,
            {_ES_MANO_DE_OBRA_SQL} AS ManoObra
        FROM   OITM
        LEFT   JOIN OITB ON OITB.ItmsGrpCod = OITM.ItmsGrpCod
        LEFT   JOIN ITM1 ON ITM1.ItemCode = OITM.ItemCode AND ITM1.PriceList = ?
        LEFT   JOIN OITW ON OITW.ItemCode = OITM.ItemCode AND OITW.WhsCode = ?
        WHERE  ISNULL(OITM.Canceled,'N') = 'N'
          AND  ISNULL(OITM.SellItem,'Y') = 'Y'
          AND  {clause}
        -- Primero lo que SÍ hay: existencia en el almacén del vendedor, luego en
        -- el general, y hasta el final lo que está en cero en ambos. El TOP se
        -- aplica DESPUÉS de ordenar, así que los 25 que se devuelven ya son los
        -- disponibles (antes se tomaban los 25 primeros por código y salían
        -- puros ceros arriba).
        ORDER BY
            CASE WHEN ISNULL(OITW.OnHand, 0) > 0 THEN 0
                 WHEN ISNULL(OITM.OnHand, 0) > 0 THEN 1
                 ELSE 2 END,
            ISNULL(OITW.OnHand, 0) DESC,
            ISNULL(OITM.OnHand, 0) DESC,
            OITM.ItemCode
    """
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            articles = [
                {
                    "ItemCode":  r.ItemCode,
                    "ItemName":  r.ItemName,
                    "Price":     float(r.Price)     if r.Price     is not None else 0.0,
                    "OnHand":    float(r.OnHand)    if r.OnHand    is not None else 0.0,
                    "OnHandWhs": float(r.OnHandWhs) if r.OnHandWhs is not None else 0.0,
                    "ManoObra":  bool(r.ManoObra),
                }
                for r in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "articles": articles}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


@router.get(
    "/kitSearch",
    summary="Busca KITS (artículos con Lista de Materiales / BOM) para armar ofertas",
)
def kit_search(
    keyword:  Optional[str] = Query(default=None, description="Texto: busca en ItemCode e ItemName del kit"),
    callId:   Optional[int] = Query(default=None, description="ODS: filtra kits por la moto de la orden (marca/submarca)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Kits = artículos cabecera de una Lista de Materiales (OITT.Code, TreeType 'S'
    Venta / 'T' Modelo), con precio de lista PRICE_LIST_CODE (ITM1).

    Si viene `callId`, filtra los kits por la MOTO de la orden: se leen marca y
    submarca del ARTÍCULO de la moto (OSCL → OINS → itemCode → OITM.U_TIPO_MARCA /
    U_SUBMARCA, porque los UDFs de la tarjeta OINS suelen estar vacíos) y se
    devuelven solo los kits cuyos U_TIPO_MARCA/U_SUBMARCA coinciden. El precio es
    editable en el portal; SAP explota el BOM al crear la oferta.
    """
    _, database = resolve_db(x_sap_db)
    words = [w for w in (keyword or "").strip().split() if w]

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Marca/submarca de la moto de la ODS (del artículo de la moto).
            moto_marca = moto_sub = None
            if callId:
                cursor.execute(
                    """
                    SELECT LTRIM(RTRIM(M.U_TIPO_MARCA)) AS marca,
                           LTRIM(RTRIM(M.U_SUBMARCA))   AS sub
                    FROM   OSCL O
                    LEFT   JOIN OINS I ON I.insID = O.insID
                    LEFT   JOIN OITM M ON M.ItemCode = ISNULL(I.itemCode, O.itemCode)
                    WHERE  O.callID = ?
                    """,
                    [callId],
                )
                mr = cursor.fetchone()
                if mr:
                    moto_marca = mr.marca or None
                    moto_sub   = mr.sub or None

            # Debe haber por qué filtrar: texto o (marca+submarca de la moto).
            if not words and not (moto_marca and moto_sub):
                return {"success": True, "kits": [], "motoMarca": moto_marca, "motoSubMarca": moto_sub}

            conds  = ["OITT.TreeType IN ('S', 'T')", "ISNULL(OITM.Canceled,'N') = 'N'"]
            params: list = [PRICE_LIST_CODE]
            for w in words:
                conds.append("(OITM.ItemCode LIKE ? OR OITM.ItemName LIKE ?)")
                like = f"%{w}%"
                params += [like, like]
            if moto_marca and moto_sub:
                conds.append("LTRIM(RTRIM(ISNULL(OITM.U_TIPO_MARCA,''))) = ?")
                conds.append("LTRIM(RTRIM(ISNULL(OITM.U_SUBMARCA,'')))   = ?")
                params += [moto_marca, moto_sub]

            sql = f"""
                SELECT TOP 25
                    OITM.ItemCode, OITM.ItemName, OITT.TreeType,
                    ISNULL(ITM1.Price, 0) AS Price
                FROM   OITT
                JOIN   OITM ON OITM.ItemCode = OITT.Code
                LEFT   JOIN ITM1 ON ITM1.ItemCode = OITM.ItemCode AND ITM1.PriceList = ?
                WHERE  {' AND '.join(conds)}
                ORDER BY OITM.ItemCode
            """
            cursor.execute(sql, params)
            kits = [
                {
                    "ItemCode": r.ItemCode,
                    "ItemName": r.ItemName,
                    "TreeType": r.TreeType,     # 'S' Venta | 'T' Modelo
                    "Price":    float(r.Price) if r.Price is not None else 0.0,
                }
                for r in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "kits": kits, "motoMarca": moto_marca, "motoSubMarca": moto_sub}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


@router.get(
    "/kitComponents",
    summary="Componentes (artículos) de un kit / Lista de Materiales (ITT1)",
)
def kit_components(
    itemCode: str           = Query(..., min_length=1, description="Código del kit (cabecera del BOM)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    Devuelve los componentes de la Lista de Materiales del kit (ITT1.Father),
    con su cantidad y precio. El precio es el del BOM (ITT1.Price); si es 0 usa
    el de lista PRICE_LIST_CODE (ITM1). Se usan estos artículos en la oferta y en
    los estimates del punto de inspección — NO el kit como tal.
    """
    _, database = resolve_db(x_sap_db)
    sql = """
        SELECT C.Code, C.Quantity, C.Price AS BomPrice, C.Warehouse,
               O.ItemName, ISNULL(I.Price, 0) AS ListPrice,
               CASE WHEN O.ItmsGrpCod = 125
                      OR REPLACE(REPLACE(UPPER(ISNULL(B.ItmsGrpNam,'')), ' ', ''), '*', '')
                         = 'TALLERSERVICIO'
                    THEN 1 ELSE 0 END AS ManoObra
        FROM   ITT1 C
        LEFT   JOIN OITM O ON O.ItemCode = C.Code
        LEFT   JOIN OITB B ON B.ItmsGrpCod = O.ItmsGrpCod
        LEFT   JOIN ITM1 I ON I.ItemCode = C.Code AND I.PriceList = ?
        WHERE  C.Father = ?
        ORDER BY C.ChildNum
    """
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(sql, [PRICE_LIST_CODE, itemCode])
            components = []
            for r in cursor.fetchall():
                bom  = float(r.BomPrice)  if r.BomPrice  is not None else 0.0
                lst  = float(r.ListPrice) if r.ListPrice is not None else 0.0
                components.append({
                    "ItemCode":  r.Code,
                    "ItemName":  r.ItemName,
                    "Quantity":  float(r.Quantity) if r.Quantity is not None else 1.0,
                    "Price":     bom if bom > 0 else lst,
                    "Warehouse": r.Warehouse,
                    "ManoObra":  bool(r.ManoObra),
                })
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "itemCode": itemCode, "components": components}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


@router.get(
    "/salespersonSearch",
    summary="Busca vendedores (OSLP) con su almacén asignado, para crear ofertas",
)
def salesperson_search(
    keyword:  str           = Query(..., min_length=1, description="Texto: busca en nombre o código del vendedor"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """
    SAP (add-on CVMSales) exige un vendedor en la Oferta y ata el almacén
    permitido a `OSLP.Telephone`:
      - Si Telephone contiene '.', el vendedor es EXENTO (cualquier almacén).
      - Si no, el almacén de las líneas debe ser ese (su almacén asignado).
    Devolvemos eso para que el portal ajuste el almacén según el vendedor elegido.
    """
    _, database = resolve_db(x_sap_db)
    like = f"%{keyword.strip()}%"
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 25 SlpCode, SlpName, Telephone
                FROM   OSLP
                WHERE  ISNULL(Active,'Y') = 'Y'
                  AND  SlpCode >= 0
                  AND  (SlpName LIKE ? OR CAST(SlpCode AS NVARCHAR(20)) LIKE ?)
                ORDER BY SlpName
                """,
                [like, like],
            )
            rows = cursor.fetchall()
            # ¿qué Telephone son códigos de almacén reales?
            whs_codes = {r[0] for r in cursor.execute("SELECT WhsCode FROM OWHS").fetchall()}
            salespeople = []
            for r in rows:
                tel = (r.Telephone or "").strip()
                exempt = "." in tel
                warehouse = tel if (tel in whs_codes and not exempt) else None
                salespeople.append({
                    "SlpCode":   int(r.SlpCode),
                    "SlpName":   r.SlpName,
                    "Warehouse": warehouse,   # almacén fijo (None si exento)
                    "Exempt":    exempt,      # True = puede cualquier almacén
                })
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "salespeople": salespeople}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


# La SUCURSAL del usuario (OUBR.Name, vía OHEM) y la LOCALIDAD del almacén del
# vendedor (OLCT.Location) viven en tablas distintas y NO se llaman igual. Con
# comparar los nombres tal cual, los usuarios de "Miramontes_Sur" (8 en el
# portal) se quedarían sin vendedores. Este mapa traduce OUBR → OLCT; lo que no
# esté aquí simplemente no filtra (se devuelven todos, agrupados).
_SUCURSAL_A_LOCALIDAD = {
    "satelite":       "Satélite",
    "patriotismo":    "Patriotismo",
    "tonala":         "Tonala",
    "aeropuerto":     "Aeropuerto",
    "miramontes_sur": "Sur (Miramontes)",
    "zona sur":       "Sur (Miramontes)",
}


def _norm_txt(s: str) -> str:
    """minúsculas y sin acentos, para comparar nombres de sucursal."""
    s = unicodedata.normalize("NFD", (s or "").strip().lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


@router.get(
    "/salespersons",
    summary="Vendedores activos con su almacén y sucursal (para el selector de la oferta)",
)
def salespersons(
    sucursal: Optional[str] = Query(default=None, description="Sucursal del usuario (OUBR.Name)"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Lista completa de vendedores activos con almacén válido, anotada con su
    localidad y si el almacén es de SERVICIO (o llantera). El portal la usa para
    armar un selector agrupado, en vez de obligar a escribir para buscar.

    No filtra por sucursal: devuelve todos con su localidad para que el portal
    ponga arriba los de la del usuario. `sucursalResuelta` dice a qué localidad
    se tradujo la sucursal recibida (None si no se pudo, y entonces el portal
    los muestra todos sin destacar ninguno)."""
    _, database = resolve_db(x_sap_db)
    localidad = _SUCURSAL_A_LOCALIDAD.get(_norm_txt(sucursal)) if sucursal else None

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT  s.SlpCode, s.SlpName,
                        LTRIM(RTRIM(ISNULL(s.Telephone,''))) AS Tel,
                        w.WhsCode, w.WhsName,
                        ISNULL(l.Location, '') AS Localidad
                FROM    OSLP s
                LEFT    JOIN OWHS w ON w.WhsCode = LTRIM(RTRIM(s.Telephone))
                LEFT    JOIN OLCT l ON l.Code = w.Location
                WHERE   ISNULL(s.Active,'Y') = 'Y'
                  AND   s.SlpCode >= 0
                ORDER BY s.SlpName
                """
            )
            vendedores = []
            for r in cursor.fetchall():
                tel    = (r.Tel or "").strip()
                exento = "." in tel
                whs    = (r.WhsCode or "").strip() or None
                nombre_whs = (r.WhsName or "").strip() or None
                # Servicio y llantera son los almacenes desde los que se cotiza
                # servicio (verificado con 6 meses de ofertas reales).
                n = _norm_txt(nombre_whs or "")
                es_servicio = ("servicio" in n) or ("llanter" in n)
                vendedores.append({
                    "SlpCode":       int(r.SlpCode),
                    "SlpName":       (r.SlpName or "").strip(),
                    "Warehouse":     None if exento else whs,
                    "WarehouseName": nombre_whs,
                    "Exempt":        exento,
                    "Sucursal":      (r.Localidad or "").strip() or None,
                    "EsServicio":    es_servicio,
                })
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "vendedores": vendedores,
                "sucursalResuelta": localidad}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


# Artículos de "alta": sirven para cotizar una refacción que NO está en el
# catálogo. El asesor los usa poniendo los datos de la pieza en la descripción
# de la línea, separados por *. SAP conserva esa descripción propia (verificado:
# 64,300 líneas así, todas creadas por Service Layer, que es por donde escribe
# el portal).
#
# La lista es de CANDIDATOS, no fija: se devuelven solo los que existen de
# verdad en la empresa consultada. Así AT aparece solo el día que lo creen en
# SAP, y no se ofrece algo que reventaría al guardar. En Proshop estos mismos
# códigos significan ANTICIPO en vez de ALTA; por eso el nombre sale de SAP.
_ARTICULOS_ALTA = ("AP", "AS", "AM", "AT", "AY")
@router.get(
    "/altaItems",
    summary="Artículos de alta (AP/AS/AM/AT/AY) que existen en la empresa",
)
def alta_items(x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB")):
    _, database = resolve_db(x_sap_db)
    marcadores = ",".join("?" * len(_ARTICULOS_ALTA))
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT  ItemCode, ItemName
                FROM    OITM
                WHERE   ItemCode IN ({marcadores})
                  AND   ISNULL(SellItem, 'Y')  = 'Y'
                  AND   ISNULL(frozenFor, 'N') <> 'Y'
                ORDER BY ItemCode
                """,
                list(_ARTICULOS_ALTA),
            )
            items = [
                {"ItemCode": (r.ItemCode or "").strip(),
                 "ItemName": (r.ItemName or "").strip()}
                for r in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()
        return {"success": True, "items": items}
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
