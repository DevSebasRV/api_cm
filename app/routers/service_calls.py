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
                DocTotal, DocCur, Comments, CardCode, CardName
        FROM    {o_table}
        WHERE   DocEntry = ?
        """,
        [doc_entry],
    )
    h = cursor.fetchone()
    if not h:
        return None

    cursor.execute(
        f"""
        SELECT  LineNum, ItemCode, Dscription, Quantity, Price, LineTotal,
                VatSum, VatPrcnt, PriceAfVAT, GTotal,
                LineStatus, WhsCode, TargetType, TrgetEntry
        FROM    {l_table}
        WHERE   DocEntry = ?
        ORDER BY LineNum
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
        "DocTotal":   float(h.DocTotal) if h.DocTotal is not None else 0.0,
        "DocCurrency": h.DocCur,
        "CardCode":   h.CardCode,
        "CardName":   h.CardName,
        "Comments":   h.Comments,
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
    Busca documentos relacionados con la orden de servicio combinando dos
    mecanismos:

    1.  Linkage estándar SAP B1 — busca líneas en QUT1/RDR1/DLN1/INV1 con
        BaseType=191 y BaseEntry=CallID. Funciona cuando se crearon los docs
        directamente desde la pestaña "Registr y Refacciones" de SAP.

    2.  Heurística por cliente + fechas — si el linkage estándar no encuentra
        nada (caso típico en Ferbel donde los docs se crean por otros flujos),
        busca docs del MISMO cliente con DocDate entre createDate y closeDate.

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
                    grouped[type_label].append(doc)
                    seen.add(key)
        except pyodbc.Error:
            pass

    # ── Mecanismo 2: Heurística por cliente + fechas ────────────────────────
    if card_code and create_date is not None:
        # Orden cerrada → hasta su fecha de cierre. Orden ABIERTA → hasta HOY,
        # para que las ofertas creadas después (ej. desde el portal) aparezcan.
        date_to = close_date if close_date is not None else datetime.date.today()

        for obj_type, _, head_table, type_label in doc_specs:
            try:
                cursor.execute(
                    f"""
                    SELECT DocEntry
                    FROM   {head_table}
                    WHERE  CardCode = ?
                      AND  DocDate >= ?
                      AND  DocDate <= ?
                    ORDER BY DocEntry
                    """,
                    [card_code, create_date, date_to],
                )
                for r in cursor.fetchall():
                    key = (obj_type, int(r.DocEntry))
                    if key in seen:
                        continue
                    doc = _fetch_document(cursor, obj_type, int(r.DocEntry))
                    if doc:
                        grouped[type_label].append(doc)
                        seen.add(key)
            except pyodbc.Error:
                pass

    # ── Mecanismo 3: marcador del portal en Comments ("ODS #<callId>") ───────
    # Determinístico para documentos creados desde el portal. NO depende de
    # fechas, así que liga las ofertas aunque la orden ya tenga closeDate y el
    # documento se haya creado DESPUÉS de esa fecha (caso que la heurística por
    # rango de fechas se perdía). El marcador lo estampa create-quote-action.
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


# ─────────────────────────────────────────────────────────────────────────────
# 3) GET /equipment/customer/{card_code} — tarjetas de equipo del cliente
# ─────────────────────────────────────────────────────────────────────────────

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

            def _fetch_people(codes: List[int]):
                """Devuelve (employees, technicians) activos, filtrados por los
                códigos de sucursal si `codes` no está vacío."""
                if codes:
                    ph = ",".join("?" * len(codes))
                    where_branch = f" AND h.branch IN ({ph}) "
                    params = list(codes)
                else:
                    where_branch, params = "", []

                # Asesores = todos los empleados activos (de la sucursal si aplica)
                try:
                    cursor.execute(
                        "SELECT h.empID, h.firstName, h.lastName "
                        "FROM   OHEM h "
                        "WHERE  ISNULL(h.Active,'Y')='Y' " + where_branch +
                        "ORDER BY h.firstName, h.lastName",
                        params,
                    )
                except pyodbc.Error:
                    cursor.execute("SELECT empID, firstName, lastName FROM OHEM ORDER BY firstName, lastName")
                emps = [
                    {"id": int(r.empID),
                     "name": (f"{r.firstName or ''} {r.lastName or ''}").strip() or f"#{r.empID}"}
                    for r in cursor.fetchall()
                ]

                # Técnicos: SAP exige que OSCL.technician tenga "rol de técnico".
                # El set fiable son los empleados ACTIVOS ya usados como técnico.
                try:
                    cursor.execute(
                        "SELECT h.empID, h.firstName, h.lastName "
                        "FROM   OHEM h "
                        "WHERE  ISNULL(h.Active,'Y')='Y' " + where_branch +
                        "  AND  h.empID IN (SELECT DISTINCT technician FROM OSCL WHERE technician IS NOT NULL) "
                        "ORDER BY h.firstName, h.lastName",
                        params,
                    )
                    techs = [
                        {"id": int(r.empID),
                         "name": (f"{r.firstName or ''} {r.lastName or ''}").strip() or f"#{r.empID}"}
                        for r in cursor.fetchall()
                    ]
                except pyodbc.Error:
                    techs = emps   # fallback: si falla, no bloqueamos

                # Mecánicos = USUARIOS (OUSR) con empleado activo — es lo que
                # SAP guarda en OSCL.assignee (campo "Mecánico" del formulario).
                try:
                    cursor.execute(
                        "SELECT DISTINCT u.USERID, u.U_NAME "
                        "FROM   OUSR u JOIN OHEM h ON h.userId = u.USERID "
                        "WHERE  ISNULL(h.Active,'Y')='Y' " + where_branch.replace("h.branch", "h.branch") +
                        "ORDER BY u.U_NAME",
                        params,
                    )
                    mecs = [
                        {"id": int(r.USERID), "name": (r.U_NAME or f"#{r.USERID}").strip()}
                        for r in cursor.fetchall()
                    ]
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
            ISNULL(OITW.OnHand, 0) AS OnHandWhs
        FROM   OITM
        LEFT   JOIN ITM1 ON ITM1.ItemCode = OITM.ItemCode AND ITM1.PriceList = ?
        LEFT   JOIN OITW ON OITW.ItemCode = OITM.ItemCode AND OITW.WhsCode = ?
        WHERE  ISNULL(OITM.Canceled,'N') = 'N'
          AND  ISNULL(OITM.SellItem,'Y') = 'Y'
          AND  {clause}
        ORDER BY OITM.ItemCode
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
               O.ItemName, ISNULL(I.Price, 0) AS ListPrice
        FROM   ITT1 C
        LEFT   JOIN OITM O ON O.ItemCode = C.Code
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
