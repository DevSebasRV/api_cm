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
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, List
import pyodbc

from app.config import EMPRESAS
from app.database import get_connection
from app.routers.shopify import resolve_db, err, _pagination  # reutilizamos helpers

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
            OHEM.firstName + ISNULL(' ' + OHEM.lastName, '') AS Tecnico
    FROM    OSCL
    LEFT    JOIN OCRD ON OCRD.CardCode  = OSCL.customer
    LEFT    JOIN OITM ON OITM.ItemCode  = OSCL.itemCode
    LEFT    JOIN OSCS ON OSCS.statusID  = OSCL.status
    LEFT    JOIN OHEM ON OHEM.empID     = OSCL.assignee
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
    }


@router.get(
    "/serviceCalls",
    summary="Lista paginada de órdenes de servicio",
)
def list_service_calls(
    cardCode: Optional[str] = Query(default=None, description="Filtra por CardCode exacto"),
    status:   Optional[int] = Query(default=None, description="Filtra por statusID (-3=Open, -2=Closed)"),
    keyword:  Optional[str] = Query(default=None, description="Búsqueda libre en Subject / CustomerName / ItemCode / ItemName"),
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
            where_parts.append(
                "(OSCL.Subject LIKE ? OR OSCL.customerName LIKE ? "
                "OR OSCL.itemCode LIKE ? OR OSCL.itemName LIKE ?)"
            )
            like = f"%{w}%"
            params += [like, like, like, like]

    where_clause = " AND ".join(where_parts)

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM OSCL WHERE {where_clause}", params)
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
# 2) GET /serviceCalls/{call_id} — detalle completo
# ─────────────────────────────────────────────────────────────────────────────

_DETAIL_HEADER = """
    SELECT  OSCL.CallID,
            OSCL.Subject,
            OSCL.customer       AS CardCode,
            OSCL.contctPrsn,
            OSCL.Telephone,
            OSCL.manufactSN,
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
            OSCL.responDate,
            OSCL.respByTime,
            OCRD.CardName        AS CustomerCardName,
            OCRD.Phone1          AS CustomerPhone,
            OCRD.E_Mail          AS CustomerEmail,
            OSCS.Name            AS StatusName,
            OSCO.Name            AS OrigenName,
            OSCP.Name            AS ProblemName,
            OHEM.firstName + ISNULL(' ' + OHEM.lastName, '') AS TecnicoName,
            OINS.manufSN         AS EquipManufSN,
            OINS.internalSN      AS EquipInternalSN,
            OITM.ItemName        AS ItemFullName
    FROM    OSCL
    LEFT    JOIN OCRD ON OCRD.CardCode    = OSCL.customer
    LEFT    JOIN OSCS ON OSCS.statusID    = OSCL.status
    LEFT    JOIN OSCO ON OSCO.OrgnCode    = OSCL.origin
    LEFT    JOIN OSCP ON OSCP.prblmTypID  = OSCL.problemTyp
    LEFT    JOIN OHEM ON OHEM.empID       = OSCL.assignee
    LEFT    JOIN OINS ON OINS.insID       = OSCL.insID
    LEFT    JOIN OITM ON OITM.ItemCode    = OSCL.itemCode
    WHERE   OSCL.CallID = ?
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
            "ContactName":  r.contctPrsn,
            "ContactPhone": r.Telephone,
        },
        "Equipment": {
            "InsID":        int(r.insID) if r.insID else None,
            "ItemCode":     r.itemCode,
            "ItemName":     r.ItemFullName,
            "ManufSN":      r.EquipManufSN or r.manufactSN,
            "InternalSN":   r.EquipInternalSN or r.internalSN,
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
        "ResponseDate":     r.responDate.isoformat() if r.responDate else None,
        "ResponseByTime":   int(r.respByTime) if r.respByTime is not None else None,
    }


def _fetch_activities(cursor, call_id: int) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT  SCL1.LineID,
                SCL1.clgCode,
                SCL1.actDate,
                SCL1.endDate,
                SCL1.assignedTo,
                OHEM.firstName + ISNULL(' ' + OHEM.lastName, '') AS Tecnico,
                OCLG.Notes
        FROM    SCL1
        LEFT    JOIN OCLG ON OCLG.ClgCode  = SCL1.clgCode
        LEFT    JOIN OHEM ON OHEM.empID    = SCL1.assignedTo
        WHERE   SCL1.callID = ?
        ORDER BY SCL1.LineID
        """,
        [call_id],
    )
    return [
        {
            "LineID":     int(r.LineID),
            "ClgCode":    int(r.clgCode) if r.clgCode else None,
            "ActDate":    r.actDate.isoformat() if r.actDate else None,
            "EndDate":    r.endDate.isoformat() if r.endDate else None,
            "AssignedTo": int(r.assignedTo) if r.assignedTo else None,
            "Tecnico":    (r.Tecnico or "").strip() or None,
            "Notes":      r.Notes,
        }
        for r in cursor.fetchall()
    ]


def _fetch_refacciones(cursor, call_id: int) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT  SCL3.LineID,
                SCL3.ItemCode,
                OITM.ItemName,
                SCL3.Quantity,
                SCL3.WhsCode,
                OWHS.WhsName,
                SCL3.Price,
                SCL3.ObjType,
                SCL3.DocEntry
        FROM    SCL3
        LEFT    JOIN OITM ON OITM.ItemCode = SCL3.ItemCode
        LEFT    JOIN OWHS ON OWHS.WhsCode  = SCL3.WhsCode
        WHERE   SCL3.callID = ?
        ORDER BY SCL3.LineID
        """,
        [call_id],
    )
    return [
        {
            "LineID":   int(r.LineID),
            "ItemCode": r.ItemCode,
            "ItemName": r.ItemName,
            "Quantity": float(r.Quantity) if r.Quantity is not None else 0.0,
            "WhsCode":  r.WhsCode,
            "WhsName":  r.WhsName,
            "Price":    float(r.Price) if r.Price is not None else 0.0,
            "Total":    float((r.Quantity or 0) * (r.Price or 0)),
            "ObjType":  int(r.ObjType) if r.ObjType is not None else None,
            "DocEntry": int(r.DocEntry) if r.DocEntry is not None else None,
            "DocLabel": OBJ_TYPE_MAP.get(int(r.ObjType), str(r.ObjType)) if r.ObjType else None,
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
            "LineTotal":   float(l.LineTotal) if l.LineTotal is not None else 0.0,
            "LineStatus":      l.LineStatus,
            "LineStatusLabel": LINE_STATUS_MAP.get(l.LineStatus, l.LineStatus or ""),
            "WhsCode":     l.WhsCode,
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


def _fetch_related_documents(cursor, call_id: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    De SCL3 saca todos los (ObjType, DocEntry) únicos, los agrupa por tipo
    y trae la cabecera + líneas de cada documento de su tabla real.
    """
    cursor.execute(
        """
        SELECT DISTINCT ObjType, DocEntry
        FROM   SCL3
        WHERE  callID = ? AND ObjType IS NOT NULL AND DocEntry IS NOT NULL
        """,
        [call_id],
    )
    unique_refs = [(int(r.ObjType), int(r.DocEntry)) for r in cursor.fetchall()]

    grouped: Dict[str, List[Dict[str, Any]]] = {
        "Oferta":  [],
        "Pedido":  [],
        "Entrega": [],
        "Factura": [],
    }
    for obj_type, doc_entry in unique_refs:
        doc = _fetch_document(cursor, obj_type, doc_entry)
        if doc:
            grouped.setdefault(doc["Type"], []).append(doc)

    return grouped


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

            # 2. Actividades (SCL1)
            activities = _fetch_activities(cursor, call_id)

            # 3. Refacciones / gastos (SCL3)
            refacciones = _fetch_refacciones(cursor, call_id)

            # 4. Documentos vinculados (Ofertas, Pedidos, Entregas, Facturas)
            documents = _fetch_related_documents(cursor, call_id)

            return {
                "success":     True,
                "message":     None,
                "header":      header,
                "activities":  activities,
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
                        OINS.startDate,
                        OINS.startupDat,
                        OINS.endDate
                FROM    OINS
                WHERE   OINS.customer = ?
                ORDER BY OINS.insID DESC
                """,
                [card_code],
            )
            rows = [
                {
                    "InsID":       int(r.insID),
                    "ItemCode":    r.itemCode,
                    "ItemName":    r.itemName,
                    "ManufSN":     r.manufSN,
                    "InternalSN":  r.internalSN,
                    "StartDate":   r.startDate.isoformat()   if r.startDate   else None,
                    "StartupDate": r.startupDat.isoformat()  if r.startupDat  else None,
                    "EndDate":     r.endDate.isoformat()     if r.endDate     else None,
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
