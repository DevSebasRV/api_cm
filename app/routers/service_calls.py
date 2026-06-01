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
                "(OSCL.subject LIKE ? OR OSCL.custmrName LIKE ? "
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
            OHEM.firstName + ISNULL(' ' + OHEM.lastName, '') AS TecnicoName,
            OINS.manufSN         AS EquipManufSN,
            OINS.internalSN      AS EquipInternalSN,
            OITM.ItemName        AS ItemFullName
    FROM    OSCL
    LEFT    JOIN OCRD ON OCRD.CardCode    = OSCL.customer
    LEFT    JOIN OSCS ON OSCS.statusID    = OSCL.status
    LEFT    JOIN OSCO ON OSCO.originID    = OSCL.origin
    LEFT    JOIN OSCP ON OSCP.prblmTypID  = OSCL.problemTyp
    LEFT    JOIN OHEM ON OHEM.empID       = OSCL.assignee
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
        # Si la orden está abierta (sin closeDate), usamos createDate como tope.
        date_to = close_date if close_date is not None else create_date

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
