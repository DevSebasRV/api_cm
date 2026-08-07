"""
Solicitudes de traslado (OWTQ/WTQ1) ligadas a una ODS.

La liga vive en el UDF OWTQ.U_ODS (creado 2026-07-31 en las 3 bases vía
UserFieldsMD): el portal lo escribe al crear la solicitud. La columna
"Filler" de OWTQ es el almacén ORIGEN (nombre heredado de SAP); ToWhsCode
es el destino.
"""

from fastapi import APIRouter, Header
from typing import Optional, Any, Dict, List
import pyodbc

from app.database import get_connection
from app.routers.common import resolve_db, err

router = APIRouter(tags=["Traslados"])

_STATUS = {"O": "Abierta", "C": "Cerrada"}


def _estatus(doc_status: Optional[str], canceled: Optional[str]) -> str:
    if (canceled or "N") == "Y":
        return "Cancelada"
    return _STATUS.get((doc_status or "").strip(), doc_status or "")


@router.get(
    "/transferRequests",
    summary="Solicitudes de traslado ligadas a una ODS (OWTQ.U_ODS)",
)
def list_transfer_requests(
    ods: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT  q.DocEntry, q.DocNum, q.DocDate, q.DocStatus, q.CANCELED,
                        q.Comments, q.SlpCode,
                        s.SlpName,
                        q.Filler    AS FromWhs,
                        wf.WhsName  AS FromWhsName,
                        q.ToWhsCode AS ToWhs,
                        wt.WhsName  AS ToWhsName,
                        (SELECT COUNT(*) FROM WTQ1 l WHERE l.DocEntry = q.DocEntry) AS Lineas
                FROM    OWTQ q
                LEFT    JOIN OSLP s  ON s.SlpCode  = q.SlpCode
                LEFT    JOIN OWHS wf ON wf.WhsCode = q.Filler
                LEFT    JOIN OWHS wt ON wt.WhsCode = q.ToWhsCode
                WHERE   q.U_ODS = ?
                ORDER BY q.DocEntry DESC
                """,
                [str(ods)],
            )
            solicitudes = [
                {
                    "docEntry":  int(r.DocEntry),
                    "docNum":    int(r.DocNum),
                    "fecha":     r.DocDate.date().isoformat() if r.DocDate else None,
                    "estatus":   _estatus(r.DocStatus, r.CANCELED),
                    "fromWhs":   {"code": (r.FromWhs or "").strip(), "name": (r.FromWhsName or "").strip() or None},
                    "toWhs":     {"code": (r.ToWhs or "").strip(),   "name": (r.ToWhsName or "").strip() or None},
                    "vendedor":  (r.SlpName or "").strip() or None,
                    "lineas":    int(r.Lineas or 0),
                    "comments":  (r.Comments or "").strip() or None,
                }
                for r in cursor.fetchall()
            ]
            return {"success": True, "message": None,
                    "data": {"solicitudes": solicitudes}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/transferRequestsOpen",
    summary="Solicitudes de traslado ABIERTAS ligadas a una ODS, agrupadas por ODS",
)
def list_open_transfer_requests(
    sucursal: Optional[str] = None,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Abiertas (DocStatus='O', no canceladas) que traen U_ODS. Solo las creadas
    desde el portal llevan esa liga; las capturadas directo en SAP no aparecen.
    Se devuelven agrupadas por ODS, con el cliente de la orden (OSCL).

    `sucursal` (OUBR.Name) limita a las órdenes cuyo ASESOR pertenece a esa
    sucursal — mismo criterio que el resto del portal (OSCL.technician → OHEM →
    OUBR). Sin sucursal se devuelven todas."""
    _, database = resolve_db(x_sap_db)
    filtro_suc = ""
    params: List[Any] = []
    if sucursal and sucursal.strip():
        filtro_suc = (" AND c.technician IN (SELECT h.empID FROM OHEM h "
                      "JOIN OUBR b ON b.Code = h.branch WHERE b.Name = ?)")
        params.append(sucursal.strip())
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT  q.DocEntry, q.DocNum, q.DocDate, q.U_ODS,
                        s.SlpName,
                        q.Filler    AS FromWhs,
                        wf.WhsName  AS FromWhsName,
                        q.ToWhsCode AS ToWhs,
                        wt.WhsName  AS ToWhsName,
                        (SELECT COUNT(*) FROM WTQ1 l WHERE l.DocEntry = q.DocEntry) AS Lineas,
                        c.custmrName AS OdsCliente,
                        c.createDate AS OdsFecha
                FROM    OWTQ q
                LEFT    JOIN OSLP s  ON s.SlpCode  = q.SlpCode
                LEFT    JOIN OWHS wf ON wf.WhsCode = q.Filler
                LEFT    JOIN OWHS wt ON wt.WhsCode = q.ToWhsCode
                LEFT    JOIN OSCL c  ON CAST(c.callID AS NVARCHAR(20)) = q.U_ODS
                WHERE   q.DocStatus = 'O'
                  AND   ISNULL(q.CANCELED, 'N') <> 'Y'
                  AND   q.U_ODS IS NOT NULL AND q.U_ODS <> ''
                  {filtro_suc}
                ORDER BY q.DocEntry DESC
                """,
                params,
            )
            por_ods: Dict[str, Dict[str, Any]] = {}
            total = 0
            for r in cursor.fetchall():
                ods = (r.U_ODS or "").strip()
                grupo = por_ods.setdefault(ods, {
                    "ods":       int(ods) if ods.isdigit() else None,
                    "cliente":   (r.OdsCliente or "").strip() or None,
                    "odsFecha":  r.OdsFecha.date().isoformat() if r.OdsFecha else None,
                    "solicitudes": [],
                })
                grupo["solicitudes"].append({
                    "docEntry": int(r.DocEntry),
                    "docNum":   int(r.DocNum),
                    "fecha":    r.DocDate.date().isoformat() if r.DocDate else None,
                    "fromWhs":  {"code": (r.FromWhs or "").strip(), "name": (r.FromWhsName or "").strip() or None},
                    "toWhs":    {"code": (r.ToWhs or "").strip(),   "name": (r.ToWhsName or "").strip() or None},
                    "vendedor": (r.SlpName or "").strip() or None,
                    "lineas":   int(r.Lineas or 0),
                })
                total += 1

            grupos = sorted(por_ods.values(), key=lambda g: (g["ods"] or 0), reverse=True)
            return {"success": True, "message": None,
                    "data": {"grupos": grupos, "total": total, "ods": len(grupos)}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/transferRequests/{doc_entry}/ticket",
    summary="Datos del ticket térmico de una solicitud de traslado",
)
def transfer_request_ticket(
    doc_entry: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT  q.DocEntry, q.DocNum, q.DocDate, q.DocStatus, q.CANCELED,
                        q.Comments, q.U_ODS, q.SlpCode,
                        s.SlpName,
                        q.Filler    AS FromWhs,
                        wf.WhsName  AS FromWhsName,
                        q.ToWhsCode AS ToWhs,
                        wt.WhsName  AS ToWhsName
                FROM    OWTQ q
                LEFT    JOIN OSLP s  ON s.SlpCode  = q.SlpCode
                LEFT    JOIN OWHS wf ON wf.WhsCode = q.Filler
                LEFT    JOIN OWHS wt ON wt.WhsCode = q.ToWhsCode
                WHERE   q.DocEntry = ?
                """,
                [doc_entry],
            )
            h = cursor.fetchone()
            if not h:
                return err(404, f"La solicitud de traslado {doc_entry} no existe.")

            cursor.execute(
                """
                SELECT  LineNum, ItemCode, Dscription, Quantity,
                        FromWhsCod, WhsCode
                FROM    WTQ1
                WHERE   DocEntry = ?
                ORDER BY LineNum
                """,
                [doc_entry],
            )
            lineas = []
            piezas = 0.0
            for l in cursor.fetchall():
                qty = float(l.Quantity or 0)
                piezas += qty
                lineas.append({
                    "itemCode": l.ItemCode,
                    "name":     l.Dscription,
                    "quantity": qty,
                    "fromWhs":  (l.FromWhsCod or "").strip(),
                    "toWhs":    (l.WhsCode or "").strip(),
                })

            cursor.execute("SELECT CompnyName, CompnyAddr, TaxIdNum FROM OADM")
            adm = cursor.fetchone()
            emisor_lineas = [ln.strip() for ln in str(adm.CompnyAddr or "").replace("\r\n", "\r")
                             .replace("\n", "\r").split("\r") if ln.strip()]

            return {
                "success": True, "message": None,
                "data": {
                    "docEntry": int(h.DocEntry),
                    "folio":    int(h.DocNum),
                    "fecha":    h.DocDate.date().isoformat() if h.DocDate else None,
                    "estatus":  _estatus(h.DocStatus, h.CANCELED),
                    "ods":      (h.U_ODS or "").strip() or None,
                    "vendedor": (h.SlpName or "").strip() or None,
                    "fromWhs":  {"code": (h.FromWhs or "").strip(), "name": (h.FromWhsName or "").strip() or None},
                    "toWhs":    {"code": (h.ToWhs or "").strip(),   "name": (h.ToWhsName or "").strip() or None},
                    "comments": (h.Comments or "").strip() or None,
                    "emisor": {
                        "nombre":    (adm.CompnyName or "").strip(),
                        "direccion": emisor_lineas,
                        "rfc":       (adm.TaxIdNum or "").strip(),
                    },
                    "lineas":        lineas,
                    "totalArticulos": len(lineas),
                    "totalPiezas":    piezas,
                },
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
