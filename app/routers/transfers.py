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
