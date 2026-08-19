"""
Solicitudes de traslado (OWTQ/WTQ1) ligadas a una ODS.

La liga vive en el UDF OWTQ.U_ODS (creado 2026-07-31 en las 3 bases vía
UserFieldsMD): el portal lo escribe al crear la solicitud. La columna
"Filler" de OWTQ es el almacén ORIGEN (nombre heredado de SAP); ToWhsCode
es el destino.
"""

from fastapi import APIRouter, Header
from typing import Optional, Any, Dict, List
import re
import pyodbc

from app.database import get_connection
from app.routers.common import resolve_db, err

router = APIRouter(tags=["Traslados"])

_STATUS = {"O": "Abierta", "C": "Cerrada"}

# SAP arma OSLP.SlpName como "CODIGO .- Nombre de la persona" (el separador
# varía: '.-', '.- ', ' .-'). Quitando ese prefijo quedan agrupados los varios
# códigos de vendedor de un mismo asesor, que es como se sabe cuáles almacenes
# son suyos.
_PREFIJO_CODIGO = re.compile(r"^\s*\S+\s*\.-\s*")


def _persona_de_slpname(slp_name: Optional[str]) -> str:
    return _PREFIJO_CODIGO.sub("", (slp_name or "").strip()).strip().lower()


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
    "/salespersonWarehouses",
    summary="Almacenes del asesor (destino permitido de un traslado)",
)
def salesperson_warehouses(
    slpCode: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Almacenes DEL ASESOR, para el destino del traslado.

    Un mismo asesor tiene VARIOS códigos de vendedor en OSLP —uno por marca— y
    cada uno con su almacén en `OSLP.Telephone` (verificado: es el campo que
    manda; el "Almacén origen" de esa ventana de SAP es `Email` y no lo usa
    ningún documento). Ej.: Christian Dominguez tiene 5 códigos → 4 almacenes
    (BRPSERV, KTMSERV, YASERPAT, YASERV). 46 asesores tienen más de uno.

    Los códigos se agrupan por el NOMBRE de la persona, quitando el prefijo
    "CODIGO .-" con el que SAP arma SlpName. No hay forma mejor: `OSLP.EmpID`
    (el enlace a Datos maestros de empleado) está en CERO en los 274 vendedores.

    Antes se devolvían todos los almacenes de la LOCALIDAD (27 para Satélite,
    con boutique y bodegas incluidas). Si el asesor no tiene ninguno (exento con
    '.'), se devuelven todos: mejor no filtrar que dejarlo trabado."""
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Nombre de la persona detrás del código elegido.
            cursor.execute("SELECT SlpName FROM OSLP WHERE SlpCode = ?", [slpCode])
            fila = cursor.fetchone()
            persona = _persona_de_slpname(fila.SlpName if fila else "")

            # Sus almacenes = los de TODOS sus códigos de vendedor. El match por
            # nombre se hace en Python (SQL Server no normaliza acentos igual).
            codigos = []
            if persona:
                cursor.execute(
                    "SELECT SlpName, LTRIM(RTRIM(ISNULL(Telephone,''))) AS Whs "
                    "FROM OSLP WHERE ISNULL(Active,'Y') = 'Y' AND SlpCode >= 0"
                )
                codigos = [
                    r.Whs for r in cursor.fetchall()
                    if r.Whs and r.Whs != "." and _persona_de_slpname(r.SlpName) == persona
                ]

            rows = []
            if codigos:
                unicos = sorted(set(codigos))
                marcas = ",".join("?" * len(unicos))
                cursor.execute(
                    f"""
                    SELECT  w.WhsCode, w.WhsName, ISNULL(l.Location, '') AS Location
                    FROM    OWHS w
                    LEFT    JOIN OLCT l ON l.Code = w.Location
                    WHERE   w.WhsCode IN ({marcas})
                    ORDER BY w.WhsName
                    """,
                    unicos,
                )
                rows = cursor.fetchall()

            filtrado = bool(rows)
            if not rows:                      # asesor exento / sin almacenes
                cursor.execute(
                    "SELECT w.WhsCode, w.WhsName, ISNULL(l.Location,'') AS Location "
                    "FROM OWHS w LEFT JOIN OLCT l ON l.Code = w.Location ORDER BY w.WhsName"
                )
                rows = cursor.fetchall()

            almacenes = [
                {"code": (r.WhsCode or "").strip(),
                 "name": (r.WhsName or "").strip(),
                 "location": (r.Location or "").strip() or None}
                for r in rows
            ]
            return {"success": True, "message": None,
                    "data": {"warehouses": almacenes, "filtered": filtrado}}
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
    "/transfersDone",
    summary="Traslados REALIZADOS (OWTR) rastreables a una ODS",
)
def list_transfers_done(
    ods:      Optional[int] = None,
    sucursal: Optional[str] = None,
    limit:    int = 100,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    """Traslados de stock ya ejecutados (OWTR) ligados a una ODS por dos vías:
    su propio U_ODS (SAP lo copia a veces al crear desde la solicitud) o la
    solicitud base (WTR1.BaseType=1250000001 → OWTQ.U_ODS). Con `ods` se filtra
    a esa orden (pestaña de la ODS); sin él se listan los recientes (módulo
    Traslados), opcionalmente acotados a la `sucursal` del asesor de la ODS."""
    _, database = resolve_db(x_sap_db)
    limit = max(1, min(int(limit), 300))

    filtros = ""
    params: List[Any] = []
    if ods is not None:
        filtros += " AND COALESCE(NULLIF(LTRIM(RTRIM(t.U_ODS)), ''), b.U_ODS) = ?"
        params.append(str(ods))
    if sucursal and sucursal.strip():
        filtros += (" AND c.technician IN (SELECT h.empID FROM OHEM h "
                    "JOIN OUBR br ON br.Code = h.branch WHERE br.Name = ?)")
        params.append(sucursal.strip())

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT  TOP {limit}
                        t.DocEntry, t.DocNum, t.DocDate,
                        s.SlpName,
                        t.Filler    AS FromWhs,
                        wf.WhsName  AS FromWhsName,
                        t.ToWhsCode AS ToWhs,
                        wt.WhsName  AS ToWhsName,
                        (SELECT COUNT(*) FROM WTR1 x WHERE x.DocEntry = t.DocEntry) AS Lineas,
                        COALESCE(NULLIF(LTRIM(RTRIM(t.U_ODS)), ''), b.U_ODS) AS Ods,
                        b.SolicitudNum,
                        c.custmrName AS OdsCliente
                FROM    OWTR t
                OUTER   APPLY (
                            SELECT TOP 1 q.U_ODS, q.DocNum AS SolicitudNum
                            FROM   WTR1 l
                            JOIN   OWTQ q ON q.DocEntry = l.BaseEntry
                            WHERE  l.DocEntry = t.DocEntry
                              AND  l.BaseType = 1250000001
                              AND  ISNULL(q.U_ODS, '') <> ''
                        ) b
                LEFT    JOIN OSLP s  ON s.SlpCode  = t.SlpCode
                LEFT    JOIN OWHS wf ON wf.WhsCode = t.Filler
                LEFT    JOIN OWHS wt ON wt.WhsCode = t.ToWhsCode
                LEFT    JOIN OSCL c  ON CAST(c.callID AS NVARCHAR(20)) =
                                        COALESCE(NULLIF(LTRIM(RTRIM(t.U_ODS)), ''), b.U_ODS)
                WHERE   COALESCE(NULLIF(LTRIM(RTRIM(t.U_ODS)), ''), b.U_ODS) IS NOT NULL
                  {filtros}
                ORDER BY t.DocEntry DESC
                """,
                params,
            )
            traslados = [
                {
                    "docEntry":     int(r.DocEntry),
                    "docNum":       int(r.DocNum),
                    "fecha":        r.DocDate.date().isoformat() if r.DocDate else None,
                    "ods":          int(r.Ods) if (r.Ods or "").strip().isdigit() else None,
                    "cliente":      (r.OdsCliente or "").strip() or None,
                    "solicitudNum": int(r.SolicitudNum) if r.SolicitudNum else None,
                    "fromWhs":      {"code": (r.FromWhs or "").strip(), "name": (r.FromWhsName or "").strip() or None},
                    "toWhs":        {"code": (r.ToWhs or "").strip(),   "name": (r.ToWhsName or "").strip() or None},
                    "vendedor":     (r.SlpName or "").strip() or None,
                    "lineas":       int(r.Lineas or 0),
                }
                for r in cursor.fetchall()
            ]
            return {"success": True, "message": None,
                    "data": {"traslados": traslados, "total": len(traslados)}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


def _ticket_traslado(doc_entry: int, database: str, cabecera: str, lineas_tbl: str,
                     etiqueta: str):
    """Datos del ticket térmico. Sirve igual para la SOLICITUD (OWTQ/WTQ1) y para
    el TRASLADO ya hecho (OWTR/WTR1): las dos tablas tienen la misma forma —
    Filler es el almacén origen, ToWhsCode el destino."""
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT  q.DocEntry, q.DocNum, q.DocDate, q.DocStatus, q.CANCELED,
                        q.Comments, q.U_ODS, q.SlpCode,
                        s.SlpName,
                        q.Filler    AS FromWhs,
                        wf.WhsName  AS FromWhsName,
                        q.ToWhsCode AS ToWhs,
                        wt.WhsName  AS ToWhsName
                FROM    {cabecera} q
                LEFT    JOIN OSLP s  ON s.SlpCode  = q.SlpCode
                LEFT    JOIN OWHS wf ON wf.WhsCode = q.Filler
                LEFT    JOIN OWHS wt ON wt.WhsCode = q.ToWhsCode
                WHERE   q.DocEntry = ?
                """,
                [doc_entry],
            )
            h = cursor.fetchone()
            if not h:
                return err(404, f"{etiqueta} {doc_entry} no existe.")

            cursor.execute(
                f"""
                SELECT  LineNum, ItemCode, Dscription, Quantity,
                        FromWhsCod, WhsCode
                FROM    {lineas_tbl}
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


@router.get(
    "/transferRequests/{doc_entry}/ticket",
    summary="Datos del ticket térmico de una solicitud de traslado",
)
def transfer_request_ticket(
    doc_entry: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    return _ticket_traslado(doc_entry, database, "OWTQ", "WTQ1",
                            "La solicitud de traslado")


@router.get(
    "/transfersDone/{doc_entry}/ticket",
    summary="Datos del ticket térmico de un traslado ya realizado",
)
def transfer_done_ticket(
    doc_entry: int,
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    return _ticket_traslado(doc_entry, database, "OWTR", "WTR1",
                            "El traslado")
