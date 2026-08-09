"""
Conciliación de CFDIs recibidos (Descarga Masiva de Facturapi) vs facturas de
proveedor capturadas en SAP B1 (OPCH).

El match es por UUID fiscal:
  - Campo principal: OPCH.U_CVM_BFOLIOUUID ("Folio UUID") — limpio, lo llenan
    manualmente al capturar (apenas se está adoptando).
  - Rescate histórico: OPCH.U_UUID — campo viejo y SUCIO (URLs de verificación
    del SAT, valores con '=' al inicio, truncados). Muchas URLs contienen el
    UUID adentro (?id=<uuid>), así que se extrae con regex; un UUID válido
    completo (8-4-4-4-12 hex) no da falsos positivos.

El portal manda la lista de CFDIs (uuid) + el rango de fechas; aquí se devuelven
los matches SAP por UUID (SIN restringir por fecha: un CFDI de junio pudo
capturarse en SAP con fecha de julio) y las facturas SAP del rango (para los
reportes de "sin UUID" y "en SAP pero no en la descarga").
"""

from fastapi import APIRouter, Header, Body, Query
from typing import Optional, List, Dict, Any
import re
import pyodbc

from app.database import get_connection
from app.routers.common import resolve_db, err

router = APIRouter(tags=["Conciliación CFDI"])

# Piso de fechas para TODAS las búsquedas de coincidencias en SAP (pedido del
# cliente, ago-2026): solo se consideran facturas de proveedor con DocDate del
# 2026-01-01 en adelante — matches por UUID, candidatas de "Relacionar",
# facturas sin UUID y los reportes del rango.
FECHA_PISO = "2026-01-01"

# UUID fiscal completo (formato 8-4-4-4-12 hex). Se busca DENTRO del texto para
# rescatar los que vienen embebidos en URLs de verificación del SAT.
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _extract_uuid(*values) -> Optional[str]:
    """Primer UUID válido encontrado en los valores dados (normalizado a MAYÚSCULAS)."""
    for v in values:
        if not v:
            continue
        m = _UUID_RE.search(str(v))
        if m:
            return m.group(0).upper()
    return None


def _row_to_invoice(r) -> Dict[str, Any]:
    return {
        "DocEntry":  int(r.DocEntry),
        "DocNum":    int(r.DocNum),
        "CardCode":  r.CardCode,
        "CardName":  r.CardName,
        "DocDate":   r.DocDate.isoformat() if r.DocDate else None,
        "DocTotal":  float(r.DocTotal) if r.DocTotal is not None else 0.0,
        "DocCur":    (r.DocCur or "").strip() or None,
        "Canceled":  (r.CANCELED or "N") == "Y",
        # UUID normalizado (del campo limpio o rescatado del sucio)
        "uuid":      _extract_uuid(r.FolioUUID, r.UUIDViejo),
        "uuidSource": "folio" if _extract_uuid(r.FolioUUID) else ("legacy" if _extract_uuid(r.UUIDViejo) else None),
    }


_OPCH_SELECT = """
    SELECT  OPCH.DocEntry,
            OPCH.DocNum,
            OPCH.CardCode,
            OPCH.CardName,
            OPCH.DocDate,
            OPCH.DocTotal,
            OPCH.DocCur,
            OPCH.CANCELED,
            OPCH.U_CVM_BFOLIOUUID AS FolioUUID,
            OPCH.U_UUID           AS UUIDViejo
    FROM    OPCH
"""


@router.post(
    "/cfdiReconcile",
    summary="Concilia CFDIs recibidos (por UUID) contra facturas de proveedor (OPCH)",
)
def cfdi_reconcile(
    dateFrom: str            = Body(..., embed=True, description="YYYY-MM-DD (rango del reporte)"),
    dateTo:   str            = Body(..., embed=True, description="YYYY-MM-DD"),
    uuids:    List[str]      = Body(default=[], embed=True, description="UUIDs de los CFDIs descargados"),
    x_sap_db: Optional[str]  = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)

    wanted = {u.strip().upper() for u in (uuids or []) if u and u.strip()}

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # 1) Facturas SAP con ALGÚN valor de UUID, desde el piso de fechas
            #    (antes: ventana dateFrom - 1 año).
            cursor.execute(
                _OPCH_SELECT + """
                WHERE   OPCH.DocDate >= ?
                  AND ( LTRIM(RTRIM(ISNULL(OPCH.U_CVM_BFOLIOUUID,''))) <> ''
                     OR LTRIM(RTRIM(ISNULL(OPCH.U_UUID,'')))           <> '' )
                """,
                [FECHA_PISO],
            )
            matches: Dict[str, Dict[str, Any]] = {}
            for r in cursor.fetchall():
                inv = _row_to_invoice(r)
                u = inv["uuid"]
                if u and (not wanted or u in wanted):
                    # Si hay UUID duplicado en SAP, gana el DocEntry más reciente
                    prev = matches.get(u)
                    if not prev or inv["DocEntry"] > prev["DocEntry"]:
                        matches[u] = inv

            # 2) TODAS las facturas SAP del rango del reporte (para "sin UUID"
            #    y "en SAP pero no en la descarga"), nunca antes del piso.
            cursor.execute(
                _OPCH_SELECT + " WHERE OPCH.DocDate >= ? AND OPCH.DocDate >= ? AND OPCH.DocDate <= ?",
                [FECHA_PISO, dateFrom, dateTo],
            )
            in_range = [_row_to_invoice(r) for r in cursor.fetchall()]

            return {
                "success":  True,
                "message":  None,
                "matches":  matches,          # {uuid: factura SAP}
                "inRange":  in_range,         # facturas SAP del rango (con o sin uuid)
            }
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/cfdiMatchCandidates",
    summary="Facturas de proveedor (OPCH) candidatas para un CFDI sin capturar",
)
def cfdi_match_candidates(
    rfc:      str            = Query(..., description="RFC del proveedor emisor del CFDI"),
    total:    float          = Query(..., description="Total del CFDI"),
    tol:      float          = Query(default=1.0, description="Tolerancia de importe (pesos)"),
    limit:    int            = Query(default=15, ge=1, le=50),
    x_sap_db: Optional[str]  = Header(default=None, alias="X-SAP-DB"),
):
    """
    Para la pestaña "Faltan por capturar": dado el RFC del proveedor y el total
    del CFDI, busca facturas de proveedor en SAP (OPCH) SIN UUID que coincidan
    en AMBOS — mismo RFC (OCRD.LicTradNum) Y el total dentro de la tolerancia —
    para que el usuario elija una y se le grabe el UUID.

    Se exige RFC Y total a propósito: solo por importe habría demasiadas
    coincidencias de otros proveedores, y un RFC que no exista como socio en SAP
    no puede tener factura (no hay match real).
    """
    _, database = resolve_db(x_sap_db)
    rfc = (rfc or "").strip().upper()

    def _view(r) -> Dict[str, Any]:
        return {
            "docEntry": int(r.DocEntry),
            "docNum":   int(r.DocNum),
            "cardCode": r.CardCode,
            "cardName": r.CardName,
            "docDate":  r.DocDate.date().isoformat() if r.DocDate else None,
            "docTotal": float(r.DocTotal) if r.DocTotal is not None else 0.0,
            "docCur":   (r.DocCur or "").strip() or None,
            "diff":     round(float(r.Dif), 2),
            "exact":    float(r.Dif) <= tol,
        }

    _NO_UUID = ("LTRIM(RTRIM(ISNULL(p.U_CVM_BFOLIOUUID,''))) = '' "
                "AND LTRIM(RTRIM(ISNULL(p.U_UUID,''))) = ''")

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # Coincidencia por RFC del proveedor Y total (dentro de la tolerancia),
            # solo facturas desde el piso de fechas (antes: sin límite de fecha).
            cursor.execute(
                f"""
                SELECT TOP {int(limit)}
                       p.DocEntry, p.DocNum, p.CardCode, p.CardName, p.DocDate,
                       p.DocTotal, p.DocCur, ABS(p.DocTotal - ?) AS Dif
                FROM   OPCH p JOIN OCRD c ON c.CardCode = p.CardCode
                WHERE  c.LicTradNum = ? AND p.CANCELED = 'N' AND {_NO_UUID}
                  AND  p.DocDate >= ?
                  AND  ABS(p.DocTotal - ?) <= ?
                ORDER BY Dif, p.DocEntry DESC
                """,
                [float(total), rfc, FECHA_PISO, float(total), tol],
            )
            rows = [_view(r) for r in cursor.fetchall()]

            return {"success": True, "message": None,
                    "data": {"candidates": rows, "mode": "rfc" if rows else "none"}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.post(
    "/cfdiMatchBySerial",
    summary="Facturas de proveedor (OPCH) que recibieron esos números de serie (motos)",
)
def cfdi_match_by_serial(
    serials:  List[str]      = Body(..., embed=True, description="Números de serie (VIN/NIV) del CFDI"),
    x_sap_db: Optional[str]  = Header(default=None, alias="X-SAP-DB"),
):
    """
    Para relacionar facturas de MOTOS: el CFDI trae el VIN/NIV de cada unidad y
    SAP registra los seriales recibidos por factura de compra. La cadena es
    OSRN (serial: DistNumber=VIN, MnfSerial=motor) → ITL1/OITL (log de
    transacciones, DocType 18 = factura de proveedor) → OPCH.

    Mismos filtros que las demás candidatas: sin UUID capturado, no cancelada y
    DocDate desde el piso de fechas. Devuelve qué seriales matchearon en cada
    factura. Verificado con caso real: VIN VBKTS3403TH705835 → factura #65004.
    """
    _, database = resolve_db(x_sap_db)
    limpios = sorted({s.strip().upper() for s in (serials or []) if s and s.strip()})
    if not limpios:
        return {"success": True, "message": None, "data": {"candidates": []}}
    if len(limpios) > 100:
        limpios = limpios[:100]

    _NO_UUID = ("LTRIM(RTRIM(ISNULL(p.U_CVM_BFOLIOUUID,''))) = '' "
                "AND LTRIM(RTRIM(ISNULL(p.U_UUID,''))) = ''")
    marks = ",".join("?" * len(limpios))

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT DISTINCT
                       p.DocEntry, p.DocNum, p.CardCode, p.CardName, p.DocDate,
                       p.DocTotal, p.DocCur, s.DistNumber,
                       (SELECT COUNT(DISTINCT s2.AbsEntry)
                        FROM OITL t2
                        JOIN ITL1 l2 ON l2.LogEntry = t2.LogEntry
                        JOIN OSRN s2 ON s2.AbsEntry = l2.MdAbsEntry
                        WHERE t2.DocType = 18 AND t2.DocEntry = p.DocEntry) AS TotalUnidades
                FROM   OITL t
                JOIN   ITL1 l ON l.LogEntry = t.LogEntry
                JOIN   OSRN s ON s.AbsEntry = l.MdAbsEntry
                JOIN   OPCH p ON p.DocEntry = t.DocEntry
                WHERE  t.DocType = 18
                  AND  (s.DistNumber IN ({marks}) OR s.MnfSerial IN ({marks}))
                  AND  p.CANCELED = 'N' AND {_NO_UUID}
                  AND  p.DocDate >= ?
                """,
                limpios + limpios + [FECHA_PISO],
            )
            por_factura: Dict[int, Dict[str, Any]] = {}
            for r in cursor.fetchall():
                inv = por_factura.setdefault(int(r.DocEntry), {
                    "docEntry": int(r.DocEntry),
                    "docNum":   int(r.DocNum),
                    "cardCode": r.CardCode,
                    "cardName": r.CardName,
                    "docDate":  r.DocDate.date().isoformat() if r.DocDate else None,
                    "docTotal": float(r.DocTotal) if r.DocTotal is not None else 0.0,
                    "docCur":   (r.DocCur or "").strip() or None,
                    # Total de unidades (series) que recibió la factura: si es
                    # MAYOR a las que ampara el CFDI, la relación sería parcial
                    # (el campo de UUID es de uno) y el portal la bloquea.
                    "totalUnidades": int(r.TotalUnidades or 0),
                    "serials":  [],
                })
                serie = (r.DistNumber or "").strip().upper()
                if serie and serie not in inv["serials"]:
                    inv["serials"].append(serie)

            candidates = sorted(por_factura.values(),
                                key=lambda i: (-len(i["serials"]), -i["docEntry"]))
            return {"success": True, "message": None, "data": {"candidates": candidates}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")


@router.get(
    "/cfdiUncapturedInvoices",
    summary="Facturas de proveedor (OPCH) SIN UUID: RFC + total (para marcar filas)",
)
def cfdi_uncaptured_invoices(
    months:   int            = Query(default=18, ge=1, le=60),
    x_sap_db: Optional[str]  = Header(default=None, alias="X-SAP-DB"),
):
    """
    Lista compacta {rfc, total} de facturas de proveedor sin UUID de los últimos
    N meses. El portal la usa para saber, por adelantado, qué CFDIs de "Faltan
    por capturar" tienen candidata (colorear el botón y ordenar), sin una
    consulta por fila.
    """
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT o.LicTradNum AS rfc, p.DocTotal
                FROM   OPCH p JOIN OCRD o ON o.CardCode = p.CardCode
                WHERE  p.CANCELED = 'N'
                  AND  LTRIM(RTRIM(ISNULL(p.U_CVM_BFOLIOUUID,''))) = ''
                  AND  LTRIM(RTRIM(ISNULL(p.U_UUID,''))) = ''
                  AND  o.LicTradNum IS NOT NULL AND LTRIM(RTRIM(o.LicTradNum)) <> ''
                  AND  p.DocDate >= DATEADD(month, -{int(months)}, GETDATE())
                  AND  p.DocDate >= ?
                """,
                [FECHA_PISO],
            )
            invoices = [
                {"rfc": (r.rfc or "").strip().upper(),
                 "total": float(r.DocTotal) if r.DocTotal is not None else 0.0}
                for r in cursor.fetchall()
            ]
            return {"success": True, "message": None,
                    "data": {"invoices": invoices}}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
