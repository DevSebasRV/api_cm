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

from fastapi import APIRouter, Header, Body
from typing import Optional, List, Dict, Any
import re
import pyodbc

from app.database import get_connection
from app.routers.shopify import resolve_db, err

router = APIRouter(tags=["Conciliación CFDI"])

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
            # 1) Facturas SAP con ALGÚN valor de UUID en una ventana amplia
            #    (dateFrom - 1 año) — el match por UUID no debe depender de la
            #    fecha de captura en SAP, pero acotamos el escaneo por tamaño.
            cursor.execute(
                _OPCH_SELECT + """
                WHERE   OPCH.DocDate >= DATEADD(year, -1, ?)
                  AND ( LTRIM(RTRIM(ISNULL(OPCH.U_CVM_BFOLIOUUID,''))) <> ''
                     OR LTRIM(RTRIM(ISNULL(OPCH.U_UUID,'')))           <> '' )
                """,
                [dateFrom],
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
            #    y "en SAP pero no en la descarga").
            cursor.execute(
                _OPCH_SELECT + " WHERE OPCH.DocDate >= ? AND OPCH.DocDate <= ?",
                [dateFrom, dateTo],
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
