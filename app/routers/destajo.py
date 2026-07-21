"""
Destajo por técnico (migración de la app de Retool "Analisis Servicio Tecnico").

- El destajo se calcula con el SP `SP_DESTAJO_X_CLASE_MECANICO_RETOOL` (existe
  en FERBEL y PROSHOP): @Start_Date, @End_Date, @Tecnico.
- @Tecnico se compara contra OUSR.U_NAME — el USUARIO SAP asignado a la ODS
  (OSCL.assignee). La clase de tarifa del técnico vive en OUSR.Fax ('A'/'B') y
  las tarifas en OITM.U_Dest_A / U_Dest_B de los artículos de mano de obra.
- En el portal, el técnico NO se elige: viene de la asignación del usuario
  logueado (users.sap_tecnico_fn / sap_tecnico_cp en Postgres). Este backend
  solo ejecuta lo que le pidan — la autorización la aplica el portal.
"""

from fastapi import APIRouter, Header, Body
from typing import Optional, Any, Dict, List
from decimal import Decimal
import datetime
import pyodbc

from app.database import get_connection
from app.routers.common import resolve_db, err

router = APIRouter(tags=["Destajo"])


def _val(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


# Mismo filtro que la app de Retool (Q_Tecnico): usuarios SAP que son técnicos
# (excluye departamentos administrativos y al usuario 'AV').
_TECNICOS_SQL = """
    SELECT  T0.USERID, T0.USER_CODE, T0.U_NAME
    FROM    OUSR T0
    WHERE   T0.U_NAME NOT IN ('AV')
      AND   T0.DEPARTMENT NOT IN (-2,9,1,3,10,20,4,6,8,1,2,11)
    ORDER BY T0.U_NAME
"""


@router.get(
    "/destajoTecnicos",
    summary="Técnicos SAP (OUSR) para asignar a usuarios del portal",
)
def list_destajo_tecnicos(
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            cursor.execute(_TECNICOS_SQL)
            tecnicos = [
                {"userId": int(r.USERID), "userCode": r.USER_CODE, "name": (r.U_NAME or "").strip()}
                for r in cursor.fetchall() if (r.U_NAME or "").strip()
            ]
            return {"success": True, "message": None, "tecnicos": tecnicos}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")


@router.post(
    "/destajo",
    summary="Destajo por técnico (SP_DESTAJO_X_CLASE_MECANICO_RETOOL)",
)
def get_destajo(
    dateFrom: str           = Body(..., embed=True, description="YYYY-MM-DD"),
    dateTo:   str           = Body(..., embed=True, description="YYYY-MM-DD"),
    tecnico:  str           = Body(..., embed=True, description="OUSR.U_NAME exacto"),
    x_sap_db: Optional[str] = Header(default=None, alias="X-SAP-DB"),
):
    _, database = resolve_db(x_sap_db)
    if not (tecnico or "").strip():
        return err(400, "Falta el técnico.")

    try:
        conn   = get_connection(database)
        cursor = conn.cursor()
        try:
            # SET NOCOUNT ON: el SP hace INSERTs a una tabla temporal antes del
            # SELECT final; sin esto pyodbc se queda en el rowcount del INSERT.
            cursor.execute(
                "SET NOCOUNT ON; "
                "EXEC [SP_DESTAJO_X_CLASE_MECANICO_RETOOL] @Start_Date=?, @End_Date=?, @Tecnico=?",
                [dateFrom, dateTo, tecnico.strip()],
            )
            # Avanza hasta el primer result set con filas (por si el SP emite varios).
            while cursor.description is None:
                if not cursor.nextset():
                    return {"success": True, "message": None, "rows": []}
            cols = [c[0] for c in cursor.description]
            rows: List[Dict[str, Any]] = [
                {col: _val(v) for col, v in zip(cols, r)} for r in cursor.fetchall()
            ]
            return {"success": True, "message": None, "rows": rows}
        finally:
            cursor.close()
            conn.close()
    except pyodbc.Error as db_err:
        return err(500, f"Error de SAP B1: {db_err}")
    except Exception as e:
        return err(500, f"Error interno: {e}")
