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
import unicodedata
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


# Filtro de técnicos (heredado de la app de Retool "Q_Tecnico"): excluir
# administración y ventas. La lista original era por CÓDIGO de departamento,
# pero los códigos significan cosas DISTINTAS en cada empresa (en PROSHOP el
# código 1 es el taller "Serv Pat Suzuki" y bloqueaba a sus técnicos). Ahora
# se excluye por NOMBRE (normalizado sin acentos), correcto en ambas bases.
_DEPARTAMENTOS_EXCLUIDOS = {
    "general", "gerencia de ventas", "venta motos", "venta ref. y acc.",
    "gerente tienda", "caja", "contabilidad", "sistemas", "direccion",
    "compras", "boutique", "refacciones", "motos", "cuentas por cobrar",
    "almacen", "venta de moto suzuki",
}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip().lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _codigos_dep_excluidos(cursor) -> List[int]:
    """-2 (sin departamento) + los departamentos administrativos POR NOMBRE."""
    codes = [-2]
    try:
        cursor.execute("SELECT Code, Name FROM OUDP")
        for r in cursor.fetchall():
            if _norm(r.Name) in _DEPARTAMENTOS_EXCLUIDOS:
                codes.append(int(r.Code))
    except pyodbc.Error:
        pass
    return codes


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
            excl = _codigos_dep_excluidos(cursor)
            marks = ",".join("?" * len(excl))
            cursor.execute(
                f"SELECT T0.USERID, T0.USER_CODE, T0.U_NAME "
                f"FROM OUSR T0 "
                f"WHERE T0.U_NAME NOT IN ('AV') "
                f"  AND T0.DEPARTMENT NOT IN ({marks}) "
                f"ORDER BY T0.U_NAME",
                excl,
            )
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
