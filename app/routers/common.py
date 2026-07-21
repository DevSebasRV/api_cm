"""
Helpers HTTP compartidos por todos los routers.

Viven aquí (y no dentro de un router de negocio) para que ningún módulo tenga
que importar de otro con el que no comparte dominio.
"""

from typing import Any, Dict, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from app.config import EMPRESAS

# Si el cliente no manda X-SAP-DB usamos la base de TEST por seguridad
# (jamás escribimos por accidente sobre producción).
DEFAULT_DB_KEY = "test"


def resolve_db(x_sap_db: Optional[str]) -> tuple[str, str]:
    """Resuelve la base SAP B1 a usar a partir del header X-SAP-DB.
    Si no se manda el header, cae al default (`test`).
    Devuelve (db_key, database_name)."""
    key = (x_sap_db or DEFAULT_DB_KEY).lower()
    if key not in EMPRESAS:
        raise HTTPException(
            status_code=400,
            detail=f"X-SAP-DB '{x_sap_db}' no válida. Usa: {list(EMPRESAS.keys())}.",
        )
    database = EMPRESAS[key]
    if not database:
        raise HTTPException(
            status_code=500,
            detail=f"La base '{key}' no está configurada en .env "
                   f"(falta SAP_DATABASE_{key.upper()}).",
        )
    return key, database


def err(status: int, message: str, extra: Optional[dict] = None):
    """Respuesta de error estándar {success, message, data}. `extra` agrega campos."""
    body: Dict[str, Any] = {"success": False, "message": message, "data": None}
    if extra:
        body.update(extra)
    return JSONResponse(status_code=status, content=body)


def _pagination(page: int, page_size: int, total: int) -> Dict[str, Any]:
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "page":       page,
        "pageSize":   page_size,
        "total":      total,
        "totalPages": total_pages,
    }
