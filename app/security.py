"""
Autenticación por API key para los endpoints de Shopify.

Las keys se configuran en `.env` con el formato:

    SHOPIFY_API_KEYS=agency_alpha:abc123secret,internal_test:def456secret

- Cada par `label:key` separado por coma.
- El label es solo para logs/audit (qué cliente está llamando).
- Si una entrada no trae `:`, se le asigna `label = "default"`.

Se compara con `secrets.compare_digest` para evitar timing attacks.
"""

import os
import secrets
from typing import Dict, Optional

from fastapi import Header, HTTPException, status


def _load_keys() -> Dict[str, str]:
    """Devuelve un dict {key: label} a partir de la env var SHOPIFY_API_KEYS."""
    raw = (os.getenv("SHOPIFY_API_KEYS") or "").strip()
    if not raw:
        return {}

    result: Dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            label, key = entry.split(":", 1)
            label = label.strip() or "default"
            key   = key.strip()
        else:
            label, key = "default", entry
        if key:
            result[key] = label
    return result


# Se cargan UNA sola vez al arrancar el proceso. Si cambias .env hay que
# reiniciar pm2 con `--update-env` (o el equivalente).
API_KEYS: Dict[str, str] = _load_keys()


def require_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> str:
    """
    FastAPI dependency.
    - Si la lista de keys está vacía → 503 (fail-closed: nunca abre por error de config).
    - Si el header X-API-Key falta → 401.
    - Si la key no coincide con ninguna conocida → 401.
    - Si coincide → devuelve el `label` (útil para logs).
    """
    if not API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key auth no está configurada en el servidor. "
                   "Define SHOPIFY_API_KEYS en .env y reinicia el servicio.",
        )

    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Falta el header 'X-API-Key'.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Comparación constant-time: recorre TODAS las keys (no early-return).
    matched_label: Optional[str] = None
    for known_key, label in API_KEYS.items():
        if secrets.compare_digest(x_api_key, known_key):
            matched_label = label

    if matched_label is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key inválida.",
        )

    return matched_label
