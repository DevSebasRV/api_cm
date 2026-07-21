import os
from dotenv import load_dotenv

load_dotenv()

# Conexión a SQL Server
SAP_SERVER   = os.getenv("SAP_SERVER")
SAP_USER     = os.getenv("SAP_USER")
SAP_PASSWORD = os.getenv("SAP_PASSWORD")

EMPRESAS = {
    "cp":   os.getenv("SAP_DATABASE_CP"),
    "fn":   os.getenv("SAP_DATABASE_FN"),
    "test": os.getenv("SAP_DATABASE_TEST"),
}

# Código de lista de precios en SAP B1
# La lista "01" se almacena como entero 1 en el campo PriceList de ITM1
PRICE_LIST_CODE = 1

# ─────────────────────────────────────────────────────────────────────────────
# Shopify integration
# ─────────────────────────────────────────────────────────────────────────────
# Lista de precios que se mapea al campo Compare At Price de Shopify.
# Puede sobrescribirse vía .env si el código cambia por base.
# (El Variant Price se responde fijo en 0.0 — ver routers/shopify.py.)
SHOPIFY_COMPARE_AT_PRICE_LIST = int(os.getenv("SHOPIFY_COMPARE_AT_PRICE_LIST", "1"))  # "LISTA DE PRECIOS 01"

# ─────────────────────────────────────────────────────────────────────────────
# ClearMechanic integration
# ─────────────────────────────────────────────────────────────────────────────
# Credenciales del servicio openapi.somosclear.com (las mismas del script del
# jefe). El usuario/password se ponen en el .env del servidor — NO hardcodear.
CM_LOGIN_URL  = os.getenv("CM_LOGIN_URL",  "https://openapi.somosclear.com/api/users/login")
CM_ORDERS_URL = os.getenv("CM_ORDERS_URL", "https://openapi.somosclear.com/api/cm/orders")
CM_USER       = os.getenv("CM_USER")
CM_PASSWORD   = os.getenv("CM_PASSWORD")
