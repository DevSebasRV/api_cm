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
SHOPIFY_COMPARE_AT_PRICE_LIST = int(os.getenv("SHOPIFY_COMPARE_AT_PRICE_LIST", "1"))  # "LISTA DE PRECIOS 01"

# Lista de precios (por NOMBRE) de la que sale el "Variant Price" de Shopify.
# Se resuelve por nombre porque el número difiere entre bases y puede no existir
# en alguna: "Descuentos Boutique" solo está en Promo/cp (= lista 3); en
# Ferbel/fn no existe → el Variant Price sale en 0.
SHOPIFY_VARIANT_PRICE_LIST_NAME = os.getenv("SHOPIFY_VARIANT_PRICE_LIST_NAME", "Descuentos Boutique")

# ─────────────────────────────────────────────────────────────────────────────
# ClearMechanic integration
# ─────────────────────────────────────────────────────────────────────────────
# Credenciales del servicio openapi.somosclear.com (las mismas del script del
# jefe). El usuario/password se ponen en el .env del servidor — NO hardcodear.
CM_LOGIN_URL  = os.getenv("CM_LOGIN_URL",  "https://openapi.somosclear.com/api/users/login")
CM_ORDERS_URL = os.getenv("CM_ORDERS_URL", "https://openapi.somosclear.com/api/cm/orders")
CM_USER       = os.getenv("CM_USER")
CM_PASSWORD   = os.getenv("CM_PASSWORD")

# Cuenta de CM POR SUCURSAL (CM_USER_<repairShopId> / CM_PASSWORD_<repairShopId>
# en el .env). Si una sucursal no tiene la suya, se usa la cuenta global de
# arriba — así el sistema funciona igual aunque falte alguna. El objetivo es
# repartir el límite de tasa de CM entre cuentas (pendiente confirmar con su
# soporte si el límite es por cuenta; si fuera por IP esto no estorba).
# 2948=Satélite/FERBEL, 2947=Patriotismo/PROSHOP, 4104=Coapa/SUR, 4105=Tonalá/ROMA.
CM_ACCOUNTS = {}
for _shop in (2948, 2947, 4104, 4105):
    _u = os.getenv(f"CM_USER_{_shop}")
    _p = os.getenv(f"CM_PASSWORD_{_shop}")
    if _u and _p:
        CM_ACCOUNTS[_shop] = (_u, _p)

# Base SQLite con la bitácora de peticiones salientes a CM (observabilidad).
CM_METRICS_DB = os.getenv(
    "CM_METRICS_DB",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cm_metrics.db"),
)
