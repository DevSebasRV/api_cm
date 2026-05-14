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
# Listas de precios que se mapean a los campos de Shopify.
# Pueden sobrescribirse vía .env si los códigos cambian por base.
SHOPIFY_VARIANT_PRICE_LIST    = int(os.getenv("SHOPIFY_VARIANT_PRICE_LIST",    "2"))  # "Precio descuento"
SHOPIFY_COMPARE_AT_PRICE_LIST = int(os.getenv("SHOPIFY_COMPARE_AT_PRICE_LIST", "1"))  # "LISTA DE PRECIOS 01"
