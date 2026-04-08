import os
from dotenv import load_dotenv

load_dotenv()

# Conexión a SQL Server
SAP_SERVER   = os.getenv("SAP_SERVER")
SAP_USER     = os.getenv("SAP_USER")
SAP_PASSWORD = os.getenv("SAP_PASSWORD")

EMPRESAS = {
    "cp": os.getenv("SAP_DATABASE_CP"),
    "fn": os.getenv("SAP_DATABASE_FN"),
}

# Código de lista de precios en SAP B1
# La lista "01" se almacena como entero 1 en el campo PriceList de ITM1
PRICE_LIST_CODE = 1
