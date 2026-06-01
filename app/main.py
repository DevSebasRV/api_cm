from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import EMPRESAS
from app.database import get_connection
from app.routers import inventory, business_partners, shopify, service_calls

app = FastAPI(
    title="SAP B1 - ClearMechanic Middleware",
    description="Expone artículos de SAP B1 en el formato /inventoryItems de ClearMechanic",
    version="1.2.0",
    root_path="/cm"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(inventory.router)
app.include_router(business_partners.router)
app.include_router(shopify.router)
app.include_router(service_calls.router)


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Sistema"])
def health_check():
    estado = {}
    for emp, database in EMPRESAS.items():
        try:
            conn = get_connection(database)
            conn.close()
            estado[emp.upper()] = "conectado"
        except Exception as e:
            estado[emp.upper()] = f"error: {str(e)}"

    return {
        "status":    "ok",
        "service":   "SAP B1 → ClearMechanic Middleware",
        "databases": estado,
    }
