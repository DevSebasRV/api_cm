# API cm — Middleware SAP B1

API interna (FastAPI + pyodbc) que expone datos y operaciones de **SAP
Business One** para el portal interno y las integraciones del grupo:
inventario, socios de negocio, órdenes de servicio, citas e inspecciones de
**ClearMechanic**, conciliación de CFDIs (**Facturapi** vs SAP) y destajo de
técnicos. Multi-empresa: cada request elige la base SAP con el header
`X-SAP-DB` (`fn` | `cp` | `test`; sin header cae a `test` por seguridad).

## Estructura

```
app/
├── main.py              # FastAPI, CORS, monta routers, GET /health
├── config.py            # .env → EMPRESAS (bases SAP), credenciales, constantes
├── database.py          # get_connection(pyodbc) + helper de stock
├── security.py          # require_api_key (X-API-Key) — solo /shopify/*
└── routers/
    ├── common.py            # helpers compartidos: resolve_db, err, _pagination
    ├── inventory.py         # /inventoryItems (formato ClearMechanic)
    ├── business_partners.py # /businessPartners* (búsqueda, RFC, nextCode, detalle)
    ├── service_calls.py     # /serviceCalls* (listado/detalle/filtro por sucursal),
    │                        #   catálogos de alta (asesores/mecánicos por Posición),
    │                        #   documentos ligados, prefactura, stock, kits, seriales
    ├── clearmechanic.py     # /clearmechanic/* (órdenes con campos del formulario
    │                        #   de CM, citas, inspecciones/estimates, alertas de
    │                        #   inspección para el Inicio del portal)
    ├── transfers.py         # /transferRequests* (solicitudes de traslado por ODS
    │                        #   vía UDF U_ODS + ticket térmico), /transferRequestsOpen
    │                        #   (abiertas agrupadas por ODS) y /salespersonWarehouses
    │                        #   (almacenes destino permitidos)
    ├── cfdi_reconcile.py    # /cfdiReconcile (CFDIs recibidos vs OPCH),
    │                        #   /cfdiMatchCandidates, /cfdiUncapturedInvoices
    ├── destajo.py           # /destajo, /destajoTecnicos (SP de nómina)
    └── shopify.py           # /shopify/* (artículos con título/HTML/imágenes,
                             #   stock, precios) — con API key
run.py                   # dev: uvicorn con reload (puerto 8000)
```

## Convenciones

- **Multi-empresa**: todo endpoint acepta `X-SAP-DB` y resuelve la base con
  `common.resolve_db`. Nunca hardcodear un nombre de base en queries.
- **Errores**: respuesta estándar `{success: false, message, data: null}` vía
  `common.err()`. Los mensajes hacia el portal van en lenguaje claro, sin
  tecnicismos.
- **SQL siempre parametrizado** (`?`); nunca interpolar input del cliente en
  el texto de la consulta.
- **Conexiones**: cerrar cursor/conexión en `try/finally`.
- **Auth**: `/shopify/*` exige `X-API-Key` (llaves en `.env`). El resto de la
  API es de red interna, detrás del reverse proxy.
- **Asignaciones de órdenes de servicio (no intuitivo)**: en este SAP,
  `OSCL.assignee` guarda un **usuario** (el Mecánico del formulario) y
  `OSCL.technician` un **empleado** (el Asesor de Servicio). El código ya
  respeta esa semántica — no "corregirla" por el nombre de la columna.
- **Citas de ClearMechanic**: `startDate` se manda como **hora local del
  taller sin zona horaria** — su API ignora el sufijo `Z` y lee la hora tal
  cual (el GET de citas, en cambio, sí regresa UTC real). Nunca convertir a
  UTC al crear.
- **Documentos ligados a una ODS**: solo por ligas verificables (grilla SCL4
  de "Registr y Refacciones", líneas con BaseType=191, o marcador "ODS #n" en
  comentarios). Nunca inferir por cliente+fechas.
- **Traslados**: la liga con la ODS vive en el UDF `OWTQ.U_ODS` (solo la ponen
  los creados desde el portal). `OWTQ.Filler` es el almacén **origen** (nombre
  heredado de SAP) y `ToWhsCode` el destino. El "Empleado del departamento" es
  `OWTQ.SlpCode`. El almacén de un vendedor es `OSLP.Telephone` (regla
  CVMSales); los vendedores exentos traen `.`, que no es un almacén real —
  tratarlo como "sin filtro" en vez de devolver una lista vacía.
- **Punto de inspección cotizado**: el que ya tiene estimates (`parts` o
  `labors`) en ClearMechanic, que es lo que escribe el portal al cotizarlo. Las
  alertas del Inicio solo cuentan los **pendientes**, y recalculan los
  contadores (los `yellowItemsCount`/`redItemsCount` de CM incluyen los ya
  cotizados). Hay caché de 90 s por orden: CM limita la tasa de peticiones.
- **Listas de precios por nombre, no por número**: el mismo catálogo tiene
  distinto `ListNum` en cada empresa y puede no existir (ej. "Descuentos
  Boutique" existe en `cp` y no en `fn`). Resolver por `OPLN.ListName`.
- Español en comentarios, mensajes y commits.

## Desarrollo

```bash
python -m venv venv
venv/Scripts/activate         # Windows (Linux: source venv/bin/activate)
pip install -r requirements.txt
# copia de .env con las variables (ver abajo) y credenciales reales
python run.py                 # http://localhost:8000/docs (Swagger)
```

Variables de `.env` (sin valores aquí): `SAP_SERVER`, `SAP_USER`,
`SAP_PASSWORD`, `SAP_DATABASE_FN`, `SAP_DATABASE_CP`, `SAP_DATABASE_TEST`,
`SHOPIFY_API_KEYS`, `SHOPIFY_COMPARE_AT_PRICE_LIST`, `CM_LOGIN_URL`,
`CM_ORDERS_URL`, `CM_USER`, `CM_PASSWORD`.

Requiere el **ODBC Driver 18 for SQL Server** instalado y acceso de red al
SQL Server de SAP.

## Despliegue

Corre como proceso administrado (pm2) detrás de un reverse proxy con
`root_path=/cm`. El flujo es por git: commit → push → `git pull` en el
servidor → restart del proceso. El `.env` del servidor no se versiona ni se
toca en despliegues.
