"""API REST del módulo NoSQL de Dann-Alpes.

La aplicación APEX conserva en Oracle la información maestra de usuarios,
sedes, habitaciones y servicios. Esta API administra la colección MongoDB
``reportes`` y expone RF1-RF9 y RFC1-RFC3.

Variable de entorno obligatoria:
    MONGO_URI  Cadena de conexión de MongoDB Atlas.

Variable opcional:
    MONGO_DB   Nombre de la base. Por defecto ISIS2304A15202619.

La API no usa API key. Los endpoints quedan disponibles para ser consumidos
directamente desde Oracle APEX.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from os import getenv
from typing import Any, Literal

import certifi
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import (
    DuplicateKeyError,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WriteError,
)
from pymongo.server_api import ServerApi

load_dotenv()

MONGO_URI = (getenv("MONGO_URI") or "").strip()
MONGO_DB = (getenv("MONGO_DB") or "ISIS2304A15202619").strip()

if not MONGO_URI:
    raise RuntimeError("Falta la variable de entorno MONGO_URI")
if not MONGO_DB:
    raise RuntimeError("La variable MONGO_DB no puede estar vacía")

client = MongoClient(
    MONGO_URI,
    server_api=ServerApi("1"),
    serverSelectionTimeoutMS=8000,
    connectTimeoutMS=8000,
    socketTimeoutMS=15000,
    tz_aware=True,
    tlsCAFile=certifi.where(),
)
db = client[MONGO_DB]
reportes: Collection = db["reportes"]


Estado = Literal[
    "limpia",
    "sucia",
    "con danos",
    "desordenada",
    "requiere mantenimiento urgente",
]
ResultadoRevision = Literal["conforme", "inconforme"]
OrdenReportes = Literal["fecha", "habitacion"]
MAX_INT64 = 9_223_372_036_854_775_807

ESTADOS = [
    "limpia",
    "sucia",
    "con danos",
    "desordenada",
    "requiere mantenimiento urgente",
]
ESTADOS_NEGATIVOS = [
    "con danos",
    "con daños",  # Compatibilidad con documentos escritos con ñ.
    "desordenada",
    "requiere mantenimiento urgente",
]
NOMBRES_MESES = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


def inicio_utc(valor: date) -> datetime:
    return datetime.combine(valor, time.min, tzinfo=timezone.utc)


def serializar(valor: Any) -> Any:
    """Convierte tipos BSON a valores JSON que APEX puede consumir."""
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, datetime):
        if valor.tzinfo is None:
            valor = valor.replace(tzinfo=timezone.utc)
        return valor.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(valor, list):
        return [serializar(item) for item in valor]
    if isinstance(valor, dict):
        return {clave: serializar(item) for clave, item in valor.items()}
    return valor


def formatear_reporte(documento: dict[str, Any]) -> dict[str, Any]:
    """Añade columnas planas útiles para informes y formularios de APEX."""
    salida = serializar(documento)
    observacion = salida.get("observacionAdministrador") or {}
    revision = salida.get("revisionAdministrativa") or {}
    marca = salida.get("marcaCritica") or {}

    salida["tieneObservacionAdministrador"] = bool(observacion)
    salida["comentarioAdministrador"] = observacion.get("comentario")
    salida["administradorObservacion"] = observacion.get(
        "identificacionAdministrador"
    )
    salida["resultadoRevision"] = revision.get("resultado")
    salida["observacionRevision"] = revision.get("observacion")
    salida["administradorRevision"] = revision.get("identificacionAdministrador")
    salida["administradorMarcaCritica"] = marca.get(
        "identificacionAdministrador"
    )
    return salida


def pagina(
    items: list[dict[str, Any]], page: int, size: int, total: int
) -> dict[str, Any]:
    return {
        "items": [formatear_reporte(item) for item in items],
        "page": page,
        "size": size,
        "total": total,
        "pages": (total + size - 1) // size if total else 0,
    }


def obtener_reporte_o_404(
    id_servicio: int, proyeccion: dict[str, int] | None = None
) -> dict[str, Any]:
    documento = reportes.find_one({"idServicio": id_servicio}, proyeccion)
    if not documento:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    return documento


def asegurar_indices() -> None:
    """Índices derivados de RF4, RF6 y las tres consultas analíticas."""
    reportes.create_index(
        [("idServicio", ASCENDING)],
        unique=True,
        name="uq_reporte_por_servicio",
    )
    reportes.create_index(
        [
            ("idSede", ASCENDING),
            ("numeroHabitacion", ASCENDING),
            ("fechaCreacion", DESCENDING),
        ],
        name="idx_historial_habitacion",
    )
    reportes.create_index(
        [
            ("identificacionPersonal", ASCENDING),
            ("fechaCreacion", DESCENDING),
        ],
        name="idx_reportes_personal",
    )
    reportes.create_index(
        [
            ("idSede", ASCENDING),
            ("fechaCreacion", ASCENDING),
            ("estadoEncontrado", ASCENDING),
        ],
        name="idx_rfc1",
    )
    reportes.create_index(
        [
            ("idSede", ASCENDING),
            ("numeroHabitacion", ASCENDING),
            ("anio", ASCENDING),
            ("mes", ASCENDING),
        ],
        name="idx_rfc2",
    )
    reportes.create_index(
        [
            ("idSede", ASCENDING),
            ("identificacionPersonal", ASCENDING),
        ],
        name="idx_rfc3",
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    client.admin.command("ping")
    asegurar_indices()
    yield
    client.close()


app = FastAPI(
    title="Dann-Alpes - API de reportes",
    version="2.1.0",
    description=(
        "API para conectar Oracle APEX con la colección reportes de MongoDB. "
        "Implementa RF1-RF9 y RFC1-RFC3 sin autenticación por API key."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ServerSelectionTimeoutError)
async def manejar_timeout_mongo(
    _: Request, __: ServerSelectionTimeoutError
) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": "MongoDB no está disponible o Atlas bloqueó la conexión"},
    )


@app.exception_handler(WriteError)
async def manejar_error_validacion_mongo(_: Request, error: WriteError) -> JSONResponse:
    codigo = (error.details or {}).get("code")
    mensaje = (
        "El documento no cumple el esquema de validación de MongoDB"
        if codigo == 121
        else "MongoDB rechazó la escritura"
    )
    return JSONResponse(status_code=422, content={"detail": mensaje})


@app.exception_handler(OperationFailure)
async def manejar_operacion_mongo(_: Request, error: OperationFailure) -> JSONResponse:
    codigo = getattr(error, "code", None)
    estado_http = 422 if codigo == 121 else 503
    mensaje = (
        "El documento no cumple el esquema de validación de MongoDB"
        if codigo == 121
        else "MongoDB no pudo completar la operación"
    )
    return JSONResponse(status_code=estado_http, content={"detail": mensaje})


@app.exception_handler(PyMongoError)
async def manejar_error_mongo(_: Request, __: PyMongoError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": "No fue posible completar la operación en MongoDB"},
    )


class ModeloAPI(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CrearReporte(ModeloAPI):
    # Datos obtenidos por APEX desde Oracle.
    idServicio: int = Field(gt=0)
    idSede: int = Field(gt=0)
    nombreSede: str = Field(min_length=1, max_length=150)
    ciudad: str = Field(min_length=1, max_length=100)
    numeroHabitacion: int = Field(gt=0)
    identificacionPersonal: int = Field(gt=0, le=MAX_INT64)
    nombrePersonal: str = Field(min_length=1, max_length=150)

    # Datos digitados por el técnico.
    estadoEncontrado: Estado
    prioridad: int = Field(ge=1, le=5)
    descripcion: str = Field(min_length=1, max_length=2000)

    # Comprobaciones que APEX debe resolver contra Oracle antes de llamar la API.
    servicioActivo: bool
    servicioAsignadoAlPersonal: bool
    servicioCorrespondeHabitacion: bool

    @field_validator("estadoEncontrado", mode="before")
    @classmethod
    def normalizar_estado(cls, valor: Any) -> Any:
        if isinstance(valor, str):
            valor = " ".join(valor.strip().lower().split())
            if valor == "con daños":
                return "con danos"
        return valor


class EditarReporte(ModeloAPI):
    identificacionPersonal: int = Field(gt=0, le=MAX_INT64)
    servicioCompletado: bool
    estadoEncontrado: Estado
    prioridad: int = Field(ge=1, le=5)
    descripcion: str = Field(min_length=1, max_length=2000)

    @field_validator("estadoEncontrado", mode="before")
    @classmethod
    def normalizar_estado(cls, valor: Any) -> Any:
        if isinstance(valor, str):
            valor = " ".join(valor.strip().lower().split())
            if valor == "con daños":
                return "con danos"
        return valor


class MarcarCritico(ModeloAPI):
    identificacionAdministrador: int = Field(gt=0, le=MAX_INT64)
    critico: bool


class GuardarObservacion(ModeloAPI):
    identificacionAdministrador: int = Field(gt=0, le=MAX_INT64)
    comentario: str = Field(min_length=1, max_length=1000)


class GuardarRevision(ModeloAPI):
    identificacionAdministrador: int = Field(gt=0, le=MAX_INT64)
    servicioCompletado: bool
    resultado: ResultadoRevision
    observacion: str | None = Field(default=None, max_length=1000)

    @field_validator("observacion")
    @classmethod
    def limpiar_observacion(cls, valor: str | None) -> str | None:
        if valor is None:
            return None
        valor = valor.strip()
        return valor or None


@app.get("/", tags=["General"])
def inicio() -> dict[str, Any]:
    return {
        "estado": "API Dann-Alpes funcionando correctamente",
        "documentacion": "/docs",
        "salud": "/api/v1/salud",
    }


@app.get("/api/v1/salud", tags=["General"])
def salud() -> dict[str, Any]:
    client.admin.command("ping")
    return {
        "estado": "ok",
        "baseDatos": MONGO_DB,
        "coleccion": "reportes",
        "timestamp": serializar(ahora_utc()),
    }


@app.get(
    "/api/v1/catalogos/estados",
    tags=["APEX"],
)
def catalogo_estados() -> dict[str, Any]:
    return {
        "items": [
            {"valor": estado, "etiqueta": estado.capitalize()} for estado in ESTADOS
        ]
    }


@app.get(
    "/api/v1/reportes/existe/{id_servicio}",
    tags=["RF1"],
)
def existe_reporte(id_servicio: int) -> dict[str, Any]:
    existe = reportes.count_documents({"idServicio": id_servicio}, limit=1) > 0
    return {"idServicio": id_servicio, "existe": existe}


@app.post(
    "/api/v1/reportes",
    tags=["RF1"],
    status_code=status.HTTP_201_CREATED,
)
def crear_reporte(datos: CrearReporte) -> dict[str, Any]:
    if not datos.servicioActivo:
        raise HTTPException(status_code=409, detail="El servicio no está activo")
    if not datos.servicioAsignadoAlPersonal:
        raise HTTPException(
            status_code=403,
            detail="El servicio no está asignado al personal autenticado",
        )
    if not datos.servicioCorrespondeHabitacion:
        raise HTTPException(
            status_code=409,
            detail="El servicio no corresponde a la habitación indicada",
        )

    ahora = ahora_utc()
    documento = datos.model_dump(
        exclude={
            "servicioActivo",
            "servicioAsignadoAlPersonal",
            "servicioCorrespondeHabitacion",
        }
    )
    documento.update(
        {
            "fechaCreacion": ahora,
            "anio": ahora.year,
            "mes": ahora.month,
            "critico": False,
            "valorCritico": 0,
        }
    )

    try:
        reportes.insert_one(documento)
    except DuplicateKeyError as error:
        raise HTTPException(
            status_code=409,
            detail="Ya existe un reporte para este servicio",
        ) from error

    creado = obtener_reporte_o_404(datos.idServicio, {"_id": 0})
    return formatear_reporte(creado)


@app.get(
    "/api/v1/reportes/{id_servicio}",
    tags=["Reportes"],
)
def consultar_reporte(id_servicio: int) -> dict[str, Any]:
    return formatear_reporte(obtener_reporte_o_404(id_servicio, {"_id": 0}))


@app.put(
    "/api/v1/reportes/{id_servicio}",
    tags=["RF2"],
)
def editar_reporte(id_servicio: int, datos: EditarReporte) -> dict[str, Any]:
    if datos.servicioCompletado:
        raise HTTPException(
            status_code=409,
            detail="No se puede editar: el servicio ya fue completado",
        )

    actualizado = reportes.find_one_and_update(
        {
            "idServicio": id_servicio,
            "identificacionPersonal": datos.identificacionPersonal,
        },
        {
            "$set": {
                "estadoEncontrado": datos.estadoEncontrado,
                "prioridad": datos.prioridad,
                "descripcion": datos.descripcion,
                "fechaEdicion": ahora_utc(),
            }
        },
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )

    if actualizado:
        return formatear_reporte(actualizado)

    existente = reportes.find_one(
        {"idServicio": id_servicio}, {"_id": 0, "identificacionPersonal": 1}
    )
    if not existente:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    raise HTTPException(
        status_code=403,
        detail="El reporte no pertenece al personal autenticado",
    )


@app.delete(
    "/api/v1/reportes/{id_servicio}/personal",
    tags=["RF3"],
)
def eliminar_reporte_personal(
    id_servicio: int,
    identificacionPersonal: int = Query(..., gt=0, le=MAX_INT64),
) -> dict[str, Any]:
    resultado = reportes.delete_one(
        {
            "idServicio": id_servicio,
            "identificacionPersonal": identificacionPersonal,
        }
    )
    if resultado.deleted_count == 1:
        return {"mensaje": "Reporte eliminado por el personal"}

    existente = reportes.count_documents({"idServicio": id_servicio}, limit=1)
    if not existente:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    raise HTTPException(
        status_code=403,
        detail="El reporte no pertenece al personal autenticado",
    )


@app.get(
    "/api/v1/habitaciones/{id_sede}/{numero_habitacion}/reportes",
    tags=["RF4"],
)
def historial_habitacion(
    id_sede: int,
    numero_habitacion: int,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    filtro = {"idSede": id_sede, "numeroHabitacion": numero_habitacion}
    total = reportes.count_documents(filtro)
    cursor = (
        reportes.find(filtro, {"_id": 0})
        .sort([("fechaCreacion", DESCENDING), ("idServicio", DESCENDING)])
        .skip((page - 1) * size)
        .limit(size)
    )
    return pagina(list(cursor), page, size, total)


@app.patch(
    "/api/v1/reportes/{id_servicio}/critico",
    tags=["RF5"],
)
def marcar_reporte_critico(
    id_servicio: int, datos: MarcarCritico
) -> dict[str, Any]:
    ahora = ahora_utc()
    if datos.critico:
        operacion = {
            "$set": {
                "critico": True,
                "valorCritico": 100,
                "marcaCritica": {
                    "identificacionAdministrador": datos.identificacionAdministrador,
                    "fechaMarcacion": ahora,
                },
            }
        }
    else:
        operacion = {
            "$set": {"critico": False, "valorCritico": 0},
            "$unset": {"marcaCritica": ""},
        }

    actualizado = reportes.find_one_and_update(
        {"idServicio": id_servicio},
        operacion,
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    if not actualizado:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    return formatear_reporte(actualizado)


@app.get(
    "/api/v1/personal/{identificacion_personal}/reportes",
    tags=["RF6"],
)
def reportes_propios(
    identificacion_personal: int,
    orden: OrdenReportes = Query("fecha"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    filtro = {"identificacionPersonal": identificacion_personal}
    ordenamiento = (
        [("fechaCreacion", DESCENDING), ("idServicio", DESCENDING)]
        if orden == "fecha"
        else [
            ("idSede", ASCENDING),
            ("numeroHabitacion", ASCENDING),
            ("fechaCreacion", DESCENDING),
        ]
    )
    total = reportes.count_documents(filtro)
    cursor = (
        reportes.find(filtro, {"_id": 0})
        .sort(ordenamiento)
        .skip((page - 1) * size)
        .limit(size)
    )
    return pagina(list(cursor), page, size, total)


@app.put(
    "/api/v1/reportes/{id_servicio}/observacion",
    tags=["RF7"],
)
def guardar_observacion(
    id_servicio: int, datos: GuardarObservacion
) -> dict[str, Any]:
    actual = obtener_reporte_o_404(
        id_servicio, {"_id": 0, "observacionAdministrador": 1}
    )
    anterior = actual.get("observacionAdministrador")
    ahora = ahora_utc()

    observacion: dict[str, Any] = {
        "identificacionAdministrador": datos.identificacionAdministrador,
        "comentario": datos.comentario,
        "fechaCreacion": anterior.get("fechaCreacion", ahora) if anterior else ahora,
    }
    if anterior:
        observacion["fechaEdicion"] = ahora

    actualizado = reportes.find_one_and_update(
        {"idServicio": id_servicio},
        {"$set": {"observacionAdministrador": observacion}},
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    return formatear_reporte(actualizado)


@app.delete(
    "/api/v1/reportes/{id_servicio}/administrador",
    tags=["RF8"],
)
def eliminar_reporte_administrador(
    id_servicio: int,
    identificacionAdministrador: int = Query(..., gt=0, le=MAX_INT64),
) -> dict[str, Any]:
    # La autorización del rol se valida en APEX/Oracle. El identificador queda
    # exigido en la llamada para que APEX registre quién ejecutó la acción.
    _ = identificacionAdministrador
    resultado = reportes.delete_one({"idServicio": id_servicio})
    if resultado.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    return {"mensaje": "Reporte eliminado por el administrador"}


@app.put(
    "/api/v1/reportes/{id_servicio}/revision",
    tags=["RF9"],
)
def guardar_revision(
    id_servicio: int, datos: GuardarRevision
) -> dict[str, Any]:
    if not datos.servicioCompletado:
        raise HTTPException(
            status_code=409,
            detail="El reporte solo puede revisarse cuando el servicio esté completado",
        )
    if datos.resultado == "inconforme" and not datos.observacion:
        raise HTTPException(
            status_code=422,
            detail="Una revisión inconforme requiere una observación",
        )

    revision: dict[str, Any] = {
        "resultado": datos.resultado,
        "identificacionAdministrador": datos.identificacionAdministrador,
        "fechaRevision": ahora_utc(),
    }
    if datos.observacion:
        revision["observacion"] = datos.observacion

    actualizado = reportes.find_one_and_update(
        {"idServicio": id_servicio},
        {"$set": {"revisionAdministrativa": revision}},
        projection={"_id": 0},
        return_document=ReturnDocument.AFTER,
    )
    if not actualizado:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    return formatear_reporte(actualizado)


@app.get(
    "/api/v1/reportes",
    tags=["APEX"],
)
def listar_reportes_administracion(
    idSede: int | None = Query(default=None, gt=0),
    numeroHabitacion: int | None = Query(default=None, gt=0),
    critico: bool | None = None,
    resultadoRevision: Literal["conforme", "inconforme", "sin_revision"] | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    filtro: dict[str, Any] = {}
    if idSede is not None:
        filtro["idSede"] = idSede
    if numeroHabitacion is not None:
        filtro["numeroHabitacion"] = numeroHabitacion
    if critico is not None:
        filtro["critico"] = critico
    if resultadoRevision == "sin_revision":
        filtro["revisionAdministrativa"] = {"$exists": False}
    elif resultadoRevision:
        filtro["revisionAdministrativa.resultado"] = resultadoRevision

    total = reportes.count_documents(filtro)
    cursor = (
        reportes.find(filtro, {"_id": 0})
        .sort([("fechaCreacion", DESCENDING), ("idServicio", DESCENDING)])
        .skip((page - 1) * size)
        .limit(size)
    )
    return pagina(list(cursor), page, size, total)


@app.get(
    "/api/v1/analitica/rfc1",
    tags=["RFC1"],
)
def rfc1_habitaciones_estados_negativos(
    idSede: int = Query(..., gt=0),
    desde: date = Query(...),
    hasta: date = Query(...),
) -> dict[str, Any]:
    if desde > hasta:
        raise HTTPException(
            status_code=422,
            detail="La fecha desde debe ser anterior o igual a la fecha hasta",
        )

    inicio = inicio_utc(desde)
    fin_exclusivo = inicio_utc(hasta + timedelta(days=1))
    pipeline = [
        {
            "$match": {
                "idSede": idSede,
                "fechaCreacion": {"$gte": inicio, "$lt": fin_exclusivo},
                "estadoEncontrado": {"$in": ESTADOS_NEGATIVOS},
            }
        },
        {
            "$group": {
                "_id": {
                    "idSede": "$idSede",
                    "numeroHabitacion": "$numeroHabitacion",
                },
                "nombreSede": {"$first": "$nombreSede"},
                "ciudad": {"$first": "$ciudad"},
                "frecuenciaEstadosNegativos": {"$sum": 1},
                "conDanos": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$estadoEncontrado", ["con danos", "con daños"]]},
                            1,
                            0,
                        ]
                    }
                },
                "desordenada": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$estadoEncontrado", "desordenada"]},
                            1,
                            0,
                        ]
                    }
                },
                "mantenimientoUrgente": {
                    "$sum": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$estadoEncontrado",
                                    "requiere mantenimiento urgente",
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "ultimaOcurrencia": {"$max": "$fechaCreacion"},
            }
        },
        {
            "$sort": {
                "frecuenciaEstadosNegativos": -1,
                "_id.numeroHabitacion": 1,
            }
        },
        {
            "$project": {
                "_id": 0,
                "idSede": "$_id.idSede",
                "numeroHabitacion": "$_id.numeroHabitacion",
                "nombreSede": 1,
                "ciudad": 1,
                "frecuenciaEstadosNegativos": 1,
                "conDanos": 1,
                "desordenada": 1,
                "mantenimientoUrgente": 1,
                "ultimaOcurrencia": 1,
            }
        },
    ]
    items = serializar(list(reportes.aggregate(pipeline)))
    return {"items": items, "total": len(items)}


@app.get(
    "/api/v1/analitica/rfc2",
    tags=["RFC2"],
)
def rfc2_distribucion_mensual(
    idSede: int = Query(..., gt=0),
    numeroHabitacion: int = Query(..., gt=0),
    anio: int = Query(..., ge=2000, le=2100),
) -> dict[str, Any]:
    pipeline = [
        {
            "$match": {
                "idSede": idSede,
                "numeroHabitacion": numeroHabitacion,
                "anio": anio,
            }
        },
        {
            "$group": {
                "_id": {"mes": "$mes", "estado": "$estadoEncontrado"},
                "cantidad": {"$sum": 1},
            }
        },
        {"$sort": {"_id.mes": 1}},
    ]

    acumulado: dict[int, dict[str, int]] = {
        mes: {estado: 0 for estado in ESTADOS} for mes in range(1, 13)
    }
    for fila in reportes.aggregate(pipeline):
        mes = fila["_id"]["mes"]
        estado = fila["_id"]["estado"]
        if estado == "con daños":
            estado = "con danos"
        if mes in acumulado and estado in acumulado[mes]:
            acumulado[mes][estado] += fila["cantidad"]

    items = []
    for mes in range(1, 13):
        valores = acumulado[mes]
        items.append(
            {
                "mes": mes,
                "nombreMes": NOMBRES_MESES[mes - 1],
                "limpia": valores["limpia"],
                "sucia": valores["sucia"],
                "conDanos": valores["con danos"],
                "desordenada": valores["desordenada"],
                "mantenimientoUrgente": valores[
                    "requiere mantenimiento urgente"
                ],
                "total": sum(valores.values()),
            }
        )
    return {"items": items, "total": 12}


@app.get(
    "/api/v1/analitica/rfc3",
    tags=["RFC3"],
)
def rfc3_perfil_tecnicos(
    idSede: int = Query(..., gt=0),
) -> dict[str, Any]:
    pipeline = [
        {"$match": {"idSede": idSede}},
        {
            "$group": {
                "_id": {
                    "identificacionPersonal": "$identificacionPersonal",
                    "nombrePersonal": "$nombrePersonal",
                },
                "nombreSede": {"$first": "$nombreSede"},
                "ciudad": {"$first": "$ciudad"},
                "totalReportes": {"$sum": 1},
                "reportesCriticos": {
                    "$sum": {"$cond": [{"$eq": ["$critico", True]}, 1, 0]}
                },
                "limpia": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$estadoEncontrado", "limpia"]},
                            1,
                            0,
                        ]
                    }
                },
                "sucia": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$estadoEncontrado", "sucia"]},
                            1,
                            0,
                        ]
                    }
                },
                "conDanos": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$estadoEncontrado", ["con danos", "con daños"]]},
                            1,
                            0,
                        ]
                    }
                },
                "desordenada": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$estadoEncontrado", "desordenada"]},
                            1,
                            0,
                        ]
                    }
                },
                "mantenimientoUrgente": {
                    "$sum": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$estadoEncontrado",
                                    "requiere mantenimiento urgente",
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "identificacionPersonal": "$_id.identificacionPersonal",
                "nombrePersonal": "$_id.nombrePersonal",
                "nombreSede": 1,
                "ciudad": 1,
                "totalReportes": 1,
                "reportesCriticos": 1,
                "limpia": 1,
                "sucia": 1,
                "conDanos": 1,
                "desordenada": 1,
                "mantenimientoUrgente": 1,
                "porcentajeCriticos": {
                    "$round": [
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$reportesCriticos",
                                        "$totalReportes",
                                    ]
                                },
                                100,
                            ]
                        },
                        2,
                    ]
                },
            }
        },
        {
            "$sort": {
                "porcentajeCriticos": -1,
                "totalReportes": -1,
                "nombrePersonal": 1,
            }
        },
    ]
    items = serializar(list(reportes.aggregate(pipeline)))
    return {"items": items, "total": len(items)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(getenv("PORT") or "8000"),
        reload=False,
    )
