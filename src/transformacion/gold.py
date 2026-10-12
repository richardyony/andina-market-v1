"""
===============================================================================
 Archivo      : gold.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Gold (nivel 2) · Lakeflow Declarative Pipelines
 Propósito    : Modelo dimensional (estrella) listo para analítica y ML.

 Modelo
   dim_fecha ─┐
   dim_cliente (SCD2) ── fact_ventas (grano: línea de pedido) ── dim_producto
   dim_canal ─┘
   dim_cliente ── fact_pagos (grano: pago) ── dim_fecha

 Decisiones
   - El cliente se une a la versión SCD2 vigente en la fecha del pedido: una venta
     cuenta para el segmento que el cliente tenía al comprar.
   - Ventas = suma de líneas (cantidad × precio − descuento), no el total de cabecera,
     que en algunos pedidos legacy no cuadra (silver.control_totales_pedido).
   - Claves sustitutas deterministas (hash de la clave natural y la vigencia):
     recalcular gold produce las mismas claves → idempotente.
   - Vistas materializadas: el pipeline las recalcula desde silver en cada corrida.
 Parámetros   : catalogo (configuración del pipeline)
 Lee          : silver.* (del mismo pipeline)
 Escribe      : <catalogo>.gold.*
 Detalle      : docs/modelo_datos.md
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-12
===============================================================================
"""

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOGO = spark.conf.get("catalogo")  # noqa: F821  (spark lo provee el pipeline)
GOLD = f"{CATALOGO}.gold"

INICIO_DE_LOS_TIEMPOS = F.lit("1900-01-01 00:00:00").cast("timestamp")
FIN_DE_LOS_TIEMPOS = F.lit("9999-12-31 00:00:00").cast("timestamp")
ESTADOS_VENTA_EFECTIVA = ["pagado", "enviado", "entregado"]


def sk(*cols):
    """Clave sustituta determinista: el mismo dato siempre produce la misma clave."""
    return F.xxhash64(*cols)


def sk_fecha(col):
    """Clave de fecha legible: AAAAMMDD."""
    return F.date_format(col, "yyyyMMdd").cast("int")


# =============================================================================
# 1. Dimensiones
# =============================================================================
@dlt.table(name=f"{GOLD}.dim_fecha", comment="Calendario diario (2024-2027) para analizar por año, mes y día.")
def dim_fecha():
    dias = spark.sql("SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS fecha")  # noqa: F821
    return dias.select(
        sk_fecha("fecha").alias("sk_fecha"), "fecha",
        F.year("fecha").alias("anio"), F.quarter("fecha").alias("trimestre"),
        F.month("fecha").alias("mes"), F.date_format("fecha", "MMMM").alias("nombre_mes"),
        F.dayofmonth("fecha").alias("dia"), F.dayofweek("fecha").alias("dia_semana"),
        F.dayofweek("fecha").isin(1, 7).alias("es_fin_de_semana"))


@dlt.table(name=f"{GOLD}.dim_canal", comment="Canales de venta de Andina Market.")
def dim_canal():
    canales = spark.createDataFrame(  # noqa: F821
        [("web", "Sitio web", "digital"), ("app", "App móvil", "digital"), ("tienda", "Tienda física", "presencial")],
        "canal STRING, nombre_canal STRING, tipo_canal STRING")
    return canales.select(sk("canal").alias("sk_canal"), "*")


@dlt.table(name=f"{GOLD}.dim_producto", comment="Catálogo de productos (estado actual, SCD1).")
def dim_producto():
    return dlt.read("productos").select(
        sk("id_producto").alias("sk_producto"), "id_producto", "sku", "nombre_producto", "categoria",
        F.col("precio").alias("precio_actual"), "estado", "tiene_descripcion")


@dlt.table(name=f"{GOLD}.dim_cliente",
           comment="Clientes con historial de segmento (SCD2): una fila por versión, con su vigencia.")
def dim_cliente():
    historial = dlt.read("clientes_historial")
    actual = dlt.read("clientes").select(
        "id_cliente", "nombre", "apellido", "email_normalizado", "email_canonico", "telefono", "ciudad",
        "codigo_pais", "fecha_alta", "es_activo", "es_email_valido")

    # La primera versión de cada cliente vale desde el inicio de los tiempos: así una venta
    # anterior a la fecha del primer cambio registrado también encuentra su versión.
    primera = Window.partitionBy("id_cliente").orderBy(F.col("__START_AT._fecha_efectiva"))
    versiones = (historial
                 .withColumn("_orden", F.row_number().over(primera))
                 .select("id_cliente", "segmento",
                         F.when(F.col("_orden") == 1, INICIO_DE_LOS_TIEMPOS)
                          .otherwise(F.col("__START_AT._fecha_efectiva")).alias("vigente_desde"),
                         F.coalesce(F.col("__END_AT._fecha_efectiva"), FIN_DE_LOS_TIEMPOS).alias("vigente_hasta"),
                         F.col("__END_AT").isNull().alias("es_vigente")))

    # Posible duplicado de negocio: misma persona con otra cuenta (mismo email canónico,
    # o mismo nombre + apellido + teléfono). Se marca; fusionar exige una regla de identidad.
    por_email = Window.partitionBy("email_canonico")
    por_persona = Window.partitionBy("nombre", "apellido", "telefono")
    actual = actual.withColumn(
        "es_posible_duplicado",
        (F.size(F.collect_set("id_cliente").over(por_email)) > 1)
        | (F.col("telefono").isNotNull() & (F.size(F.collect_set("id_cliente").over(por_persona)) > 1)))

    return (versiones.join(actual, "id_cliente", "inner")
            .select(sk("id_cliente", "vigente_desde").alias("sk_cliente"), "id_cliente", "nombre", "apellido",
                    "email_normalizado", "ciudad", "codigo_pais", "segmento", "fecha_alta", "es_activo",
                    "es_email_valido", "es_posible_duplicado", "vigente_desde", "vigente_hasta", "es_vigente"))


# =============================================================================
# 2. Hechos
# =============================================================================
def cliente_vigente_en(df, col_id, col_fecha):
    """Une cada fila con la versión del cliente vigente en la fecha indicada (SCD2)."""
    dim = dlt.read(f"{GOLD}.dim_cliente").select("sk_cliente", F.col("id_cliente").alias("_id"),
                                                 "vigente_desde", "vigente_hasta")
    cond = ((F.col(col_id) == F.col("_id")) & (F.col(col_fecha) >= F.col("vigente_desde"))
            & (F.col(col_fecha) < F.col("vigente_hasta")))
    return df.join(dim, cond, "left").drop("_id", "vigente_desde", "vigente_hasta")


@dlt.table(name=f"{GOLD}.fact_ventas", cluster_by=["sk_fecha", "sk_cliente"],
           comment="Ventas al grano de línea de pedido, con el segmento del cliente vigente al comprar.")
def fact_ventas():
    lineas = dlt.read("lineas_pedido")
    pedidos = dlt.read("pedidos").select("id_pedido", "id_cliente", "fecha_pedido", "canal",
                                         F.col("estado").alias("estado_pedido"))
    productos = dlt.read(f"{GOLD}.dim_producto").select("sk_producto", "id_producto")
    ventas = lineas.join(pedidos, "id_pedido", "inner").join(productos, "id_producto", "left")
    ventas = cliente_vigente_en(ventas, "id_cliente", "fecha_pedido")
    return ventas.select(
        sk_fecha("fecha_pedido").alias("sk_fecha"), "sk_cliente", "sk_producto", sk("canal").alias("sk_canal"),
        "id_pedido", "id_linea", "id_cliente", "id_producto", "fecha_pedido", "estado_pedido",
        "cantidad", "precio_unitario", "descuento", "monto_linea",
        F.col("estado_pedido").isin(ESTADOS_VENTA_EFECTIVA).alias("es_venta_efectiva"))


@dlt.table(name=f"{GOLD}.fact_pagos", cluster_by=["sk_fecha"],
           comment="Pagos (estado actual) con su recorrido de estados: cambios, tiempo hasta aprobarse e intentos del pedido.")
def fact_pagos():
    pagos = dlt.read("pagos")
    historial = (dlt.read("pagos_historial").groupBy("id_pago")
                 .agg((F.count("*") - 1).alias("n_cambios_estado"),
                      F.min(F.when(F.col("estado") == "aprobado", F.col("__START_AT._fecha_efectiva")))
                       .alias("fecha_aprobacion")))
    pedidos = dlt.read("pedidos").select("id_pedido", "id_cliente", "fecha_pedido")
    intentos = Window.partitionBy("id_pedido")
    df = (pagos.join(historial, "id_pago", "left").join(pedidos, "id_pedido", "left")
          .withColumn("n_intentos_pago", F.count("*").over(intentos)))
    df = cliente_vigente_en(df, "id_cliente", "fecha_pedido")
    return df.select(
        sk_fecha("fecha_pago").alias("sk_fecha"), "sk_cliente", "id_pago", "id_pedido", "id_cliente",
        "metodo_pago", "monto", "estado", "fecha_pago", "fecha_aprobacion",
        F.round((F.unix_timestamp("fecha_aprobacion") - F.unix_timestamp("fecha_pago")) / 60, 1)
         .alias("minutos_hasta_aprobacion"),
        F.coalesce("n_cambios_estado", F.lit(0)).alias("n_cambios_estado"), "n_intentos_pago")
