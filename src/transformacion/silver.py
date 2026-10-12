"""
===============================================================================
 Archivo      : silver.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Silver (nivel 2) · Lakeflow Declarative Pipelines
 Propósito    : Pasar de los cambios CDC de bronze a una fila correcta por
                entidad: tipos y nombres estándar, limpieza, calidad, estado
                actual (SCD1) o historial (SCD2) y cuarentena de lo inválido.

 Flujo por tabla (el mismo para las 6)
   bronze.<tabla>_raw ─▶ vista limpio_<tabla> ─▶ AUTO CDC ─▶ silver.<tabla>
     (cambios R/I/U/D)    (renombra, normaliza,    (ordena por    (SCD1) y, en
                           aplica reglas)           _secuencia)   clientes y pagos,
                                │                                 <tabla>_historial (SCD2)
                                └─▶ silver.cuarentena (lo que no pasó una regla)

 Parámetros   : catalogo (configuración del pipeline, la pone el bundle)
 Lee          : <catalogo>.bronze.*_raw
 Escribe      : <catalogo>.silver.* (esquema por defecto del pipeline)
 Idempotencia : AUTO CDC aplica cada cambio una vez y en orden; re-ejecutar sin
                cambios nuevos no modifica las tablas.
 Detalle      : docs/modelo_datos.md · reglas en reglas_calidad.py
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-12
===============================================================================
"""

import dlt
from pyspark.sql import functions as F

from reglas_calidad import CIUDADES_CANONICAS, REGEX_EMAIL, reglas

CATALOGO = spark.conf.get("catalogo")  # noqa: F821  (spark lo provee el pipeline)

# AUTO CDC: nombre actual de la API; apply_changes es el nombre anterior.
aplicar_cdc = getattr(dlt, "create_auto_cdc_flow", None) or dlt.apply_changes

# Columnas técnicas de bronze que no pasan a silver (la trazabilidad queda en
# _lsn, _fecha_efectiva, _id_ejecucion e _ingestion_ts).
EXCLUIR_DE_SILVER = ["_op", "_seqval", "_commit_ts", "_secuencia", "_extract_ts", "_rescued_data", "_source_file"]


# =============================================================================
# 1. Utilidades comunes
# =============================================================================
def leer_bronze(tabla_raw: str):
    """Lee los cambios de bronze como stream (solo procesa lo nuevo en cada corrida)."""
    return spark.readStream.table(f"{CATALOGO}.bronze.{tabla_raw}")  # noqa: F821


def con_secuencia(df):
    """Agrega el orden de aplicación de los cambios.

    _fecha_efectiva: hora real del cambio. En los cambios incrementales es el commit
    en Azure SQL (_commit_ts); en la foto completa (_op = 'R') es nula, así que se
    usa UpdatedAt, que los triggers de la fuente mantienen en cada cambio.
    _lsn y _seqval desempatan cambios de una misma transacción.
    """
    return (df.withColumn("_fecha_efectiva", F.coalesce("_commit_ts", "UpdatedAt"))
              .withColumn("_secuencia", F.struct(F.col("_fecha_efectiva"), F.col("_lsn"),
                                                 F.coalesce("_seqval", F.lit("")).alias("_seqval"))))


def tecnicas():
    """Columnas técnicas que se arrastran hasta silver para trazabilidad y para AUTO CDC."""
    return ["_op", "_lsn", "_seqval", "_commit_ts", "_fecha_efectiva", "_secuencia", "_id_ejecucion",
            "_ingestion_ts", "_extract_ts", "_rescued_data", "_source_file"]


def registrar_cuarentena(tabla: str, vista: str, clave: str):
    """Envía a silver.cuarentena las filas de la vista que no cumplen una regla de DESCARTAR,
    con la lista de reglas incumplidas. Así nada se descarta en silencio."""
    reglas_descartar = reglas(tabla, "descartar")

    @dlt.append_flow(target="cuarentena", name=f"cuarentena_{tabla}")
    def _flujo():
        df = dlt.read_stream(vista)
        fallidas = F.array_compact(F.array(*[F.when(~F.expr(expr), F.lit(nombre))
                                             for nombre, expr in reglas_descartar.items()]))
        return (df.withColumn("reglas_fallidas", fallidas)
                  .filter(F.size("reglas_fallidas") > 0)
                  .select(F.lit(tabla).alias("tabla"),
                          F.col(clave).cast("string").alias("clave"),
                          "reglas_fallidas",
                          F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("fila"),
                          "_op", "_lsn", "_id_ejecucion", "_ingestion_ts",
                          F.current_timestamp().alias("_cuarentena_ts")))


def publicar_silver(tabla: str, vista: str, clave: str, historial_de: list | None = None):
    """Crea silver.<tabla> (SCD1) y, si se indica, silver.<tabla>_historial (SCD2)
    aplicando con AUTO CDC los cambios de la vista limpia."""
    dlt.create_streaming_table(name=tabla, comment=f"Estado actual de {tabla} (SCD1) desde CDC de Azure SQL.")
    aplicar_cdc(target=tabla, source=vista, keys=[clave], sequence_by=F.col("_secuencia"),
                apply_as_deletes=F.expr("_op = 'D'"), except_column_list=EXCLUIR_DE_SILVER,
                stored_as_scd_type=1)
    if historial_de:
        destino = f"{tabla}_historial"
        dlt.create_streaming_table(name=destino,
                                   comment=f"Historial SCD2 de {tabla}: abre versión solo cuando cambia "
                                           f"{', '.join(historial_de)}. Vigencia en __START_AT / __END_AT.")
        aplicar_cdc(target=destino, source=vista, keys=[clave], sequence_by=F.col("_secuencia"),
                    apply_as_deletes=F.expr("_op = 'D'"), except_column_list=EXCLUIR_DE_SILVER,
                    stored_as_scd_type=2, track_history_column_list=historial_de)
    registrar_cuarentena(tabla, vista, clave)


dlt.create_streaming_table(name="cuarentena",
                           comment="Filas que no cumplieron una regla de calidad, con la tabla, la clave y las reglas incumplidas.")


# =============================================================================
# 2. Clientes · SCD1 + historial SCD2 de segmento
# =============================================================================
mapa_ciudades = F.create_map(*[F.lit(x) for par in CIUDADES_CANONICAS.items() for x in par])


@dlt.view(name="limpio_clientes", comment="Clientes de bronze renombrados, normalizados y validados.")
@dlt.expect_all_or_fail(reglas("clientes", "fallar"))
@dlt.expect_all_or_drop(reglas("clientes", "descartar"))
@dlt.expect_all(reglas("clientes", "advertir"))
def limpio_clientes():
    df = con_secuencia(leer_bronze("customers_raw"))
    email_norm = F.lower(F.trim("Email"))
    local, dominio = F.split(email_norm, "@").getItem(0), F.split(email_norm, "@").getItem(1)
    return df.select(
        F.col("CustomerID").alias("id_cliente"),
        F.trim("FirstName").alias("nombre"),
        F.trim("LastName").alias("apellido"),
        F.col("Email").alias("email_original"),
        email_norm.alias("email_normalizado"),
        # Sin puntos en la parte local: juan.perez@ y juanperez@ son la misma persona
        F.concat(F.regexp_replace(local, r"\.", ""), F.lit("@"), dominio).alias("email_canonico"),
        F.col("Phone").alias("telefono"),
        F.col("City").alias("ciudad_original"),
        F.coalesce(mapa_ciudades[F.upper(F.trim("City"))], F.trim("City")).alias("ciudad"),
        F.col("CountryCode").alias("codigo_pais"),
        F.lower("Segment").alias("segmento"),
        F.col("SignupDate").alias("fecha_alta"),
        F.col("IsActive").alias("es_activo"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        email_norm.rlike(REGEX_EMAIL).alias("es_email_valido"),
        *tecnicas())


publicar_silver("clientes", "limpio_clientes", "id_cliente", historial_de=["segmento"])


# =============================================================================
# 3. Productos · SCD1
# =============================================================================
@dlt.view(name="limpio_productos", comment="Productos de bronze renombrados y validados.")
@dlt.expect_all_or_fail(reglas("productos", "fallar"))
@dlt.expect_all_or_drop(reglas("productos", "descartar"))
@dlt.expect_all(reglas("productos", "advertir"))
def limpio_productos():
    df = con_secuencia(leer_bronze("products_raw"))
    return df.select(
        F.col("ProductID").alias("id_producto"),
        F.trim("SKU").alias("sku"),
        F.trim("ProductName").alias("nombre_producto"),
        F.trim("Category").alias("categoria"),
        F.col("Price").alias("precio"),
        F.col("Description").alias("descripcion"),
        F.lower("Status").alias("estado"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        F.col("Description").isNotNull().alias("tiene_descripcion"),
        *tecnicas())


publicar_silver("productos", "limpio_productos", "id_producto")


# =============================================================================
# 4. Pedidos · SCD1
# =============================================================================
@dlt.view(name="limpio_pedidos", comment="Pedidos de bronze renombrados y validados.")
@dlt.expect_all_or_fail(reglas("pedidos", "fallar"))
@dlt.expect_all_or_drop(reglas("pedidos", "descartar"))
@dlt.expect_all(reglas("pedidos", "advertir"))
def limpio_pedidos():
    df = con_secuencia(leer_bronze("orders_raw"))
    return df.select(
        F.col("OrderID").alias("id_pedido"),
        F.col("CustomerID").alias("id_cliente"),
        F.col("OrderDate").alias("fecha_pedido"),
        F.lower("Channel").alias("canal"),
        F.lower("Status").alias("estado"),
        F.col("TotalAmount").alias("monto_total"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        *tecnicas())


publicar_silver("pedidos", "limpio_pedidos", "id_pedido")


# =============================================================================
# 5. Líneas de pedido · SCD1 (incluye los DELETE de líneas)
# =============================================================================
@dlt.view(name="limpio_lineas_pedido", comment="Líneas de pedido de bronze renombradas y validadas.")
@dlt.expect_all_or_fail(reglas("lineas_pedido", "fallar"))
@dlt.expect_all_or_drop(reglas("lineas_pedido", "descartar"))
@dlt.expect_all(reglas("lineas_pedido", "advertir"))
def limpio_lineas_pedido():
    df = con_secuencia(leer_bronze("order_items_raw"))
    return df.select(
        F.col("OrderItemID").alias("id_linea"),
        F.col("OrderID").alias("id_pedido"),
        F.col("ProductID").alias("id_producto"),
        F.col("Quantity").alias("cantidad"),
        F.col("UnitPrice").alias("precio_unitario"),
        F.col("DiscountAmount").alias("descuento"),
        # Importe de la línea: base de las ventas en gold (no el total de cabecera)
        (F.col("Quantity") * F.col("UnitPrice") - F.col("DiscountAmount")).cast("decimal(14,2)").alias("monto_linea"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        *tecnicas())


publicar_silver("lineas_pedido", "limpio_lineas_pedido", "id_linea")


# =============================================================================
# 6. Pagos · SCD1 + historial SCD2 de estado
# =============================================================================
@dlt.view(name="limpio_pagos", comment="Pagos de bronze renombrados y validados.")
@dlt.expect_all_or_fail(reglas("pagos", "fallar"))
@dlt.expect_all_or_drop(reglas("pagos", "descartar"))
@dlt.expect_all(reglas("pagos", "advertir"))
def limpio_pagos():
    df = con_secuencia(leer_bronze("payments_raw"))
    return df.select(
        F.col("PaymentID").alias("id_pago"),
        F.col("OrderID").alias("id_pedido"),
        F.lower("PaymentMethod").alias("metodo_pago"),
        F.col("Amount").alias("monto"),
        F.lower("Status").alias("estado"),
        F.col("PaymentDate").alias("fecha_pago"),
        F.col("TransactionRef").alias("referencia_transaccion"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        *tecnicas())


publicar_silver("pagos", "limpio_pagos", "id_pago", historial_de=["estado"])


# =============================================================================
# 7. Tickets de soporte · SCD1 (incluye los DELETE de tickets)
# =============================================================================
@dlt.view(name="limpio_tickets", comment="Tickets de bronze renombrados y validados.")
@dlt.expect_all_or_fail(reglas("tickets", "fallar"))
@dlt.expect_all_or_drop(reglas("tickets", "descartar"))
@dlt.expect_all(reglas("tickets", "advertir"))
def limpio_tickets():
    df = con_secuencia(leer_bronze("support_tickets_raw"))
    return df.select(
        F.col("TicketID").alias("id_ticket"),
        F.col("CustomerID").alias("id_cliente"),
        F.col("OrderID").alias("id_pedido"),
        F.col("Subject").alias("asunto"),
        F.col("Body").alias("cuerpo"),
        F.lower("Priority").alias("prioridad"),
        F.lower("Status").alias("estado"),
        F.col("OpenedAt").alias("fecha_apertura"),
        F.col("ClosedAt").alias("fecha_cierre"),
        F.col("CreatedAt").alias("creado_en"),
        F.col("UpdatedAt").alias("actualizado_en"),
        F.col("Body").isNotNull().alias("tiene_cuerpo"),
        *tecnicas())


publicar_silver("tickets", "limpio_tickets", "id_ticket")


# =============================================================================
# 8. Controles de integridad entre tablas (vistas materializadas)
# =============================================================================
@dlt.table(name="control_totales_pedido",
           comment="Integridad cabecera-detalle por pedido: suma de líneas frente al total, pedidos sin líneas "
                   "y cliente inexistente.")
def control_totales_pedido():
    pedidos = dlt.read("pedidos")
    lineas = (dlt.read("lineas_pedido").groupBy("id_pedido")
              .agg(F.count("*").alias("n_lineas"), F.sum("monto_linea").alias("suma_lineas")))
    clientes = dlt.read("clientes").select("id_cliente", F.lit(True).alias("_cliente_existe"))
    return (pedidos.join(lineas, "id_pedido", "left").join(clientes, "id_cliente", "left")
            .select("id_pedido", "id_cliente", "fecha_pedido", "estado", "monto_total",
                    F.coalesce("n_lineas", F.lit(0)).alias("n_lineas"),
                    F.coalesce("suma_lineas", F.lit(0)).cast("decimal(14,2)").alias("suma_lineas"),
                    (F.coalesce("n_lineas", F.lit(0)) == 0).alias("es_sin_lineas"),
                    (F.abs(F.col("monto_total") - F.coalesce("suma_lineas", F.lit(0))) > 0.01)
                    .alias("es_total_descuadrado"),
                    F.col("_cliente_existe").isNull().alias("es_cliente_inexistente")))


@dlt.table(name="control_integridad",
           comment="Resumen de integridad referencial entre tablas de silver (filas huérfanas por relación).")
def control_integridad():
    def huerfanos(hija, col, padre, col_padre, relacion):
        h = dlt.read(hija).select(F.col(col).alias("k")).filter("k IS NOT NULL")
        p = dlt.read(padre).select(F.col(col_padre).alias("k")).distinct()
        return h.join(p, "k", "left_anti").agg(F.count("*").alias("filas_huerfanas")) \
                .select(F.lit(relacion).alias("relacion"), "filas_huerfanas")
    return (huerfanos("lineas_pedido", "id_pedido", "pedidos", "id_pedido", "lineas_pedido → pedidos")
            .unionByName(huerfanos("lineas_pedido", "id_producto", "productos", "id_producto", "lineas_pedido → productos"))
            .unionByName(huerfanos("pagos", "id_pedido", "pedidos", "id_pedido", "pagos → pedidos"))
            .unionByName(huerfanos("pedidos", "id_cliente", "clientes", "id_cliente", "pedidos → clientes"))
            .unionByName(huerfanos("tickets", "id_cliente", "clientes", "id_cliente", "tickets → clientes")))
