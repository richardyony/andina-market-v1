# Databricks notebook source
# MAGIC %md
# MAGIC # nb_bronze_sqlserver_ingesta
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Proyecto | Andina Market · Reto técnico de Ingeniería de Datos |
# MAGIC | Capa | Ingesta → Landing → **Bronze** (Nivel 1) |
# MAGIC | Propósito | Extraer los cambios de Azure SQL con **CDC** y cargarlos a Bronze con **Auto Loader** |
# MAGIC | Flujo | `Azure SQL (CDC)` → Parquet en `landing.sqlserver` → tablas Delta `bronze.<tabla>_raw` |
# MAGIC | Parámetros | `catalogo`, `secret_scope`, `etapa` (todo · extraer · cargar), `tablas` (* o lista), `forzar_full` |
# MAGIC | Lee | Azure SQL `sqldb-andinamarket-cus-001` (usuario de solo lectura), `<catalogo>.control.*` |
# MAGIC | Escribe | `/Volumes/<catalogo>/landing/sqlserver/...`, `<catalogo>.bronze.*_raw`, `<catalogo>.control.*` |
# MAGIC | Idempotencia | El watermark (LSN) solo avanza si el Parquet se escribió bien; Auto Loader usa checkpoint (exactly-once). Re-ejecutar no duplica ni pierde datos. |
# MAGIC | Cómputo | Serverless (job) |
# MAGIC | Autoría | Richard Huamán · con asistencia de IA (Claude) |
# MAGIC | Versión | 1.2 · 2026-10-12 · registro de ctl_objetos con DataFrame (corrige paréntesis sobrante en el MERGE) |
# MAGIC
# MAGIC **Secciones:** 1. Parámetros · 2. Configuración · 3. Tablas de control · 4. Funciones de extracción · 5. Extracción CDC → landing · 6. Carga Auto Loader → Bronze · 7. Resumen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros
# MAGIC Los widgets permiten que **el mismo notebook** corra en dev y prd: el job o el bundle solo cambian los valores.

# COMMAND ----------

dbutils.widgets.text("catalogo", "andina_dev", "Catálogo (ambiente)")
dbutils.widgets.text("secret_scope", "kv-andina-dev", "Secret scope (Key Vault)")
dbutils.widgets.dropdown("etapa", "todo", ["todo", "extraer", "cargar"], "Etapa")
dbutils.widgets.text("tablas", "*", "Tablas (* o lista separada por comas)")
dbutils.widgets.dropdown("forzar_full", "false", ["false", "true"], "Forzar carga completa")

CATALOGO = dbutils.widgets.get("catalogo").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
ETAPA = dbutils.widgets.get("etapa")
FILTRO_TABLAS = [t.strip() for t in dbutils.widgets.get("tablas").split(",") if t.strip() and t.strip() != "*"]
FORZAR_FULL = dbutils.widgets.get("forzar_full") == "true"

print(f"catálogo={CATALOGO} · scope={SECRET_SCOPE} · etapa={ETAPA} · tablas={FILTRO_TABLAS or 'todas'} · forzar_full={FORZAR_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuración
# MAGIC - Credenciales desde el **secret scope** (Key Vault del ambiente): nunca en el código.
# MAGIC - Rutas de landing y checkpoints derivadas del catálogo.
# MAGIC - Cada corrida tiene un `id_ejecucion` único que viaja en los archivos y en la bitácora.

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

JDBC_URL = ("jdbc:sqlserver://sql-andinamarket-cus-001.database.windows.net:1433;"
            "database=sqldb-andinamarket-cus-001;encrypt=true;loginTimeout=60")
JDBC_OPCIONES = {
    "url": JDBC_URL,
    "user": dbutils.secrets.get(SECRET_SCOPE, "sql-usuario"),
    "password": dbutils.secrets.get(SECRET_SCOPE, "sql-password"),
}

RUTA_LANDING = f"/Volumes/{CATALOGO}/landing/sqlserver"
RUTA_CHECKPOINTS = f"/Volumes/{CATALOGO}/bronze/checkpoints/sqlserver"
ID_EJECUCION = str(uuid.uuid4())
FECHA_EXTRACCION = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Verificar que existan los Volumes (se crean con 04_volumes_landing.sql / bundle)
for ruta in (RUTA_LANDING, f"/Volumes/{CATALOGO}/bronze/checkpoints"):
    try:
        dbutils.fs.ls(ruta)
    except Exception as e:
        raise RuntimeError(f"No existe o no hay acceso a {ruta}. Crea los Volumes antes de ejecutar.") from e

print(f"id_ejecucion={ID_EJECUCION} · fecha_extraccion={FECHA_EXTRACCION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tablas de control (idempotente)
# MAGIC Son la **memoria** de la ingesta:
# MAGIC - `ctl_objetos` → **qué** extraer (una fila por tabla de origen). Agregar una tabla = insertar una fila.
# MAGIC - `ctl_watermarks` → **hasta dónde** se extrajo (último LSN por tabla).
# MAGIC - `ctl_ejecuciones` → **qué pasó** en cada corrida (auditoría).
# MAGIC
# MAGIC `CREATE ... IF NOT EXISTS` y `MERGE` permiten ejecutar esta sección siempre, sin duplicar nada.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.control "
          "COMMENT 'Metadatos operativos de la ingesta: objetos, watermarks y ejecuciones.'")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.control.ctl_objetos (
  id_objeto INT, fuente STRING, esquema_origen STRING, tabla_origen STRING,
  capture_instance STRING, clave_primaria STRING, carpeta_landing STRING,
  tabla_bronze STRING, orden_carga INT, activo BOOLEAN
) COMMENT 'Qué tablas extrae la ingesta CDC y hacia dónde.'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.control.ctl_watermarks (
  tabla_origen STRING, ultimo_lsn STRING, tipo_ultima_carga STRING, actualizado_en TIMESTAMP
) COMMENT 'Último LSN extraído por tabla. Solo avanza tras escribir el Parquet con éxito.'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.control.ctl_ejecuciones (
  id_ejecucion STRING, etapa STRING, tabla_origen STRING, tipo_carga STRING,
  lsn_desde STRING, lsn_hasta STRING, filas BIGINT, ruta STRING,
  inicio TIMESTAMP, fin TIMESTAMP, estado STRING, mensaje_error STRING
) COMMENT 'Bitácora de cada extracción y carga: qué se leyó, cuánto y si terminó bien.'""")

# Registro de las 6 tablas de origen (orden = padres antes que hijos).
# DataFrame con esquema explícito: tipos declarados y sin SQL armado a mano.
objetos_ref = spark.createDataFrame([
    (1, "sqlserver", "dbo", "Customers",      "dbo_Customers",      "CustomerID",  "customers",       "customers_raw",       1, True),
    (2, "sqlserver", "dbo", "Products",       "dbo_Products",       "ProductID",   "products",        "products_raw",        2, True),
    (3, "sqlserver", "dbo", "Orders",         "dbo_Orders",         "OrderID",     "orders",          "orders_raw",          3, True),
    (4, "sqlserver", "dbo", "OrderItems",     "dbo_OrderItems",     "OrderItemID", "order_items",     "order_items_raw",     4, True),
    (5, "sqlserver", "dbo", "Payments",       "dbo_Payments",       "PaymentID",   "payments",        "payments_raw",        5, True),
    (6, "sqlserver", "dbo", "SupportTickets", "dbo_SupportTickets", "TicketID",    "support_tickets", "support_tickets_raw", 6, True),
], "id_objeto INT, fuente STRING, esquema_origen STRING, tabla_origen STRING, capture_instance STRING, "
   "clave_primaria STRING, carpeta_landing STRING, tabla_bronze STRING, orden_carga INT, activo BOOLEAN")
objetos_ref.createOrReplaceTempView("v_objetos_ref")

spark.sql(f"""
MERGE INTO {CATALOGO}.control.ctl_objetos t
USING v_objetos_ref s
ON t.id_objeto = s.id_objeto
WHEN NOT MATCHED THEN INSERT *""")   # no se pisan cambios manuales (p. ej. activo = false)

objetos = [r.asDict() for r in spark.sql(
    f"SELECT * FROM {CATALOGO}.control.ctl_objetos WHERE activo ORDER BY orden_carga").collect()]
if FILTRO_TABLAS:
    objetos = [o for o in objetos if o["tabla_origen"] in FILTRO_TABLAS]
print("Tablas a procesar:", [o["tabla_origen"] for o in objetos])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funciones de extracción
# MAGIC **Lógica del watermark (LSN = posición en el log de transacciones de SQL Server):**
# MAGIC 1. Sin watermark, `forzar_full = true` o watermark vencido (más antiguo que el LSN mínimo que CDC aún conserva) → **foto completa** (`_op = 'R'`).
# MAGIC 2. Watermark igual al LSN máximo actual → **sin cambios**, no se escribe nada.
# MAGIC 3. Si no → **incremental**: `cdc.fn_cdc_get_all_changes_<tabla>` desde el LSN siguiente al guardado hasta el máximo actual. Devuelve **cada** cambio (incluidos estados intermedios y DELETE).
# MAGIC
# MAGIC El LSN máximo se lee **antes** de la foto completa: un cambio ocurrido durante la foto puede llegar repetido en la siguiente corrida (Silver lo absorbe con MERGE), pero nunca se pierde.
# MAGIC
# MAGIC Las dos rutas producen **las mismas columnas técnicas**: `_lsn`, `_seqval`, `_op` (R/I/U/D), `_commit_ts`.

# COMMAND ----------

def leer_sql(consulta: str):
    """Ejecuta una consulta de solo lectura en Azure SQL y devuelve un DataFrame."""
    return spark.read.format("jdbc").options(**JDBC_OPCIONES).option("query", consulta).load()


def valor_sql(consulta: str):
    """Devuelve el primer valor de una consulta escalar.
    Toda columna necesita alias: Spark envuelve la consulta en una subconsulta y SQL Server lo exige."""
    return leer_sql(consulta).collect()[0][0]


def columnas_origen(esquema: str, tabla: str) -> str:
    """Lista de columnas de la tabla de origen, en orden y con corchetes (evita palabras reservadas)."""
    # Sin ORDER BY en SQL: Spark envuelve la consulta en una subconsulta y SQL Server no lo permite.
    filas = leer_sql(f"""SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS
                         WHERE TABLE_SCHEMA = '{esquema}' AND TABLE_NAME = '{tabla}'""").collect()
    return ", ".join(f"[{r[0]}]" for r in sorted(filas, key=lambda r: r[1]))


def leer_watermark(tabla: str):
    filas = spark.sql(f"SELECT ultimo_lsn FROM {CATALOGO}.control.ctl_watermarks "
                      "WHERE tabla_origen = :t", args={"t": tabla}).collect()
    return filas[0][0] if filas else None


def guardar_watermark(tabla: str, lsn: str, tipo: str) -> None:
    spark.sql(f"""
        MERGE INTO {CATALOGO}.control.ctl_watermarks w
        USING (SELECT :t AS tabla_origen, :lsn AS ultimo_lsn, :tipo AS tipo_ultima_carga) s
        ON w.tabla_origen = s.tabla_origen
        WHEN MATCHED THEN UPDATE SET w.ultimo_lsn = s.ultimo_lsn,
             w.tipo_ultima_carga = s.tipo_ultima_carga, w.actualizado_en = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (tabla_origen, ultimo_lsn, tipo_ultima_carga, actualizado_en)
             VALUES (s.tabla_origen, s.ultimo_lsn, s.tipo_ultima_carga, current_timestamp())""",
              args={"t": tabla, "lsn": lsn, "tipo": tipo})


def registrar(etapa, tabla, tipo, desde, hasta, filas, ruta, inicio, estado, error=None) -> None:
    """Agrega una fila a la bitácora ctl_ejecuciones."""
    spark.sql(f"""
        INSERT INTO {CATALOGO}.control.ctl_ejecuciones VALUES
        (:id, :etapa, :tabla, :tipo, :desde, :hasta, :filas, :ruta, :inicio, current_timestamp(), :estado, :error)""",
              args={"id": ID_EJECUCION, "etapa": etapa, "tabla": tabla, "tipo": tipo, "desde": desde,
                    "hasta": hasta, "filas": filas, "ruta": ruta, "inicio": inicio,
                    "estado": estado, "error": (error or "")[:2000] or None})


def extraer_tabla(obj: dict) -> dict:
    """Extrae una tabla (foto completa o cambios CDC) y la escribe como Parquet en landing."""
    tabla, esquema, ci = obj["tabla_origen"], obj["esquema_origen"], obj["capture_instance"]
    inicio = datetime.now(timezone.utc)

    max_lsn = valor_sql("SELECT CONVERT(VARCHAR(30), sys.fn_cdc_get_max_lsn(), 1) AS max_lsn")
    min_lsn = valor_sql(f"SELECT CONVERT(VARCHAR(30), sys.fn_cdc_get_min_lsn('{ci}'), 1) AS min_lsn")
    ultimo = leer_watermark(tabla)
    cols = columnas_origen(esquema, tabla)

    # Los LSN en hex tienen largo fijo (0x + 20 dígitos), así que se comparan como texto.
    vencido = ultimo is not None and ultimo < min_lsn
    if ultimo is None or FORZAR_FULL or vencido:
        tipo, desde = "full", None
        consulta = f"""SELECT CAST('{max_lsn}' AS VARCHAR(30)) AS _lsn, CAST(NULL AS VARCHAR(30)) AS _seqval,
                              'R' AS _op, CAST(NULL AS DATETIME2(3)) AS _commit_ts, {cols}
                       FROM [{esquema}].[{tabla}]"""
        if vencido:
            print(f"  ⚠ {tabla}: watermark {ultimo} fuera de la retención de CDC (mín {min_lsn}) → foto completa")
    elif ultimo >= max_lsn:
        registrar("extraer", tabla, "incremental", ultimo, max_lsn, 0, None, inicio, "SIN_CAMBIOS")
        return {"tabla": tabla, "tipo": "incremental", "filas": 0, "estado": "SIN_CAMBIOS"}
    else:
        tipo, desde = "incremental", ultimo
        consulta = f"""SELECT CONVERT(VARCHAR(30), __$start_lsn, 1) AS _lsn,
                              CONVERT(VARCHAR(30), __$seqval, 1)   AS _seqval,
                              CASE __$operation WHEN 1 THEN 'D' WHEN 2 THEN 'I' WHEN 4 THEN 'U' END AS _op,
                              CAST(sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS DATETIME2(3)) AS _commit_ts,
                              {cols}
                       FROM cdc.fn_cdc_get_all_changes_{ci}(sys.fn_cdc_increment_lsn({ultimo}), {max_lsn}, N'all')"""

    # Carpeta por fecha y por corrida: re-ejecutar la misma corrida sobrescribe solo su carpeta.
    # (Sin '_' al inicio: Spark ignora carpetas que empiezan con '_' o '.')
    ruta = f"{RUTA_LANDING}/{obj['carpeta_landing']}/fecha_extraccion={FECHA_EXTRACCION}/id_ejecucion={ID_EJECUCION}"
    (leer_sql(consulta)
        .withColumn("_extract_ts", F.current_timestamp())
        .withColumn("_id_ejecucion", F.lit(ID_EJECUCION))
        .write.mode("overwrite").parquet(ruta))

    filas = spark.read.parquet(ruta).count()
    estado = "OK"
    if filas == 0:                                   # la base cambió, pero no esta tabla:
        dbutils.fs.rm(ruta, True)                    # no dejar carpetas vacías en landing
        ruta, estado = None, "SIN_CAMBIOS"
    guardar_watermark(tabla, max_lsn, tipo)          # solo después de escribir con éxito
    registrar("extraer", tabla, tipo, desde, max_lsn, filas, ruta, inicio, estado)
    return {"tabla": tabla, "tipo": tipo, "filas": filas, "estado": estado}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extracción CDC → landing
# MAGIC Recorre las tablas en `orden_carga`. Un error en una tabla **no detiene** las demás: se registra y, al final, el notebook falla para que el job alerte.

# COMMAND ----------

resultados_extraccion, errores = [], []

if ETAPA in ("todo", "extraer"):
    for obj in objetos:
        try:
            r = extraer_tabla(obj)
            print(f"  ✓ {r['tabla']:<15} {r['tipo']:<12} {r['filas']:>8,} filas  {r['estado']}")
            resultados_extraccion.append(r)
        except Exception as e:
            print(f"  ✗ {obj['tabla_origen']}: {e}")
            registrar("extraer", obj["tabla_origen"], None, None, None, None, None,
                      datetime.now(timezone.utc), "ERROR", str(e))
            errores.append(obj["tabla_origen"])
else:
    print("Etapa 'cargar': se omite la extracción.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Carga Auto Loader → Bronze
# MAGIC Por cada tabla, Auto Loader:
# MAGIC - detecta **solo los archivos nuevos** de landing (el checkpoint recuerda los procesados → exactly-once);
# MAGIC - agrega `_source_file` (de qué archivo vino) y `_ingestion_ts` (cuándo entró a Bronze);
# MAGIC - escribe en modo **append** en `bronze.<tabla>_raw` (Bronze no deduplica ni limpia);
# MAGIC - con `availableNow` procesa lo pendiente y se apaga (apto para serverless).
# MAGIC
# MAGIC Cambios de esquema: columnas inesperadas quedan en `_rescued_data` en lugar de romper la carga.

# COMMAND ----------

resultados_carga = []

def cargar_tabla(obj: dict) -> dict:
    """Carga incremental de landing a Bronze con Auto Loader (availableNow + checkpoint)."""
    tabla, carpeta = obj["tabla_origen"], obj["carpeta_landing"]
    destino = f"{CATALOGO}.bronze.{obj['tabla_bronze']}"
    inicio = datetime.now(timezone.utc)
    # Conteo ANTES de cargar (0 si la tabla aún no existe: primera corrida)
    antes = spark.table(destino).count() if spark.catalog.tableExists(destino) else 0

    consulta = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{RUTA_CHECKPOINTS}/{carpeta}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.partitionColumns", "")          # no inferir columnas desde las carpetas
        .load(f"{RUTA_LANDING}/{carpeta}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{RUTA_CHECKPOINTS}/{carpeta}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(destino))
    consulta.awaitTermination()

    # Filas que entraron en ESTA corrida = conteo después − conteo antes.
    # Bronze es solo append, así que la diferencia es exacta.
    # (recentProgress llega vacío en serverless y comparar por _ingestion_ts
    #  falló por zona horaria/precisión → se descartaron ambos métodos)
    filas = spark.table(destino).count() - antes
    registrar("cargar", tabla, None, None, None, filas, destino, inicio, "OK" if filas else "SIN_CAMBIOS")
    return {"tabla": tabla, "destino": destino, "filas": filas}

if ETAPA in ("todo", "cargar"):
    for obj in objetos:
        if obj["tabla_origen"] in errores:
            print(f"  – {obj['tabla_origen']}: se omite porque falló su extracción")
            continue
        try:
            r = cargar_tabla(obj)
            print(f"  ✓ {r['destino']:<40} {r['filas']:>8,} filas nuevas")
            resultados_carga.append(r)
        except Exception as e:
            print(f"  ✗ {obj['tabla_origen']}: {e}")
            registrar("cargar", obj["tabla_origen"], None, None, None, None, None,
                      datetime.now(timezone.utc), "ERROR", str(e))
            errores.append(obj["tabla_origen"])
else:
    print("Etapa 'extraer': se omite la carga a Bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen de la corrida
# MAGIC Bitácora de esta ejecución y watermarks actuales. Si hubo errores, el notebook falla al final para que el job lo marque y envíe la alerta.

# COMMAND ----------

display(spark.sql(f"""
    SELECT etapa, tabla_origen, tipo_carga, filas, estado, lsn_desde, lsn_hasta,
           round(timestampdiff(MILLISECOND, inicio, fin) / 1000, 1) AS segundos, mensaje_error
    FROM {CATALOGO}.control.ctl_ejecuciones
    WHERE id_ejecucion = :id
    ORDER BY etapa DESC, tabla_origen""", args={"id": ID_EJECUCION}))

display(spark.sql(f"SELECT * FROM {CATALOGO}.control.ctl_watermarks ORDER BY tabla_origen"))

if errores:
    raise RuntimeError(f"La ingesta terminó con errores en: {sorted(set(errores))}. Ver {CATALOGO}.control.ctl_ejecuciones")
print(f"Ingesta completada sin errores · id_ejecucion={ID_EJECUCION}")