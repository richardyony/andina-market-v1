"""
===============================================================================
 Archivo      : silver/comun.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Silver (nivel 2) · utilidades compartidas del pipeline pl_andina_silver
 Propósito    : Transformaciones puras que repiten todas las entidades: leer bronze como
                stream, calcular el orden de los cambios, armar las filas de cuarentena y
                los parámetros de AUTO CDC.

 Importante   : este módulo NO declara datasets. Cada notebook slv_<entidad> declara los suyos
                (vista, tabla y AUTO CDC): el pipeline registra lo que
                declara cada notebook fuente, y así cada notebook muestra todo lo que publica.

 Flujo por entidad (limpiar → validar → publicar / apartar)
   bronze.<tabla>_raw ─▶ limpio_<entidad> ─┬─▶ validado_<entidad> ─▶ AUTO CDC ─▶ silver.<entidad>
     (cambios R/I/U/D)   (renombra,         │   (descarta lo que       (ordena por
                          normaliza; fallar │    rompe una regla)       _secuencia)
                          y advertir)       └─▶ silver.cuarentena (lo que rompe una regla de descarte)
   El descarte va en validado_ y no en limpio_: un expect_all_or_drop en una vista se aplica
   a todo el que la lee, y la cuarentena también lee limpio_.

 Parámetros   : catalogo (configuración del pipeline, la pone el bundle)
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 2.4 · 2026-10-21 · flujo con vista validado_: la cuarentena ya recibe las filas descartadas
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from reglas_calidad import CIUDADES_CANONICAS, reglas

spark = SparkSession.getActiveSession()
CATALOGO = spark.conf.get("catalogo")

# Columnas técnicas de bronze que no pasan a silver (la trazabilidad queda en
# _lsn, _fecha_efectiva, _id_ejecucion e _ingestion_ts).
EXCLUIR_DE_SILVER = ["_op", "_seqval", "_commit_ts", "_secuencia", "_extract_ts", "_rescued_data", "_source_file"]

# Columnas técnicas que cada vista arrastra para trazabilidad y para AUTO CDC.
TECNICAS = ["_op", "_lsn", "_seqval", "_commit_ts", "_fecha_efectiva", "_secuencia", "_id_ejecucion",
            "_ingestion_ts", "_extract_ts", "_rescued_data", "_source_file"]


def leer_bronze(tabla_raw: str):
    """Lee los cambios de bronze como stream: en cada corrida solo procesa lo nuevo."""
    return spark.readStream.table(f"{CATALOGO}.bronze.{tabla_raw}")


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


MAPA_CIUDADES = F.create_map(*[F.lit(x) for par in CIUDADES_CANONICAS.items() for x in par])


def normalizar_email(col):
    """Email comparable: sin espacios alrededor y en minúsculas."""
    return F.lower(F.trim(col))


def email_canonico(email_norm):
    """Email sin puntos en la parte local: juan.perez@ y juanperez@ son la misma persona.
    Se usa solo para detectar posibles duplicados, no para contactar al cliente."""
    partes = F.split(email_norm, "@")
    return F.concat(F.regexp_replace(partes.getItem(0), r"\.", ""), F.lit("@"), partes.getItem(1))


def ciudad_canonica(col):
    """Ciudad en su forma canónica (STGO → Santiago); si no está en el mapa, la original sin espacios."""
    return F.coalesce(MAPA_CIUDADES[F.upper(F.trim(col))], F.trim(col))


def parametros_cdc(clave: str, historial_de: list | None = None) -> dict:
    """Parámetros de AUTO CDC comunes a todas las entidades.
    Sin historial_de → SCD1 (estado actual). Con historial_de → SCD2 sobre esas columnas."""
    p = dict(keys=[clave], sequence_by=F.col("_secuencia"), apply_as_deletes=F.expr("_op = 'D'"),
             except_column_list=EXCLUIR_DE_SILVER, stored_as_scd_type=1)
    if historial_de:
        p.update(stored_as_scd_type=2, track_history_column_list=historial_de)
    return p


def filas_en_cuarentena(df, entidad: str, clave: str):
    """Filas de la vista que no cumplen una regla de descarte, con la lista de reglas
    incumplidas y la fila original en JSON. Nada se descarta en silencio."""
    reglas_descartar = reglas(entidad, "descartar")   # null-safe: NULL cuenta como incumplida
    fallidas = F.array_compact(F.array(*[F.when(~F.expr(expr), F.lit(nombre))
                                         for nombre, expr in reglas_descartar.items()]))
    return (df.withColumn("reglas_fallidas", fallidas)
              .filter(F.size("reglas_fallidas") > 0)
              .select(F.lit(entidad).alias("tabla"),
                      F.col(clave).cast("string").alias("clave"),
                      "reglas_fallidas",
                      F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("fila"),
                      "_op", "_lsn", "_id_ejecucion", "_ingestion_ts",
                      F.current_timestamp().alias("_cuarentena_ts")))
