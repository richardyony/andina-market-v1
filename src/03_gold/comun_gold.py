"""
===============================================================================
 Archivo      : gold/comun_gold.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Gold (nivel 2) · utilidades compartidas del pipeline pl_andina_gold
 Propósito    : Lectura de silver (otro pipeline), claves sustitutas deterministas y la
                unión de un hecho con la versión SCD2 del cliente vigente en una fecha.
                No declara datasets: cada notebook dim_* / fact_* declara los suyos.
 Decisiones   : - Claves sustitutas por hash: recalcular gold produce las mismas claves.
                - Gold lee silver como tablas de Unity Catalog: silver y gold son pipelines
                  distintos, unidos solo por el contrato de las tablas.
 Parámetros   : catalogo (configuración del pipeline, la pone el bundle)
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 2.1 · 2026-10-12 · un notebook por tabla (antes gold.py)
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
CATALOGO = spark.conf.get("catalogo")

INICIO_DE_LOS_TIEMPOS = F.lit("1900-01-01 00:00:00").cast("timestamp")
FIN_DE_LOS_TIEMPOS = F.lit("9999-12-31 00:00:00").cast("timestamp")
ESTADOS_VENTA_EFECTIVA = ["pagado", "enviado", "entregado"]


def leer_silver(tabla: str):
    """Lee una tabla de silver (la publica el pipeline pl_andina_silver)."""
    return spark.read.table(f"{CATALOGO}.silver.{tabla}")


def sk(*cols):
    """Clave sustituta determinista: el mismo dato siempre produce la misma clave."""
    return F.xxhash64(*cols)


def sk_fecha(col):
    """Clave de fecha legible: AAAAMMDD."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def cliente_vigente_en(df, dim_clientes, col_id: str, col_fecha: str):
    """Une cada fila con la versión de dim_clientes vigente en la fecha indicada (SCD2):
    una venta de marzo cuenta para el segmento que el cliente tenía en marzo.
    dim_clientes la pasa cada hecho con dlt.read("dim_clientes") (dataset del mismo pipeline)."""
    dim = dim_clientes.select("sk_cliente", F.col("id_cliente").alias("_id"), "vigente_desde", "vigente_hasta")
    cond = ((F.col(col_id) == F.col("_id")) & (F.col(col_fecha) >= F.col("vigente_desde"))
            & (F.col(col_fecha) < F.col("vigente_hasta")))
    return df.join(dim, cond, "left").drop("_id", "vigente_desde", "vigente_hasta")
