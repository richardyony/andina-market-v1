"""
===============================================================================
 Archivo      : tests/conftest.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Pruebas unitarias de silver (nivel 2)
 Propósito    : Sesión de Spark local compartida por todas las pruebas, y una función
                para evaluar las reglas de calidad sobre filas armadas a mano.

 Por qué aquí : comun.py lee la configuración `catalogo` al importarse (en el pipeline
                la pone el bundle). Por eso la sesión se crea al cargar este archivo,
                antes de que pytest importe los módulos de prueba.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-14
===============================================================================
"""

import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK = (SparkSession.builder
         .master("local[1]")
         .appName("andina-tests")
         .config("catalogo", "andina_test")            # lo que comun.py espera del pipeline
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
SPARK.sparkContext.setLogLevel("ERROR")


@pytest.fixture(scope="session")
def spark():
    return SPARK


def _literal(valor) -> str:
    """Valor de Python como literal SQL de texto (luego se convierte al tipo de la columna)."""
    if valor is None:
        return "NULL"
    texto = valor.isoformat(sep=" ") if hasattr(valor, "isoformat") else str(valor)
    return "'" + texto.replace("\\", "\\\\").replace("'", "\\'") + "'"


def crear_df(spark, filas: list[tuple], esquema: str):
    """DataFrame con ese esquema DDL ("a INT, b STRING, ...") armado con SQL VALUES.

    No usa spark.createDataFrame a propósito: en Windows ese camino lanza un worker de
    Python que se cae con Spark local. Con VALUES todo corre en la JVM, igual en
    Windows, Linux o en la integración continua."""
    columnas = [c.strip().split(" ", 1) for c in re.split(r",\s*(?![^()]*\))", esquema)]
    alias = ", ".join(f"c{i}" for i in range(len(columnas)))
    valores = ", ".join("(" + ", ".join(_literal(v) for v in f) + ")" for f in filas)
    seleccion = ", ".join(f"CAST(c{i} AS {tipo}) AS {nombre}" for i, (nombre, tipo) in enumerate(columnas))
    return spark.sql(f"SELECT {seleccion} FROM VALUES {valores} AS t({alias})")


def evaluar_reglas(df, reglas_por_nombre: dict) -> list[dict]:
    """Para cada fila de df (en orden), el resultado de cada regla: True, False o None (NULL)."""
    marcas = [F.expr(expr).alias(nombre) for nombre, expr in reglas_por_nombre.items()]
    return [fila.asDict() for fila in df.select(*marcas).collect()]


def reglas_incumplidas(df, reglas_por_nombre: dict) -> list[set]:
    """Para cada fila de df (en orden), las reglas que NO da por cumplidas (False o NULL)."""
    return [{nombre for nombre, ok in fila.items() if ok is not True}
            for fila in evaluar_reglas(df, reglas_por_nombre)]
