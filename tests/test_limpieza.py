"""
===============================================================================
 Archivo      : tests/test_limpieza.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Pruebas unitarias de silver (nivel 2)
 Propósito    : Las transformaciones de limpieza de comun.py con los casos borde que
                siembra el generador (emails con mayúsculas o espacios, ciudades mal
                escritas o nulas) y el orden de aplicación de los cambios CDC.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-14
===============================================================================
"""

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from conftest import crear_df
from comun import ciudad_canonica, con_secuencia, email_canonico, normalizar_email


def aplicar(spark, funcion, valores, tipo="STRING"):
    """Aplica una función de columna a una lista de valores y devuelve los resultados en orden."""
    df = crear_df(spark, [(v,) for v in valores], f"x {tipo}")
    return [fila.y for fila in df.select(funcion(F.col("x")).alias("y")).collect()]


# --- Email -------------------------------------------------------------------------

@pytest.mark.parametrize("original, esperado", [
    ("  Ana.Quispe@Mail.COM ", "ana.quispe@mail.com"),
    ("ANA@MAIL.COM", "ana@mail.com"),
    ("ana@mail.com", "ana@mail.com"),
    (None, None),
])
def test_normalizar_email(spark, original, esperado):
    assert aplicar(spark, normalizar_email, [original]) == [esperado]


@pytest.mark.parametrize("normalizado, esperado", [
    ("juan.perez@mail.com", "juanperez@mail.com"),
    ("juanperez@mail.com", "juanperez@mail.com"),   # mismo canónico → posible duplicado
    ("j.u.a.n@mail.com", "juan@mail.com"),
    ("juan@mail.co.pe", "juan@mail.co.pe"),          # los puntos del dominio se conservan
    (None, None),
])
def test_email_canonico(spark, normalizado, esperado):
    assert aplicar(spark, email_canonico, [normalizado]) == [esperado]


def test_duplicado_por_email_se_detecta(spark):
    """Dos cuentas del mismo cliente, escritas distinto, dan el mismo email canónico."""
    a, b = aplicar(spark, lambda c: email_canonico(normalizar_email(c)), [" Juan.Perez@Mail.com", "juanperez@mail.COM"])
    assert a == b


# --- Ciudad ------------------------------------------------------------------------

@pytest.mark.parametrize("original, esperado", [
    ("Lima", "Lima"),
    ("  lima ", "Lima"),
    ("LIMA METROPOLITANA", "Lima"),
    ("Lima - Perú", "Lima"),
    ("Bogota", "Bogotá"),
    ("bogota d.c.", "Bogotá"),
    ("STGO", "Santiago"),
    ("Santiago de Chile", "Santiago"),
    ("  Arequipa ", "Arequipa"),     # fuera del mapa: se conserva, sin espacios
    (None, None),                    # nula: sigue nula (la marca la regla ciudad_informada)
])
def test_ciudad_canonica(spark, original, esperado):
    assert aplicar(spark, ciudad_canonica, [original]) == [esperado]


# --- Orden de los cambios CDC (_secuencia) -----------------------------------------

def test_secuencia_usa_commit_o_updated_at(spark):
    """Foto completa (_op R): sin _commit_ts, la fecha efectiva es UpdatedAt.
    Cambio incremental: la fecha efectiva es el commit en Azure SQL."""
    df = crear_df(spark, [
        ("R", None, datetime(2026, 10, 1), "0x01", None),
        ("U", datetime(2026, 10, 12, 1, 0), datetime(2026, 10, 12, 0, 59), "0x02", "0x0a"),
    ], "_op STRING, _commit_ts TIMESTAMP, UpdatedAt TIMESTAMP, _lsn STRING, _seqval STRING")
    filas = con_secuencia(df).select("_op", "_fecha_efectiva").collect()
    assert [f._fecha_efectiva for f in filas] == [datetime(2026, 10, 1), datetime(2026, 10, 12, 1, 0)]


def test_secuencia_desempata_por_lsn_y_seqval(spark):
    """Un pago pendiente → aprobado → reembolsado en la misma hora queda en su orden real."""
    mismo_commit = datetime(2026, 10, 12, 1, 0)
    df = crear_df(spark, [
        ("reembolsado", mismo_commit, "0x0002", "0x01"),
        ("pendiente", mismo_commit, "0x0001", "0x01"),
        ("aprobado", mismo_commit, "0x0001", "0x02"),
    ], "estado STRING, _commit_ts TIMESTAMP, _lsn STRING, _seqval STRING").withColumn("UpdatedAt", F.lit(None).cast("timestamp"))
    orden = [f.estado for f in con_secuencia(df).orderBy("_secuencia").collect()]
    assert orden == ["pendiente", "aprobado", "reembolsado"]
