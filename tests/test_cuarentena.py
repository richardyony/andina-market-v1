"""
===============================================================================
 Archivo      : tests/test_cuarentena.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Pruebas unitarias de silver (nivel 2)
 Propósito    : La promesa de silver: «nada se descarta en silencio». Toda fila que las
                expectations de descarte sacan de silver aparece en la cuarentena, con
                las reglas que incumplió; y ninguna fila válida aparece en ella.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-14
===============================================================================
"""

import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

import reglas_calidad as rc
from comun import filas_en_cuarentena
from filas import CLAVES, VALIDAS, df_de, fila


def lote_mixto(entidad: str) -> list[dict]:
    """Un lote como el que llegaría de bronze: válidas, inválidas, nulas y un borrado inválido."""
    clave = CLAVES[entidad]
    malas = {
        "clientes": [{"segmento": "premium"}, {"codigo_pais": None}, {"segmento": "oro", "codigo_pais": "AR"}],
        "productos": [{"precio": Decimal("-1")}, {"sku": None}, {"precio": None, "estado": "x"}],
        "pedidos": [{"canal": "fax"}, {"monto_total": None}, {"estado": "x", "canal": None}],
        "items_pedido": [{"cantidad": 0}, {"precio_unitario": None}, {"cantidad": -1, "descuento": Decimal("-1")}],
        "pagos": [{"monto": Decimal("-1")}, {"metodo_pago": None}, {"estado": "x", "metodo_pago": "cheque"}],
        "tickets": [{"prioridad": "urgente"}, {"estado": None}, {"prioridad": None, "estado": "x"}],
    }[entidad]
    filas = [fila(entidad, **{clave: 1}), fila(entidad, **{clave: 2})]
    filas += [fila(entidad, **{clave: 10 + i}, **cambio) for i, cambio in enumerate(malas)]
    filas.append(fila(entidad, **{clave: 99}, _op="D", **malas[0]))     # borrado: nunca se descarta
    return filas


def quedan_en_silver(df, entidad):
    """Lo que dejan pasar las expectations de descarte (expect_all_or_drop)."""
    condicion = " AND ".join(f"({e})" for e in rc.reglas(entidad, "descartar").values())
    return df.filter(F.expr(condicion))


@pytest.mark.parametrize("entidad", list(VALIDAS))
def test_nada_se_descarta_en_silencio(spark, entidad):
    """silver + cuarentena = todo lo que llegó, sin solaparse."""
    df = df_de(spark, entidad, lote_mixto(entidad))
    clave = CLAVES[entidad]
    en_silver = {str(f[clave]) for f in quedan_en_silver(df, entidad).collect()}
    en_cuarentena = {f.clave for f in filas_en_cuarentena(df, entidad, clave).collect()}
    todas = {str(f[clave]) for f in df.collect()}
    assert en_silver | en_cuarentena == todas
    assert en_silver & en_cuarentena == set()
    assert en_silver == {"1", "2", "99"}            # válidas + el borrado
    assert en_cuarentena == {"10", "11", "12"}


@pytest.mark.parametrize("entidad", list(VALIDAS))
def test_cuarentena_lista_todas_las_reglas_incumplidas(spark, entidad):
    """La fila que rompe dos reglas a la vez las nombra a ambas."""
    df = df_de(spark, entidad, lote_mixto(entidad))
    por_clave = {f.clave: set(f.reglas_fallidas) for f in filas_en_cuarentena(df, entidad, CLAVES[entidad]).collect()}
    assert len(por_clave["12"]) == 2


def test_cuarentena_conserva_la_fila_original_y_su_trazabilidad(spark):
    df = df_de(spark, "productos", [fila("productos", id_producto=7, precio=Decimal("-3.50"))])
    q = filas_en_cuarentena(df, "productos", "id_producto").collect()[0]
    original = json.loads(q.fila)
    assert (q.tabla, q.clave, q.reglas_fallidas) == ("productos", "7", ["precio_no_negativo"])
    assert original["precio"] == -3.5 and original["sku"] == "AND-0001"
    assert not any(c.startswith("_") for c in original)            # el JSON lleva solo datos de negocio
    assert (q._op, q._lsn, q._id_ejecucion) == ("I", "0x0001", "test")
    assert q._cuarentena_ts is not None
