"""
===============================================================================
 Archivo      : tests/test_reglas_calidad.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Pruebas unitarias de silver (nivel 2)
 Propósito    : Cada regla de reglas_calidad.py, probada con filas armadas a mano:
                una fila válida no rompe nada; cambiar un campo rompe exactamente la
                regla esperada, con la acción esperada (fallar, descartar o advertir).

 Por qué      : la fuente tiene CHECK de dominio, así que en la carga real la cuarentena
                quedó en 0 filas. Estas pruebas demuestran que las reglas actúan cuando
                llega un dato malo (otra fuente, un CHECK desactivado, un cambio de esquema).
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-14
===============================================================================
"""

import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

import reglas_calidad as rc
from conftest import evaluar_reglas, reglas_incumplidas
from filas import VALIDAS, df_de, fila

ACCIONES = ("fallar", "descartar", "advertir")
DDL = Path(__file__).resolve().parents[1] / "fuentes" / "01_ddl_tablas.sql"


def incumplidas(spark, entidad, accion, **cambios) -> set:
    """Reglas de una acción que incumple la fila válida con esos cambios."""
    return reglas_incumplidas(df_de(spark, entidad, [fila(entidad, **cambios)]), rc.reglas(entidad, accion))[0]


# --- 1. Una fila válida pasa todas las reglas -----------------------------------

@pytest.mark.parametrize("entidad", list(VALIDAS))
@pytest.mark.parametrize("accion", ACCIONES)
def test_fila_valida_cumple_todo(spark, entidad, accion):
    assert incumplidas(spark, entidad, accion) == set()


# --- 2. FALLAR: clave primaria nula detiene el pipeline --------------------------

@pytest.mark.parametrize("entidad, clave", [
    ("clientes", "id_cliente"), ("productos", "id_producto"), ("pedidos", "id_pedido"),
    ("items_pedido", "id_item"), ("pagos", "id_pago"), ("tickets", "id_ticket")])
def test_clave_nula_falla(spark, entidad, clave):
    assert incumplidas(spark, entidad, "fallar", **{clave: None}) == {f"{clave}_no_nulo"}


# --- 3. DESCARTAR: dato inválido → cuarentena -------------------------------------
# (entidad, campo cambiado, valor malo, regla que debe romperse)
CASOS_DESCARTAR = [
    ("clientes", "segmento", "premium", "segmento_valido"),
    ("clientes", "segmento", "VIP", "segmento_valido"),          # silver ya baja a minúsculas; si no, no pasa
    ("clientes", "codigo_pais", "AR", "pais_valido"),
    ("productos", "sku", None, "sku_informado"),
    ("productos", "precio", Decimal("-0.01"), "precio_no_negativo"),
    ("productos", "estado", "en_oferta", "estado_valido"),
    ("pedidos", "monto_total", Decimal("-10.00"), "monto_total_no_negativo"),
    ("pedidos", "canal", "telefono", "canal_valido"),
    ("pedidos", "estado", "perdido", "estado_valido"),
    ("items_pedido", "cantidad", 0, "cantidad_positiva"),
    ("items_pedido", "precio_unitario", Decimal("-1.00"), "precio_no_negativo"),
    ("items_pedido", "descuento", Decimal("-1.00"), "descuento_no_negativo"),
    ("pagos", "monto", Decimal("-5.00"), "monto_no_negativo"),
    ("pagos", "metodo_pago", "bitcoin", "metodo_valido"),
    ("pagos", "estado", "anulado", "estado_valido"),
    ("tickets", "prioridad", "urgente", "prioridad_valida"),
    ("tickets", "estado", "reabierto", "estado_valido"),
    ("tickets", "fecha_cierre", datetime(2026, 9, 30), "cierre_posterior_a_apertura"),
]


@pytest.mark.parametrize("entidad, campo, valor, regla", CASOS_DESCARTAR)
def test_dato_invalido_se_descarta(spark, entidad, campo, valor, regla):
    assert incumplidas(spark, entidad, "descartar", **{campo: valor}) == {regla}


# Valores NULOS en campos con dominio o rango: en SQL dan NULL (no FALSE) y antes de
# la versión 1.1 de reglas_calidad.py quedaban en un limbo entre el pipeline y la cuarentena.
CASOS_NULOS = [
    ("clientes", "segmento", "segmento_valido"),
    ("clientes", "codigo_pais", "pais_valido"),
    ("productos", "precio", "precio_no_negativo"),
    ("productos", "estado", "estado_valido"),
    ("pedidos", "monto_total", "monto_total_no_negativo"),
    ("pedidos", "canal", "canal_valido"),
    ("items_pedido", "cantidad", "cantidad_positiva"),
    ("pagos", "metodo_pago", "metodo_valido"),
    ("tickets", "prioridad", "prioridad_valida"),
]


@pytest.mark.parametrize("entidad, campo, regla", CASOS_NULOS)
def test_nulo_en_dominio_se_descarta(spark, entidad, campo, regla):
    assert incumplidas(spark, entidad, "descartar", **{campo: None}) == {regla}


@pytest.mark.parametrize("entidad, campo, regla", CASOS_NULOS)
def test_regla_sin_coalesce_daria_null(spark, entidad, campo, regla):
    """Documenta el problema que corrige la versión 1.1: la expresión cruda da NULL, no FALSE."""
    df = df_de(spark, entidad, [fila(entidad, **{campo: None})])
    assert evaluar_reglas(df, {regla: rc.REGLAS[entidad]["descartar"][regla]})[0][regla] is None


@pytest.mark.parametrize("entidad", list(VALIDAS))
@pytest.mark.parametrize("accion", ACCIONES)
def test_ninguna_regla_devuelve_null(spark, entidad, accion):
    """Con todos los campos de negocio nulos, cada regla responde TRUE o FALSE, nunca NULL."""
    nulos = {c: None for c in VALIDAS[entidad]}
    resultado = evaluar_reglas(df_de(spark, entidad, [fila(entidad, **nulos)]), rc.reglas(entidad, accion))[0]
    assert None not in resultado.values(), resultado


# --- 4. Un DELETE de la fuente nunca se descarta -----------------------------------

@pytest.mark.parametrize("entidad, campo, valor, regla", CASOS_DESCARTAR)
def test_borrado_cdc_nunca_se_descarta(spark, entidad, campo, valor, regla):
    assert incumplidas(spark, entidad, "descartar", _op="D", **{campo: valor}) == set()


# --- 5. ADVERTIR: dato mejorable → entra y queda la métrica --------------------------

CASOS_ADVERTIR = [
    ("clientes", {"email_normalizado": "ana.quispe@mail"}, "email_valido"),        # sin dominio de nivel superior
    ("clientes", {"email_normalizado": "ana quispe@mail.com"}, "email_valido"),    # espacio interno
    ("clientes", {"email_normalizado": None}, "email_valido"),
    ("clientes", {"telefono": None}, "telefono_informado"),
    ("clientes", {"ciudad": None}, "ciudad_informada"),
    ("productos", {"descripcion": None}, "descripcion_informada"),
    ("pedidos", {"id_cliente": None}, "cliente_informado"),
    ("items_pedido", {"descuento": Decimal("100.00")}, "descuento_menor_al_importe"),
    ("pagos", {"referencia_transaccion": None}, "referencia_informada_si_aprobado"),
    ("tickets", {"cuerpo": None}, "cuerpo_informado"),
]


@pytest.mark.parametrize("entidad, cambios, regla", CASOS_ADVERTIR)
def test_dato_mejorable_advierte_pero_no_descarta(spark, entidad, cambios, regla):
    assert incumplidas(spark, entidad, "advertir", **cambios) == {regla}
    assert incumplidas(spark, entidad, "descartar", **cambios) == set()


@pytest.mark.parametrize("email", ["luis@mail.com", "ana.quispe@mail.com", "sss@gmail.com", "x@andina.pe"])
def test_email_valido_con_letra_s(spark, email):
    """Regresión: la regex iba sin escapar dentro del SQL y Spark convertía \\s en 's',
    así que todo email con la letra «s» quedaba marcado como inválido."""
    assert incumplidas(spark, "clientes", "advertir", email_normalizado=email) == set()


def test_regex_de_email_llega_intacta_a_spark(spark):
    expr = rc.REGLAS["clientes"]["advertir"]["email_valido"]
    literal = expr.split("RLIKE ", 1)[1]
    assert spark.sql(f"SELECT {literal} AS p").collect()[0].p == rc.REGEX_EMAIL


def test_pago_no_aprobado_sin_referencia_es_normal(spark):
    assert incumplidas(spark, "pagos", "advertir", estado="pendiente", referencia_transaccion=None) == set()


def test_ticket_abierto_sin_fecha_cierre_es_valido(spark):
    assert incumplidas(spark, "tickets", "descartar", estado="abierto", fecha_cierre=None) == set()


# --- 6. Los dominios de silver son los mismos CHECK de la fuente ---------------------

def _check_in(columna: str) -> set:
    """Valores del CHECK (<columna> IN (...)) de fuentes/01_ddl_tablas.sql."""
    m = re.search(rf"CHECK \({columna} IN \(([^)]*)\)\)", DDL.read_text(encoding="utf-8"))
    assert m, f"no encontré el CHECK de {columna}"
    return set(re.findall(r"'([^']+)'", m.group(1)))


@pytest.mark.parametrize("columna, dominio", [
    ("CountryCode", rc.PAISES), ("Segment", rc.SEGMENTOS), ("Channel", rc.CANALES),
    ("PaymentMethod", rc.METODOS_PAGO), ("Priority", rc.PRIORIDADES_TICKET)])
def test_dominio_igual_al_check_de_la_fuente(columna, dominio):
    assert set(dominio) == _check_in(columna)


def test_estados_iguales_a_los_check_de_la_fuente():
    ddl = DDL.read_text(encoding="utf-8")
    estados = [set(re.findall(r"'([^']+)'", m)) for m in re.findall(r"CHECK \(Status IN \(([^)]*)\)\)", ddl)]
    assert estados == [set(rc.ESTADOS_PRODUCTO), set(rc.ESTADOS_PEDIDO), set(rc.ESTADOS_PAGO), set(rc.ESTADOS_TICKET)]
