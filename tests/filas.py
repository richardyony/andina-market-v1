"""
===============================================================================
 Archivo      : tests/filas.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Pruebas unitarias de silver (nivel 2)
 Propósito    : Una fila VÁLIDA por entidad, con las columnas que deja la vista
                limpio_<entidad> (nombres de silver + columnas técnicas). Cada prueba
                parte de esta fila y cambia un solo campo para romper una regla.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-14
===============================================================================
"""

from datetime import datetime
from decimal import Decimal

from conftest import crear_df

# Columnas técnicas mínimas: _op (tipo de cambio CDC) y la trazabilidad que usa la cuarentena.
TECNICAS = "_op STRING, _lsn STRING, _id_ejecucion STRING, _ingestion_ts TIMESTAMP"
TECNICAS_VALOR = {"_op": "I", "_lsn": "0x0001", "_id_ejecucion": "test",
                  "_ingestion_ts": datetime(2026, 10, 12, 2, 0)}

ESQUEMAS = {
    "clientes": "id_cliente INT, email_normalizado STRING, telefono STRING, ciudad STRING, "
                "codigo_pais STRING, segmento STRING, " + TECNICAS,
    "productos": "id_producto INT, sku STRING, precio DECIMAL(12,2), descripcion STRING, estado STRING, " + TECNICAS,
    "pedidos": "id_pedido INT, id_cliente INT, monto_total DECIMAL(14,2), canal STRING, estado STRING, " + TECNICAS,
    "items_pedido": "id_item INT, cantidad INT, precio_unitario DECIMAL(12,2), descuento DECIMAL(12,2), " + TECNICAS,
    "pagos": "id_pago INT, monto DECIMAL(14,2), metodo_pago STRING, estado STRING, "
             "referencia_transaccion STRING, " + TECNICAS,
    "tickets": "id_ticket INT, cuerpo STRING, prioridad STRING, estado STRING, "
               "fecha_apertura TIMESTAMP, fecha_cierre TIMESTAMP, " + TECNICAS,
}

CLAVES = {"clientes": "id_cliente", "productos": "id_producto", "pedidos": "id_pedido",
          "items_pedido": "id_item", "pagos": "id_pago", "tickets": "id_ticket"}

VALIDAS = {
    "clientes": {"id_cliente": 1, "email_normalizado": "ana.quispe@mail.com", "telefono": "+51 999 111 222",
                 "ciudad": "Lima", "codigo_pais": "PE", "segmento": "regular"},
    "productos": {"id_producto": 1, "sku": "AND-0001", "precio": Decimal("49.90"),
                  "descripcion": "Polo de algodón", "estado": "activo"},
    "pedidos": {"id_pedido": 1, "id_cliente": 1, "monto_total": Decimal("99.80"), "canal": "web",
                "estado": "pagado"},
    "items_pedido": {"id_item": 1, "cantidad": 2, "precio_unitario": Decimal("49.90"), "descuento": Decimal("0.00")},
    "pagos": {"id_pago": 1, "monto": Decimal("99.80"), "metodo_pago": "yape", "estado": "aprobado",
              "referencia_transaccion": "TX-123"},
    "tickets": {"id_ticket": 1, "cuerpo": "No llegó mi pedido", "prioridad": "alta", "estado": "cerrado",
                "fecha_apertura": datetime(2026, 10, 1, 10, 0), "fecha_cierre": datetime(2026, 10, 2, 9, 0)},
}


def fila(entidad: str, **cambios) -> dict:
    """La fila válida de la entidad con algunos campos cambiados."""
    return {**VALIDAS[entidad], **TECNICAS_VALOR, **cambios}


def df_de(spark, entidad: str, filas: list[dict]):
    """DataFrame de la entidad con su esquema (tipos iguales a los de silver)."""
    columnas = [c.split()[0] for c in ESQUEMAS[entidad].split(", ")]
    return crear_df(spark, [tuple(f[c] for c in columnas) for f in filas], ESQUEMAS[entidad])
