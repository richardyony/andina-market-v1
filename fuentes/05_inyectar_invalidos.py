#!/usr/bin/env python3
"""
===============================================================================
 Archivo      : 05_inyectar_invalidos.py
 Proyecto     : Andina Market · Reto técnico de Ingeniería de Datos
 Capa         : Fuente (OLTP Azure SQL) · prueba de calidad de punta a punta
 Propósito    : Simular una falla de calidad en la fuente (un CHECK desactivado o una
                carga masiva sin validar) para comprobar que silver aparta los datos
                malos en silver.cuarentena con la regla que rompieron.

 Qué hace (en UNA transacción: si algo falla, no queda nada a medias)
   1. Desactiva solo los CHECK necesarios (ALTER TABLE ... NOCHECK CONSTRAINT).
   2. Inserta 7 filas inválidas, una por regla de descarte de silver.
   3. Reactiva los CHECK con WITH NOCHECK: vuelven a bloquear datos nuevos, sin
      revalidar las 7 filas (los CHECK quedan "no confiables", is_not_trusted = 1).

 Uso          : .\fuentes\inyectar_invalidos.ps1  (pide la clave sin mostrarla)
 Después      : correr job_andina_diario y revisar andina_dev.silver.cuarentena.
 Salidas      : fuentes/logs/invalidos_<fecha>.json con el id de cada fila y la regla esperada
 Solo para dev: en producción nunca se desactiva un CHECK para cargar datos.
 Autoría      : Richard Huamán · con asistencia de IA (Claude)
 Versión      : 1.0 · 2026-10-16
===============================================================================
"""

import importlib.util
import json
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent

# Reutiliza la conexión del generador (reintentos mientras la base serverless despierta).
_spec = importlib.util.spec_from_file_location("generador", AQUI / "02_generar_datos_andina.py")
generador = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generador)

# CHECK que se desactivan durante la carga: (tabla, constraint)
CHECKS = [
    ("dbo.Customers", "CK_Customers_Segment"), ("dbo.Customers", "CK_Customers_Country"),
    ("dbo.Products", "CK_Products_Price"), ("dbo.Products", "CK_Products_Status"),
    ("dbo.Orders", "CK_Orders_Channel"), ("dbo.Payments", "CK_Payments_Method"),
    ("dbo.SupportTickets", "CK_Tickets_Priority"),
]

# (tabla de silver, regla esperada, sentencia INSERT ... OUTPUT con el id nuevo)
# {cliente} y {pedido} son ids existentes, para no romper las claves foráneas.
FILAS = [
    ("clientes", "segmento_valido",
     "INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, CountryCode, Segment, SignupDate) "
     "OUTPUT INSERTED.CustomerID VALUES (N'Prueba', N'Segmento', N'prueba.segmento@mail.com', NULL, N'Lima', "
     "'PE', 'premium', CAST(SYSUTCDATETIME() AS DATE))"),
    ("clientes", "pais_valido",
     "INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, CountryCode, Segment, SignupDate) "
     "OUTPUT INSERTED.CustomerID VALUES (N'Prueba', N'Pais', N'prueba.pais@mail.com', NULL, N'Buenos Aires', "
     "'AR', 'nuevo', CAST(SYSUTCDATETIME() AS DATE))"),
    ("productos", "precio_no_negativo",
     "INSERT INTO dbo.Products (SKU, ProductName, Category, Price, Description, Status) "
     "OUTPUT INSERTED.ProductID VALUES ('INV-PRECIO-{sufijo}', N'Producto con precio negativo', N'Prueba', -5.00, "
     "N'Fila inválida inyectada a propósito', 'activo')"),
    ("productos", "estado_valido",
     "INSERT INTO dbo.Products (SKU, ProductName, Category, Price, Description, Status) "
     "OUTPUT INSERTED.ProductID VALUES ('INV-ESTADO-{sufijo}', N'Producto con estado inválido', N'Prueba', 10.00, "
     "N'Fila inválida inyectada a propósito', 'en_oferta')"),
    ("pedidos", "canal_valido",
     "INSERT INTO dbo.Orders (CustomerID, OrderDate, Channel, Status, TotalAmount) "
     "OUTPUT INSERTED.OrderID VALUES ({cliente}, SYSUTCDATETIME(), 'telefono', 'pendiente', 25.00)"),
    ("pagos", "metodo_valido",
     "INSERT INTO dbo.Payments (OrderID, PaymentMethod, Amount, Status, PaymentDate, TransactionRef) "
     "OUTPUT INSERTED.PaymentID VALUES ({pedido}, 'bitcoin', 25.00, 'pendiente', SYSUTCDATETIME(), NULL)"),
    ("tickets", "prioridad_valida",
     "INSERT INTO dbo.SupportTickets (CustomerID, OrderID, Subject, Body, Priority, Status, OpenedAt) "
     "OUTPUT INSERTED.TicketID VALUES ({cliente}, NULL, N'Prioridad inválida', N'Fila inválida inyectada a "
     "propósito', 'urgente', 'abierto', SYSUTCDATETIME())"),
]


def main():
    conn = generador.conectar()
    cur = conn.cursor()
    if not generador.cdc_habilitado(cur):
        raise SystemExit("CDC no está habilitado: los cambios no llegarían a bronze.")
    cur.execute("SELECT TOP 1 CustomerID FROM dbo.Customers ORDER BY CustomerID")
    cliente = cur.fetchone()[0]
    cur.execute("SELECT TOP 1 OrderID FROM dbo.Orders ORDER BY OrderID")
    pedido = cur.fetchone()[0]
    sufijo = datetime.now().strftime("%H%M%S")

    insertadas = []
    try:
        for tabla, ck in CHECKS:
            cur.execute(f"ALTER TABLE {tabla} NOCHECK CONSTRAINT {ck}")
        for entidad, regla, sql in FILAS:
            cur.execute(sql.format(cliente=cliente, pedido=pedido, sufijo=sufijo))
            nuevo_id = cur.fetchone()[0]
            insertadas.append({"tabla_silver": entidad, "id": nuevo_id, "regla_esperada": regla})
            generador.log(f"{entidad:10s} id={nuevo_id:<6} → debe ir a cuarentena por {regla}")
        for tabla, ck in CHECKS:
            cur.execute(f"ALTER TABLE {tabla} WITH NOCHECK CHECK CONSTRAINT {ck}")
        conn.commit()
    except Exception:
        conn.rollback()            # también revierte los ALTER TABLE: los CHECK quedan como estaban
        raise
    finally:
        conn.close()

    resumen = {"ejecutado": datetime.now().isoformat(timespec="seconds"), "filas": insertadas,
               "checks_reactivados_sin_revalidar": [ck for _, ck in CHECKS]}
    carpeta = AQUI / "logs"
    carpeta.mkdir(exist_ok=True)
    archivo = carpeta / f"invalidos_{datetime.now():%Y%m%d_%H%M%S}.json"
    archivo.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
    generador.log(f"{len(insertadas)} filas inválidas confirmadas · log: {archivo.name}")
    generador.log("Siguiente paso: correr job_andina_diario y revisar andina_dev.silver.cuarentena")


if __name__ == "__main__":
    main()
