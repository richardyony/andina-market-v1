# Databricks notebook source
# Verificar que Spark está activo y ver la versión
print(f"Spark version: {spark.version}")
print(f"Python version: {spark.sparkContext.pythonVer}")



# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date

# Crear dataset de ventas
data = [
    Row(id_venta=1, producto="Laptop", categoria="Electrónica", cantidad=2, precio_unitario=1200.00, fecha_venta=date(2024, 1, 15), vendedor="Carlos López", region="Lima"),
    Row(id_venta=2, producto="Smartphone", categoria="Electrónica", cantidad=5, precio_unitario=800.00, fecha_venta=date(2024, 1, 20), vendedor="María García", region="Arequipa"),
    Row(id_venta=3, producto="Escritorio", categoria="Muebles", cantidad=1, precio_unitario=350.00, fecha_venta=date(2024, 2, 3), vendedor="Carlos López", region="Lima"),
    Row(id_venta=4, producto="Silla Ergonómica", categoria="Muebles", cantidad=3, precio_unitario=250.00, fecha_venta=date(2024, 2, 10), vendedor="Ana Torres", region="Cusco"),
    Row(id_venta=5, producto="Monitor 27\"", categoria="Electrónica", cantidad=4, precio_unitario=450.00, fecha_venta=date(2024, 2, 14), vendedor="María García", region="Arequipa"),
    Row(id_venta=6, producto="Teclado Mecánico", categoria="Accesorios", cantidad=10, precio_unitario=120.00, fecha_venta=date(2024, 3, 1), vendedor="Pedro Ruiz", region="Trujillo"),
    Row(id_venta=7, producto="Mouse Inalámbrico", categoria="Accesorios", cantidad=15, precio_unitario=45.00, fecha_venta=date(2024, 3, 5), vendedor="Ana Torres", region="Cusco"),
    Row(id_venta=8, producto="Impresora", categoria="Electrónica", cantidad=2, precio_unitario=300.00, fecha_venta=date(2024, 3, 12), vendedor="Carlos López", region="Lima"),
    Row(id_venta=9, producto="Estante", categoria="Muebles", cantidad=2, precio_unitario=180.00, fecha_venta=date(2024, 3, 20), vendedor="Pedro Ruiz", region="Trujillo"),
    Row(id_venta=10, producto="Tablet", categoria="Electrónica", cantidad=3, precio_unitario=600.00, fecha_venta=date(2024, 4, 2), vendedor="María García", region="Arequipa"),
    Row(id_venta=11, producto="Audífonos", categoria="Accesorios", cantidad=8, precio_unitario=85.00, fecha_venta=date(2024, 4, 8), vendedor="Ana Torres", region="Cusco"),
    Row(id_venta=12, producto="Webcam HD", categoria="Accesorios", cantidad=6, precio_unitario=95.00, fecha_venta=date(2024, 4, 15), vendedor="Carlos López", region="Lima"),
    Row(id_venta=13, producto="Sofá", categoria="Muebles", cantidad=1, precio_unitario=900.00, fecha_venta=date(2024, 5, 1), vendedor="Pedro Ruiz", region="Trujillo"),
    Row(id_venta=14, producto="Laptop Gamer", categoria="Electrónica", cantidad=1, precio_unitario=2500.00, fecha_venta=date(2024, 5, 10), vendedor="María García", region="Arequipa"),
    Row(id_venta=15, producto="Cable HDMI", categoria="Accesorios", cantidad=20, precio_unitario=15.00, fecha_venta=date(2024, 5, 18), vendedor="Ana Torres", region="Cusco"),
]

df_ventas = spark.createDataFrame(data)

# Agregar columna de total por venta
from pyspark.sql.functions import col, round as spark_round
df_ventas = df_ventas.withColumn("total_venta", spark_round(col("cantidad") * col("precio_unitario"), 2))

# Escribir como Delta Table
df_ventas.write.format("delta").mode("overwrite").saveAsTable("andina_dev.default.tabla_test")

print("✅ Tabla 'andina_dev.default.tabla_test' creada exitosamente!")
print(f"Total de registros: {df_ventas.count()}")
display(df_ventas)