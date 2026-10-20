# Databricks notebook source
# =============================================================================
# nb_test_conexion_cdc · Prueba de conectividad Fuente → Databricks (DEV)
# Propósito : verificar que dbx_reader_dev lee tablas de origen y tablas CDC.
# Escribe   : nada (solo lectura)
# Versión   : 1.0 · 2026-10-11
# =============================================================================
jdbc_url = ("jdbc:sqlserver://sql-andinamarket-cus-001.database.windows.net:1433;"
            "database=sqldb-andinamarket-cus-001;encrypt=true;loginTimeout=60")
opciones = {
    "url": jdbc_url,
    "user": dbutils.secrets.get("kv-andina-dev", "sql-usuario"),
    "password": dbutils.secrets.get("kv-andina-dev", "sql-password"),
}

def consultar(sql: str):
    """Ejecuta una consulta de solo lectura en Azure SQL y devuelve un DataFrame."""
    return spark.read.format("jdbc").options(**opciones).option("query", sql).load()

# COMMAND ----------

# 2. Lectura de tablas de origen (permiso db_datareader)
display(consultar("""
    SELECT 'Customers' AS tabla, COUNT(*) AS filas FROM dbo.Customers UNION ALL
    SELECT 'Orders',   COUNT(*) FROM dbo.Orders   UNION ALL
    SELECT 'Payments', COUNT(*) FROM dbo.Payments
"""))

# COMMAND ----------

# 3. Funciones y tablas de CDC (permiso del rol cdc_reader)
#    LSN = posición en el log de transacciones; será el watermark del extractor
display(consultar("""
    SELECT CONVERT(VARCHAR(30), sys.fn_cdc_get_min_lsn('dbo_Payments'), 1) AS min_lsn_payments,
           CONVERT(VARCHAR(30), sys.fn_cdc_get_max_lsn(), 1)               AS max_lsn_base
"""))

# Tabla de cambios de Payments: hoy debe tener 0 filas (CDC se activó después de la carga)
display(consultar("SELECT COUNT(*) AS cambios_registrados FROM cdc.dbo_Payments_CT"))