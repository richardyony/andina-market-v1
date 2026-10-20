# Databricks notebook source
# MAGIC %md
# MAGIC # nb_validacion_bronze_sqlserver
# MAGIC | Campo      | Valor |
# MAGIC |------------|-------|
# MAGIC | Proyecto   | Andina Market · Reto técnico de Ingeniería de Datos |
# MAGIC | Capa       | Bronze · validación (no productivo, no va en el job) |
# MAGIC | Propósito  | Verificar que la ingesta CDC llegó completa y trazable a Bronze |
# MAGIC | Pruebas    | 1. Filas por corrida (`_id_ejecucion`, `_op`) · 2. Carpetas en landing · 3. Recorrido de estados de pagos |
# MAGIC | Evidencia  | Simulador día 1 (`fuentes/logs/dia_20261012_003146.json`) · corrida `a59b5082` |
# MAGIC | Autoría    | Richard Huamán · con asistencia de IA (Claude) |
# MAGIC | Versión    | 1.0 · 2026-10-12 |

# COMMAND ----------

# MAGIC %md ## 1. Filas por corrida en Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- Diagnóstico · ¿llegaron a Bronze los cambios del día?
# MAGIC -- Agrupa payments_raw por corrida de extracción: si aparece un segundo
# MAGIC -- _id_ejecucion con ~319 filas (I/U) → los datos entraron (caso A).
# MAGIC -- Si solo aparece la carga inicial (R) → Auto Loader no los leyó (caso B).
# MAGIC -- =====================================================================
# MAGIC SELECT _id_ejecucion,
# MAGIC        _op,
# MAGIC        COUNT(*)            AS filas,
# MAGIC        MIN(_ingestion_ts)  AS primera_ingesta,
# MAGIC        MAX(_ingestion_ts)  AS ultima_ingesta
# MAGIC FROM andina_dev.bronze.payments_raw
# MAGIC GROUP BY ALL
# MAGIC ORDER BY primera_ingesta;

# COMMAND ----------

# MAGIC %md ## 2. Carpetas de corridas en landing

# COMMAND ----------

# Diagnóstico · carpetas de corridas en landing para payments
# Espero 2 carpetas id_ejecucion=...: la carga inicial y la de hoy
for f in dbutils.fs.ls("/Volumes/andina_dev/landing/sqlserver/payments/"):
    print(f.path)
    for sub in dbutils.fs.ls(f.path):
        print("   ", sub.name)

# COMMAND ----------

# MAGIC %md ## 3. Recorrido de estados de pagos (CDC)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- Verificación · historial de estados de pagos del simulador (día 1)
# MAGIC -- Esperado:
# MAGIC --   42213 → 3 filas: I pendiente → U aprobado → U reembolsado
# MAGIC --   42178 → U rechazado
# MAGIC --   42247 → reintento de 42178: I pendiente → U aprobado
# MAGIC -- =====================================================================
# MAGIC SELECT *
# MAGIC FROM andina_dev.bronze.payments_raw
# MAGIC WHERE PaymentID IN (42213, 42178, 42247)
# MAGIC ORDER BY PaymentID, _lsn, _seqval;   -- orden real en que ocurrieron los cambios