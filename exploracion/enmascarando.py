# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS andina_dev.gobierno
# MAGIC   COMMENT 'Funciones de gobierno: máscaras de datos personales';
# MAGIC
# MAGIC -- ana.quispe@mail.com → ***@mail.com (se conserva el dominio, útil para analítica)
# MAGIC CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_email(email STRING) RETURNS STRING
# MAGIC COMMENT 'Email completo solo para grp-andina-pii-lectores'
# MAGIC RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN email
# MAGIC             ELSE concat('***@', split_part(email, '@', 2)) END;
# MAGIC
# MAGIC -- +51 999 111 222 → ***-222
# MAGIC CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_telefono(tel STRING) RETURNS STRING
# MAGIC COMMENT 'Teléfono completo solo para grp-andina-pii-lectores'
# MAGIC RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN tel
# MAGIC             ELSE concat('***-', right(tel, 3)) END;
# MAGIC
# MAGIC -- Quispe → Q***
# MAGIC CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_nombre(txt STRING) RETURNS STRING
# MAGIC COMMENT 'Nombre completo solo para grp-andina-pii-lectores'
# MAGIC RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN txt
# MAGIC             ELSE concat(left(txt, 1), '***') END;
# MAGIC
# MAGIC -- Prueba: estando en el grupo, deben salir completos
# MAGIC SELECT andina_dev.gobierno.mask_email('ana.quispe@mail.com') AS email,
# MAGIC        andina_dev.gobierno.mask_telefono('+51 999 111 222')  AS telefono,
# MAGIC        andina_dev.gobierno.mask_nombre('Quispe')             AS apellido;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analistas: solo el piso de gold
# MAGIC GRANT USE CATALOG ON CATALOG andina_dev TO `grp-andina-analistas`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA andina_dev.gold TO `grp-andina-analistas`;
# MAGIC
# MAGIC -- Ingeniería: todos los pisos (ya lo tiene, en infra/04_unity_catalog_objetos.sql, línea 64)
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
# MAGIC   ON CATALOG andina_dev TO `grp-andina-data-engineers`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA andina_dev.gold;                    -- quién tiene qué en gold
# MAGIC SHOW GRANTS `grp-andina-analistas` ON CATALOG andina_dev; -- qué tiene este grupo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA andina_dev.gold;
# MAGIC REVOKE BROWSE ON CATALOG andina_dev FROM `account users`;
# MAGIC
# MAGIC /*PROBANDO EL ENMASCARADO*/
# MAGIC
# MAGIC SELECT andina_dev.gobierno.mask_email('ana.quispe@mail.com') AS email,
# MAGIC        andina_dev.gobierno.mask_telefono('+51 999 111 222')  AS telefono,
# MAGIC        andina_dev.gobierno.mask_nombre('Quispe')             AS apellido;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE ALL PRIVILEGES ON CATALOG andina_dev FROM `grp-andina-data-engineers`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_cliente, nombre, apellido, email_normalizado, segmento, ciudad
# MAGIC FROM andina_dev.gold.dim_clientes
# MAGIC WHERE es_vigente
# MAGIC ORDER BY id_cliente
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
# MAGIC   ON CATALOG andina_dev TO `grp-andina-data-engineers`;
# MAGIC
# MAGIC