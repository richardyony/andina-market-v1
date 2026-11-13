/* =====================================================================
   Andina Market · infraestructura · 04_unity_catalog_objetos.sql
   Objetos de Unity Catalog que se crean UNA vez por ambiente (dev, stg, prd).
   Ejecutar como administrador del workspace en el SQL Editor (o en un
   notebook con %sql), reemplazando los valores de cada ambiente:

     ambiente   catálogo     storage              Key Vault
     dev        andina_dev   standinadevcus001    kv-andina-dev-cus-001
     stg        andina_stg   standinadevcus001    kv-andina-dev-cus-001   (comparte storage y Key Vault de dev)
     prd        andina_prd   standinaprdcus001    kv-andina-prd-cus-001

   Prerrequisitos en Azure (portal):
   - Access Connector dbac-andinamarket-cus-001 con el rol
     "Storage Blob Data Contributor" sobre el storage del ambiente.
   - Contenedor "lakehouse" en el storage (ADLS Gen2, HNS activado).

   Cadena de acceso (sin claves ni secretos):
     catálogo → ubicación externa → credencial → Access Connector (identidad
     administrada) → rol sobre el storage.

   Idempotente: todo usa IF NOT EXISTS u OR REPLACE. El job de bronze además recrea los
   esquemas y Volumes que necesita (src/ingesta/nb_preparar_entorno.ipynb).
   ===================================================================== */

-- 1. Credencial de almacenamiento (una por workspace; la comparten dev y prd).
--    Se crea desde Catalog Explorer → External data → Credentials, indicando el
--    resource ID del Access Connector:
--    /subscriptions/<sub>/resourceGroups/rg-andinamarket-shared-cus-001/providers/
--      Microsoft.Databricks/accessConnectors/dbac-andinamarket-cus-001
--    Nombre: cred_andinamarket

-- 2. Ubicación externa del ambiente (raíz del contenedor lakehouse).
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_andina_dev
  URL 'abfss://lakehouse@standinadevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_andinamarket)
  COMMENT 'Raíz del lakehouse de dev (ADLS Gen2 standinadevcus001).';

-- 3. Catálogo del ambiente, con sus tablas administradas en el storage propio
--    (independiente del ciclo de vida del workspace).
CREATE CATALOG IF NOT EXISTS andina_dev
  MANAGED LOCATION 'abfss://lakehouse@standinadevcus001.dfs.core.windows.net/andina_dev'
  COMMENT 'Andina Market · ambiente dev.';

-- 4. Esquemas por responsabilidad.
CREATE SCHEMA IF NOT EXISTS andina_dev.landing COMMENT 'Archivos tal como llegan de las fuentes, antes de bronze.';
CREATE SCHEMA IF NOT EXISTS andina_dev.control COMMENT 'Metadatos operativos de la ingesta: objetos, watermarks y ejecuciones.';
CREATE SCHEMA IF NOT EXISTS andina_dev.bronze  COMMENT 'Capa bronze: datos crudos tal como llegan de la fuente, append-only, con metadatos de ingesta.';
CREATE SCHEMA IF NOT EXISTS andina_dev.silver  COMMENT 'Capa silver: datos limpios, tipados, deduplicados y con calidad validada.';
CREATE SCHEMA IF NOT EXISTS andina_dev.gold    COMMENT 'Capa gold: modelo dimensional listo para consumo analítico y ML.';

-- 5. Volumes.
--    Externos en landing: los archivos viven en el storage del ambiente.
CREATE EXTERNAL VOLUME IF NOT EXISTS andina_dev.landing.sqlserver
  LOCATION 'abfss://lakehouse@standinadevcus001.dfs.core.windows.net/landing/sqlserver'
  COMMENT 'Parquet del extractor CDC de Azure SQL (una carpeta por tabla y fecha). Retención 90 días.';
CREATE EXTERNAL VOLUME IF NOT EXISTS andina_dev.landing.clickstream
  LOCATION 'abfss://lakehouse@standinadevcus001.dfs.core.windows.net/landing/clickstream'
  COMMENT 'Eventos de la app móvil en .jsonl (muestra del reto). Retención 90 días.';
--    Administrado para el estado de Auto Loader (no se borra: garantiza exactly-once).
CREATE VOLUME IF NOT EXISTS andina_dev.bronze.checkpoints
  COMMENT 'Checkpoints y schemaLocation de Auto Loader. No borrar: garantiza exactly-once.';

-- 6. Permisos (a grupos, nunca a personas). Requiere que el grupo de Entra ID esté
--    sincronizado en la cuenta de Databricks.
--    Ingeniería: todas las capas (necesita el dato real para limpiar y deduplicar).
GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
  ON CATALOG andina_dev TO `grp-andina-data-engineers`;

--    Analítica: solo gold, y con las columnas personales enmascaradas (paso 7).
--    Sin permisos sobre landing, bronze ni silver: ahí está el dato crudo sin máscara.
GRANT USE CATALOG ON CATALOG andina_dev TO `grp-andina-analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA andina_dev.gold TO `grp-andina-analistas`;

--    Sin BROWSE para toda la cuenta: quien no tiene un GRANT no ve ni los nombres
--    de los esquemas y tablas del catálogo.
REVOKE BROWSE ON CATALOG andina_dev FROM `account users`;

-- 7. Gobierno de datos personales: máscaras de columna.
--    Grupos (de cuenta, creados en Settings → Identity and access → Groups):
--      grp-andina-analistas     → consume gold (Consumer + Databricks SQL access)
--      grp-andina-pii-lectores  → ve el dato personal real; no da acceso a nada por sí
--                                 mismo, solo lo consultan las funciones de máscara
--    Las funciones viven en su propio esquema: quien cambia una máscara decide quién
--    ve PII, y eso no debe estar al alcance de quien puede escribir en las capas.
--    Las aplica el pipeline de gold en el esquema de la tabla (MASK en @dlt.table,
--    src/transformacion/gold/dim_clientes.ipynb): quedan versionadas en el bundle.
--    Los analistas no necesitan EXECUTE sobre las funciones.
CREATE SCHEMA IF NOT EXISTS andina_dev.gobierno
  COMMENT 'Funciones de gobierno: máscaras de datos personales';

-- ana.quispe@mail.com → ***@mail.com (se conserva el dominio, útil para analítica)
CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_email(email STRING) RETURNS STRING
COMMENT 'Email completo solo para grp-andina-pii-lectores'
RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN email
            ELSE concat('***@', split_part(email, '@', 2)) END;

-- +51 999 111 222 → ***-222
CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_telefono(tel STRING) RETURNS STRING
COMMENT 'Teléfono completo solo para grp-andina-pii-lectores'
RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN tel
            ELSE concat('***-', right(tel, 3)) END;

-- Quispe → Q***
CREATE OR REPLACE FUNCTION andina_dev.gobierno.mask_nombre(txt STRING) RETURNS STRING
COMMENT 'Nombre completo solo para grp-andina-pii-lectores'
RETURN CASE WHEN is_account_group_member('grp-andina-pii-lectores') THEN txt
            ELSE concat(left(txt, 1), '***') END;

-- Verificación: dentro del grupo salen completos; fuera, enmascarados.
--   SELECT andina_dev.gobierno.mask_email('ana.quispe@mail.com'), andina_dev.gobierno.mask_nombre('Quispe');
--   SHOW GRANTS ON SCHEMA andina_dev.gold;

-- Para prd: repetir los pasos 2 a 7 con loc_andina_prd, andina_prd y standinaprdcus001.

-- Para stg (staging): comparte la ubicación externa loc_andina_dev y el storage de dev
-- (supuesto de costo), pero tiene catálogo propio y su propia carpeta de landing.
-- No se repite el paso 2. Los esquemas y Volumes de landing los crea el job
-- (nb_preparar_entorno con carpeta_landing = stg/landing). Solo hace falta:
CREATE CATALOG IF NOT EXISTS andina_stg
  MANAGED LOCATION 'abfss://lakehouse@standinadevcus001.dfs.core.windows.net/andina_stg'
  COMMENT 'Andina Market · ambiente stg (staging): réplica de prd para probar despliegues.';
-- Luego, repetir los pasos 6 y 7 reemplazando andina_dev por andina_stg (permisos,
-- esquema gobierno y funciones de máscara), y dar acceso al service principal de
-- CI/CD (docs/despliegue_cicd.md, sección 3.1).
