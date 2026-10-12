/* =====================================================================
   Andina Market · infraestructura · 04_unity_catalog_objetos.sql
   Objetos de Unity Catalog que se crean UNA vez por ambiente (dev, prd).
   Ejecutar como administrador del workspace en el SQL Editor (o en un
   notebook con %sql), reemplazando los valores de cada ambiente:

     ambiente   catálogo     storage              Key Vault
     dev        andina_dev   standinadevcus001    kv-andina-dev-cus-001
     prd        andina_prd   standinaprdcus001    kv-andina-prd-cus-001

   Prerrequisitos en Azure (portal):
   - Access Connector dbac-andinamarket-cus-001 con el rol
     "Storage Blob Data Contributor" sobre el storage del ambiente.
   - Contenedor "lakehouse" en el storage (ADLS Gen2, HNS activado).

   Cadena de acceso (sin claves ni secretos):
     catálogo → ubicación externa → credencial → Access Connector (identidad
     administrada) → rol sobre el storage.

   Idempotente: todo usa IF NOT EXISTS. El job de bronze además recrea los
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
GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, READ VOLUME, WRITE VOLUME
  ON CATALOG andina_dev TO `grp-andina-data-engineers`;

-- Para prd: repetir los pasos 2 a 6 con loc_andina_prd, andina_prd y standinaprdcus001.
