/* =====================================================================
   Andina Market · OLTP de origen · 03_habilitar_cdc.sql
   Ejecutar:  como sqladmin_andina (CDC exige db_owner),
              DESPUÉS de la carga inicial con el generador (paso 02).

   Por qué después de la carga:
   - La carga inicial no debe llenar las tablas de cambios con ~200 mil
     inserts: la primera ingesta será una FOTO COMPLETA de cada tabla,
     guardando el LSN máximo de ese momento como watermark.
   - Desde ahí, cada corrida lee solo los cambios entre el último LSN
     guardado y el actual.

   Decisión: CDC en las 6 tablas (un solo mecanismo).
   - Un tipo de watermark (LSN), un camino de código en el extractor.
   - Captura inserts, updates (antes/después), deletes y estados
     intermedios en todas las tablas.
   - Costo: más uso del log y de almacenamiento; con este volumen es
     despreciable. En un OLTP grande se activaría solo donde aporta.
   ===================================================================== */

SET NOCOUNT ON;

/* 1. Habilitar CDC en la base ----------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;

/* 2. Habilitar CDC por tabla -------------------------------------------
   @role_name       : solo los miembros de cdc_reader leen los cambios
   @supports_net_changes = 1 : habilita fn_cdc_get_net_changes (requiere PK)
   @capture_instance: nombre explícito; las funciones quedan como
                      cdc.fn_cdc_get_all_changes_dbo_<Tabla>
   ---------------------------------------------------------------------- */
DECLARE @tbl SYSNAME, @ci SYSNAME;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM (VALUES ('Customers'),('Products'),('Orders'),
                             ('OrderItems'),('Payments'),('SupportTickets')) v(name);
OPEN c;
FETCH NEXT FROM c INTO @tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ci = N'dbo_' + @tbl;
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = @tbl AND is_tracked_by_cdc = 1)
        EXEC sys.sp_cdc_enable_table
             @source_schema        = N'dbo',
             @source_name          = @tbl,
             @role_name            = N'cdc_reader',
             @supports_net_changes = 1,
             @capture_instance     = @ci;
    FETCH NEXT FROM c INTO @tbl;
END
CLOSE c; DEALLOCATE c;

/* 3. Permiso para leer las funciones de cambios ------------------------ */
GRANT SELECT ON SCHEMA::cdc TO cdc_reader;

/* 4. Retención: de 3 días (defecto) a 7 días = 10080 minutos -----------
   Debe cubrir el PEOR escenario de caída del pipeline, no su frecuencia.
   Si esta línea da error en tu base, la retención queda en 3 días:
   avísame y ajustamos el chequeo del extractor.
   ---------------------------------------------------------------------- */
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;

/* 5. Verificación ------------------------------------------------------ */
SELECT name AS base, is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();

SELECT t.name AS tabla, t.is_tracked_by_cdc, ct.capture_instance, ct.role_name,
       ct.supports_net_changes
FROM sys.tables t
LEFT JOIN cdc.change_tables ct ON ct.source_object_id = t.object_id
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY t.name;
-- Esperado: 6 filas con is_tracked_by_cdc = 1 y role_name = cdc_reader

EXEC sys.sp_cdc_help_jobs;       -- muestra la retención configurada (minutos)

/* 6. Prueba rápida (opcional): un cambio y su captura ------------------
   El capturador corre cada ~20 s; espera unos segundos antes del SELECT.

UPDATE dbo.Payments SET Status = 'aprobado', UpdatedAt = SYSUTCDATETIME()
WHERE PaymentID = 1;

SELECT __$operation AS op,            -- 1=delete 2=insert 3=antes 4=después
       sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS fecha_cambio,
       PaymentID, Status, UpdatedAt
FROM cdc.fn_cdc_get_all_changes_dbo_Payments(
        sys.fn_cdc_get_min_lsn('dbo_Payments'), sys.fn_cdc_get_max_lsn(), N'all update old');
   ---------------------------------------------------------------------- */