/* =====================================================================
   Andina Market · OLTP de origen · 05_inyectar_invalidos.sql
   Ejecutar:  como sqladmin_andina (ALTER TABLE exige permiso sobre las
              tablas), en el Query editor del portal, SSMS o Azure Data
              Studio. Solo en dev: en producción nunca se desactiva un
              CHECK para cargar datos.

   Propósito: simular una falla de calidad en la fuente (un CHECK
   desactivado o una carga masiva sin validar) para comprobar que silver
   aparta los datos malos en silver.cuarentena con la regla que rompieron.
   Es la versión SQL de 05_inyectar_invalidos.py: mismas 7 filas.

   Qué hace (en UNA transacción: si algo falla, no queda nada a medias)
   1. Desactiva solo los CHECK necesarios (NOCHECK CONSTRAINT).
   2. Inserta 7 filas inválidas, una por regla de descarte de silver.
   3. Reactiva los CHECK con WITH NOCHECK: vuelven a bloquear datos
      nuevos sin revalidar las 7 filas (quedan is_not_trusted = 1).
   4. Muestra el id de cada fila y la regla que debe saltar en silver.

   Después: correr job_andina_diario y revisar andina_dev.silver.cuarentena.
   Se puede correr más de una vez (el SKU lleva la hora para no chocar
   con UQ_Products_SKU).
   ===================================================================== */

SET NOCOUNT ON;
SET XACT_ABORT ON;      -- cualquier error revierte toda la transacción

/* 0. Sin CDC los cambios no llegarían a bronze ------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    THROW 50001, 'CDC no está habilitado: los cambios no llegarían a bronze.', 1;

/* Ids existentes, para no romper las claves foráneas */
DECLARE @cliente INT = (SELECT TOP 1 CustomerID FROM dbo.Customers ORDER BY CustomerID);
DECLARE @pedido  INT = (SELECT TOP 1 OrderID    FROM dbo.Orders    ORDER BY OrderID);
DECLARE @sufijo  VARCHAR(6) = REPLACE(CONVERT(VARCHAR(8), SYSUTCDATETIME(), 108), ':', '');

DECLARE @insertadas TABLE (tabla_silver VARCHAR(20), id INT, regla_esperada VARCHAR(40));

BEGIN TRANSACTION;

/* 1. Desactivar los CHECK ---------------------------------------------- */
ALTER TABLE dbo.Customers      NOCHECK CONSTRAINT CK_Customers_Segment;
ALTER TABLE dbo.Customers      NOCHECK CONSTRAINT CK_Customers_Country;
ALTER TABLE dbo.Products       NOCHECK CONSTRAINT CK_Products_Price;
ALTER TABLE dbo.Products       NOCHECK CONSTRAINT CK_Products_Status;
ALTER TABLE dbo.Orders         NOCHECK CONSTRAINT CK_Orders_Channel;
ALTER TABLE dbo.Payments       NOCHECK CONSTRAINT CK_Payments_Method;
ALTER TABLE dbo.SupportTickets NOCHECK CONSTRAINT CK_Tickets_Priority;

/* 2. Insertar una fila inválida por regla ------------------------------
   OUTPUT INSERTED.<id> guarda el id nuevo para el resumen final.       */
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, CountryCode, Segment, SignupDate)
OUTPUT INSERTED.CustomerID, 'clientes', 'segmento_valido' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES (N'Prueba', N'Segmento', N'prueba.segmento@mail.com', NULL, N'Lima', 'PE', 'premium', CAST(SYSUTCDATETIME() AS DATE));

INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, CountryCode, Segment, SignupDate)
OUTPUT INSERTED.CustomerID, 'clientes', 'pais_valido' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES (N'Prueba', N'Pais', N'prueba.pais@mail.com', NULL, N'Buenos Aires', 'AR', 'nuevo', CAST(SYSUTCDATETIME() AS DATE));

INSERT INTO dbo.Products (SKU, ProductName, Category, Price, Description, Status)
OUTPUT INSERTED.ProductID, 'productos', 'precio_no_negativo' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES ('INV-PRECIO-' + @sufijo, N'Producto con precio negativo', N'Prueba', -5.00, N'Fila inválida inyectada a propósito', 'activo');

INSERT INTO dbo.Products (SKU, ProductName, Category, Price, Description, Status)
OUTPUT INSERTED.ProductID, 'productos', 'estado_valido' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES ('INV-ESTADO-' + @sufijo, N'Producto con estado inválido', N'Prueba', 10.00, N'Fila inválida inyectada a propósito', 'en_oferta');

INSERT INTO dbo.Orders (CustomerID, OrderDate, Channel, Status, TotalAmount)
OUTPUT INSERTED.OrderID, 'pedidos', 'canal_valido' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES (@cliente, SYSUTCDATETIME(), 'telefono', 'pendiente', 25.00);

INSERT INTO dbo.Payments (OrderID, PaymentMethod, Amount, Status, PaymentDate, TransactionRef)
OUTPUT INSERTED.PaymentID, 'pagos', 'metodo_valido' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES (@pedido, 'bitcoin', 25.00, 'pendiente', SYSUTCDATETIME(), NULL);

INSERT INTO dbo.SupportTickets (CustomerID, OrderID, Subject, Body, Priority, Status, OpenedAt)
OUTPUT INSERTED.TicketID, 'tickets', 'prioridad_valida' INTO @insertadas (id, tabla_silver, regla_esperada)
VALUES (@cliente, NULL, N'Prioridad inválida', N'Fila inválida inyectada a propósito', 'urgente', 'abierto', SYSUTCDATETIME());

/* 3. Reactivar los CHECK sin revalidar lo ya cargado ------------------- */
ALTER TABLE dbo.Customers      WITH NOCHECK CHECK CONSTRAINT CK_Customers_Segment;
ALTER TABLE dbo.Customers      WITH NOCHECK CHECK CONSTRAINT CK_Customers_Country;
ALTER TABLE dbo.Products       WITH NOCHECK CHECK CONSTRAINT CK_Products_Price;
ALTER TABLE dbo.Products       WITH NOCHECK CHECK CONSTRAINT CK_Products_Status;
ALTER TABLE dbo.Orders         WITH NOCHECK CHECK CONSTRAINT CK_Orders_Channel;
ALTER TABLE dbo.Payments       WITH NOCHECK CHECK CONSTRAINT CK_Payments_Method;
ALTER TABLE dbo.SupportTickets WITH NOCHECK CHECK CONSTRAINT CK_Tickets_Priority;

COMMIT TRANSACTION;

/* 4. Resumen: guardar este resultado como evidencia -------------------- */
SELECT tabla_silver, id, regla_esperada,
       CONCAT('debe ir a cuarentena por ', regla_esperada) AS esperado
FROM @insertadas
ORDER BY tabla_silver, id;

/* Comprobación: los 7 CHECK quedan activos (is_disabled = 0) pero no
   confiables (is_not_trusted = 1), porque no revalidaron estas filas. */
SELECT OBJECT_NAME(parent_object_id) AS tabla, name AS constraint_check,
       is_disabled, is_not_trusted
FROM sys.check_constraints
WHERE name IN ('CK_Customers_Segment', 'CK_Customers_Country', 'CK_Products_Price',
               'CK_Products_Status', 'CK_Orders_Channel', 'CK_Payments_Method',
               'CK_Tickets_Priority')
ORDER BY tabla, constraint_check;
