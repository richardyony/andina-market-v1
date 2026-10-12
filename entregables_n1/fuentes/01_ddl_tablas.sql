/* =====================================================================
   Andina Market · OLTP de origen · 01_ddl_tablas.sql
   Base:      sqldb-andinamarket-cus-001 (Azure SQL, oferta gratuita)
   Ejecutar:  como sqladmin_andina (o el admin Entra), sobre la base vacía
   Orden:     01_ddl_tablas.sql → 02 generador Python (carga) → 03_habilitar_cdc.sql

   Decisiones:
   - Nombres en inglés y PascalCase (convención típica de SQL Server).
     Silver los renombra a snake_case en español (id_cliente, fecha_pedido...).
   - Fechas de NEGOCIO (OrderDate, PaymentDate...) separadas de fechas de
     AUDITORÍA (CreatedAt, UpdatedAt). Todas en UTC, DATETIME2(3).
   - Dinero en DECIMAL(12,2), nunca FLOAT. Supuesto: montos en USD.
   - FKs reales: un OLTP sano no admite huérfanos. Los huérfanos que Silver
     debe manejar aparecen por el orden de llegada en la ingesta.
   - Email SIN restricción UNIQUE: el sistema "legacy" permite clientes
     duplicados (caso borde para Silver). Ojo: la collation por defecto es
     case-insensitive e ignora espacios finales, así que un UNIQUE bloquearía
     'Ana@x.com' vs 'ana@x.com '.
   - Sin columnas calculadas: CDC no captura sus valores.
   - Sin GO: el script corre igual en el Query editor del portal, en
     VS Code (extensión mssql) o en SSMS. Los triggers se crean con EXEC.
   ===================================================================== */

SET NOCOUNT ON;

/* ---------------------------------------------------------------------
   1. Customers · dimensión base
   --------------------------------------------------------------------- */
CREATE TABLE dbo.Customers (
    CustomerID     INT IDENTITY(1,1) NOT NULL,
    FirstName      NVARCHAR(80)      NOT NULL,
    LastName       NVARCHAR(80)      NOT NULL,
    Email          NVARCHAR(160)     NOT NULL,
    Phone          VARCHAR(20)       NULL,
    City           NVARCHAR(80)      NULL,
    CountryCode    CHAR(2)           NOT NULL,
    Segment        VARCHAR(15)       NOT NULL,
    SignupDate     DATE              NOT NULL,          -- fecha de alta (negocio)
    IsActive       BIT               NOT NULL CONSTRAINT DF_Customers_IsActive DEFAULT (1),
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Customers_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Customers_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Customers PRIMARY KEY CLUSTERED (CustomerID),
    CONSTRAINT CK_Customers_Country CHECK (CountryCode IN ('PE','CO','EC','BO','CL')),
    CONSTRAINT CK_Customers_Segment CHECK (Segment IN ('nuevo','regular','frecuente','vip'))
);

/* ---------------------------------------------------------------------
   2. Products · catálogo (Description alimenta el nivel RAG)
   --------------------------------------------------------------------- */
CREATE TABLE dbo.Products (
    ProductID      INT IDENTITY(1,1) NOT NULL,
    SKU            VARCHAR(20)       NOT NULL,
    ProductName    NVARCHAR(150)     NOT NULL,
    Category       NVARCHAR(60)      NOT NULL,
    Price          DECIMAL(12,2)     NOT NULL,
    Description    NVARCHAR(MAX)     NULL,
    Status         VARCHAR(15)       NOT NULL CONSTRAINT DF_Products_Status DEFAULT ('activo'),
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Products_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Products_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Products PRIMARY KEY CLUSTERED (ProductID),
    CONSTRAINT UQ_Products_SKU UNIQUE (SKU),
    CONSTRAINT CK_Products_Price  CHECK (Price >= 0),
    CONSTRAINT CK_Products_Status CHECK (Status IN ('activo','agotado','descontinuado'))
);

/* ---------------------------------------------------------------------
   3. Orders · hecho transaccional principal
   --------------------------------------------------------------------- */
CREATE TABLE dbo.Orders (
    OrderID        INT IDENTITY(1,1) NOT NULL,
    CustomerID     INT               NOT NULL,
    OrderDate      DATETIME2(3)      NOT NULL,          -- fecha del pedido (negocio)
    Channel        VARCHAR(10)       NOT NULL,
    Status         VARCHAR(15)       NOT NULL CONSTRAINT DF_Orders_Status DEFAULT ('pendiente'),
    TotalAmount    DECIMAL(12,2)     NOT NULL,
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Orders_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Orders_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED (OrderID),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers (CustomerID),
    CONSTRAINT CK_Orders_Channel CHECK (Channel IN ('web','app','tienda')),
    CONSTRAINT CK_Orders_Status  CHECK (Status IN ('pendiente','pagado','enviado','entregado','cancelado','devuelto')),
    CONSTRAINT CK_Orders_Total   CHECK (TotalAmount >= 0)
);

/* ---------------------------------------------------------------------
   4. OrderItems · detalle del pedido
   UnitPrice = precio AL MOMENTO de la compra (no el precio actual).
   --------------------------------------------------------------------- */
CREATE TABLE dbo.OrderItems (
    OrderItemID    INT IDENTITY(1,1) NOT NULL,
    OrderID        INT               NOT NULL,
    ProductID      INT               NOT NULL,
    Quantity       INT               NOT NULL,
    UnitPrice      DECIMAL(12,2)     NOT NULL,
    DiscountAmount DECIMAL(12,2)     NOT NULL CONSTRAINT DF_OrderItems_Discount DEFAULT (0),
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_OrderItems_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_OrderItems_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_OrderItems PRIMARY KEY CLUSTERED (OrderItemID),
    CONSTRAINT FK_OrderItems_Orders   FOREIGN KEY (OrderID)   REFERENCES dbo.Orders (OrderID),
    CONSTRAINT FK_OrderItems_Products FOREIGN KEY (ProductID) REFERENCES dbo.Products (ProductID),
    CONSTRAINT UQ_OrderItems_Order_Product UNIQUE (OrderID, ProductID),
    CONSTRAINT CK_OrderItems_Quantity CHECK (Quantity > 0),
    CONSTRAINT CK_OrderItems_Price    CHECK (UnitPrice >= 0),
    CONSTRAINT CK_OrderItems_Discount CHECK (DiscountAmount >= 0)
);

/* ---------------------------------------------------------------------
   5. Payments · se actualiza después de creado el pedido (estado)
   Un pedido puede tener varios intentos de pago (rechazado → aprobado).
   --------------------------------------------------------------------- */
CREATE TABLE dbo.Payments (
    PaymentID      INT IDENTITY(1,1) NOT NULL,
    OrderID        INT               NOT NULL,
    PaymentMethod  VARCHAR(20)       NOT NULL,
    Amount         DECIMAL(12,2)     NOT NULL,
    Status         VARCHAR(15)       NOT NULL CONSTRAINT DF_Payments_Status DEFAULT ('pendiente'),
    PaymentDate    DATETIME2(3)      NOT NULL,          -- fecha del intento de pago (negocio)
    TransactionRef VARCHAR(40)       NULL,
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Payments_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Payments_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_Payments PRIMARY KEY CLUSTERED (PaymentID),
    CONSTRAINT FK_Payments_Orders FOREIGN KEY (OrderID) REFERENCES dbo.Orders (OrderID),
    CONSTRAINT CK_Payments_Method CHECK (PaymentMethod IN ('tarjeta_credito','tarjeta_debito','yape','plin','transferencia','contraentrega')),
    CONSTRAINT CK_Payments_Status CHECK (Status IN ('pendiente','aprobado','rechazado','reembolsado')),
    CONSTRAINT CK_Payments_Amount CHECK (Amount >= 0)
);

/* ---------------------------------------------------------------------
   6. SupportTickets · texto no estructurado (features + RAG)
   --------------------------------------------------------------------- */
CREATE TABLE dbo.SupportTickets (
    TicketID       INT IDENTITY(1,1) NOT NULL,
    CustomerID     INT               NOT NULL,
    OrderID        INT               NULL,              -- no todo ticket es de un pedido
    Subject        NVARCHAR(200)     NOT NULL,
    Body           NVARCHAR(MAX)     NULL,
    Priority       VARCHAR(10)       NOT NULL CONSTRAINT DF_Tickets_Priority DEFAULT ('media'),
    Status         VARCHAR(15)       NOT NULL CONSTRAINT DF_Tickets_Status DEFAULT ('abierto'),
    OpenedAt       DATETIME2(3)      NOT NULL,          -- negocio
    ClosedAt       DATETIME2(3)      NULL,              -- negocio
    CreatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Tickets_CreatedAt DEFAULT (SYSUTCDATETIME()),
    UpdatedAt      DATETIME2(3)      NOT NULL CONSTRAINT DF_Tickets_UpdatedAt DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_SupportTickets PRIMARY KEY CLUSTERED (TicketID),
    CONSTRAINT FK_Tickets_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers (CustomerID),
    CONSTRAINT FK_Tickets_Orders    FOREIGN KEY (OrderID)    REFERENCES dbo.Orders (OrderID),
    CONSTRAINT CK_Tickets_Priority CHECK (Priority IN ('baja','media','alta')),
    CONSTRAINT CK_Tickets_Status   CHECK (Status IN ('abierto','en_proceso','resuelto','cerrado')),
    CONSTRAINT CK_Tickets_Dates    CHECK (ClosedAt IS NULL OR ClosedAt >= OpenedAt)
);

/* ---------------------------------------------------------------------
   7. Índices
   - FKs: SQL Server no las indexa solo; sin índice los joins y los
     DELETE en la tabla padre escanean la tabla hija.
   - UpdatedAt: plan B de recarga si se vence la retención de CDC y
     consultas de reconciliación.
   --------------------------------------------------------------------- */
CREATE INDEX IX_Orders_CustomerID       ON dbo.Orders (CustomerID);
CREATE INDEX IX_Orders_OrderDate        ON dbo.Orders (OrderDate);
CREATE INDEX IX_OrderItems_OrderID      ON dbo.OrderItems (OrderID);
CREATE INDEX IX_OrderItems_ProductID    ON dbo.OrderItems (ProductID);
CREATE INDEX IX_Payments_OrderID        ON dbo.Payments (OrderID);
CREATE INDEX IX_Tickets_CustomerID      ON dbo.SupportTickets (CustomerID);
CREATE INDEX IX_Tickets_OrderID         ON dbo.SupportTickets (OrderID);
CREATE INDEX IX_Customers_Email         ON dbo.Customers (Email);   -- búsqueda, NO único

CREATE INDEX IX_Customers_UpdatedAt      ON dbo.Customers (UpdatedAt);
CREATE INDEX IX_Products_UpdatedAt       ON dbo.Products (UpdatedAt);
CREATE INDEX IX_Orders_UpdatedAt         ON dbo.Orders (UpdatedAt);
CREATE INDEX IX_OrderItems_UpdatedAt     ON dbo.OrderItems (UpdatedAt);
CREATE INDEX IX_Payments_UpdatedAt       ON dbo.Payments (UpdatedAt);
CREATE INDEX IX_SupportTickets_UpdatedAt ON dbo.SupportTickets (UpdatedAt);

/* ---------------------------------------------------------------------
   8. Triggers de auditoría (UpdatedAt)
   - Solo actúan si el UPDATE NO trae UpdatedAt. El generador (la "app")
     siempre lo envía, así cada cambio produce UN solo evento en CDC.
     Si alguien actualiza a mano sin UpdatedAt, el trigger lo completa
     (genera un 2.º evento en CDC que Silver descarta: solo cambió UpdatedAt).
   - CreatedAt nunca se modifica: se protege restaurando el valor original.
   --------------------------------------------------------------------- */
DECLARE @t TABLE (tbl SYSNAME, pk SYSNAME);
INSERT INTO @t VALUES
 ('Customers','CustomerID'), ('Products','ProductID'), ('Orders','OrderID'),
 ('OrderItems','OrderItemID'), ('Payments','PaymentID'), ('SupportTickets','TicketID');

DECLARE @tbl SYSNAME, @pk SYSNAME, @sql NVARCHAR(MAX);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT tbl, pk FROM @t;
OPEN c;
FETCH NEXT FROM c INTO @tbl, @pk;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
CREATE TRIGGER dbo.TR_' + @tbl + N'_Audit ON dbo.' + QUOTENAME(@tbl) + N'
AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    IF UPDATE(UpdatedAt) RETURN;          -- la app ya lo envió
    UPDATE t
       SET UpdatedAt = SYSUTCDATETIME(),
           CreatedAt = d.CreatedAt        -- CreatedAt es inmutable
      FROM dbo.' + QUOTENAME(@tbl) + N' t
      JOIN inserted i ON t.' + QUOTENAME(@pk) + N' = i.' + QUOTENAME(@pk) + N'
      JOIN deleted  d ON d.' + QUOTENAME(@pk) + N' = i.' + QUOTENAME(@pk) + N';
END;';
    EXEC sys.sp_executesql @sql;
    FETCH NEXT FROM c INTO @tbl, @pk;
END
CLOSE c; DEALLOCATE c;

/* ---------------------------------------------------------------------
   9. Rol de lectura de CDC (se usará en 03_habilitar_cdc.sql)
   dbx_reader_dev / dbx_reader_prd ya existen con db_datareader.
   --------------------------------------------------------------------- */
IF DATABASE_PRINCIPAL_ID('cdc_reader') IS NULL
    CREATE ROLE cdc_reader;
ALTER ROLE cdc_reader ADD MEMBER dbx_reader_dev;
ALTER ROLE cdc_reader ADD MEMBER dbx_reader_prd;

/* ---------------------------------------------------------------------
   10. Verificación
   --------------------------------------------------------------------- */
SELECT t.name AS tabla,
       (SELECT COUNT(*) FROM sys.foreign_keys f WHERE f.parent_object_id = t.object_id) AS fks,
       (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0) AS indices,
       (SELECT COUNT(*) FROM sys.triggers tr WHERE tr.parent_id = t.object_id) AS triggers
FROM sys.tables t
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY t.name;
-- Esperado: 6 tablas, 1 trigger cada una.