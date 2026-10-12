# Modelo de datos · Nivel 2 (transformación y modelado)

Diseño de las etapas silver y gold. Se implementa con **Lakeflow Declarative Pipelines** en Python, con **un notebook por entidad** ([src/transformacion/silver/](../../src/transformacion/silver/) y [src/transformacion/gold/](../../src/transformacion/gold/)), en dos pipelines (`pl_andina_silver`, `pl_andina_gold`) que ejecuta el mismo job diario después de la ingesta, todo en un solo bundle.

## 1. Etapas y responsabilidad de cada una

| Etapa | Responsabilidad | Qué NO hace | Pregunta que responde |
|---|---|---|---|
| **Landing** | Guardar los archivos tal como llegan | Ni interpreta ni transforma | ¿Qué se extrajo? |
| **Bronze** (nivel 1) | Registro fiel y append-only de cada cambio (`_op` R/I/U/D) con su trazabilidad | No limpia, no deduplica | ¿Qué llegó y cuándo? |
| **Silver** | Una fila correcta por entidad: tipos, nombres estándar, limpieza, calidad, estado actual (SCD1) o historial (SCD2) | No agrega ni modela para un reporte | ¿Qué es correcto? |
| **Gold** | Modelo dimensional (estrella) listo para analítica y ML | No corrige datos: confía en silver | ¿Qué le importa al negocio? |

**Por qué estas etapas:** cada una tiene un solo motivo para cambiar. Una regla de calidad nueva toca solo silver; un KPI nuevo, solo gold; una fuente nueva, solo la ingesta. Y como bronze conserva todo, cualquier capa posterior se puede reconstruir desde cero.

**Idempotencia:** silver usa `AUTO CDC` (aplicar el mismo cambio dos veces deja el mismo resultado) y gold son vistas materializadas recalculadas por el pipeline. Re-ejecutar sin datos nuevos no cambia nada.

## 2. Silver: de los cambios CDC al estado correcto

### Cómo se aplican los cambios (`AUTO CDC`)

Bronze tiene, por tabla, la foto completa (`R`) y cada cambio (`I`, `U`, `D`). `AUTO CDC`:
- **ordena** los cambios por la secuencia y se queda con el último por clave (resuelve duplicados y llegadas fuera de orden);
- aplica los `D` como **borrados**;
- arma **SCD1** (estado actual) o **SCD2** (una versión por cambio, con vigencia `__START_AT` / `__END_AT`).

**Secuencia de los cambios:** `struct(fecha_efectiva, _lsn, _seqval)`, con `fecha_efectiva = coalesce(_commit_ts, UpdatedAt)`.
- En los cambios incrementales, `_commit_ts` es la hora real del commit en Azure SQL, y `_lsn`/`_seqval` desempatan dentro de una misma transacción.
- En la foto completa (`R`), `_commit_ts` es nulo: se usa `UpdatedAt`, que los triggers de la fuente mantienen en cada cambio. Así la versión inicial de un cliente queda fechada cuando realmente cambió, no cuando se hizo la foto.

### Tablas de silver

| Tabla silver | Origen | Clave | Tipo | Por qué |
|---|---|---|---|---|
| `clientes` | `customers_raw` | `id_cliente` | SCD1 | Estado actual para cruces y features |
| `clientes_historial` | `customers_raw` | `id_cliente` | **SCD2** sobre `segmento` | Atribuir cada venta al segmento que el cliente tenía **al comprar**. Solo `segmento` abre versión nueva: corregir un teléfono no es un cambio de negocio |
| `productos` | `products_raw` | `id_producto` | SCD1 | El precio histórico ya vive en cada línea (`precio_unitario`) |
| `pedidos` | `orders_raw` | `id_pedido` | SCD1 | Estado actual del pedido |
| `items_pedido` | `order_items_raw` | `id_item` | SCD1 | Incluye los DELETE de líneas |
| `pagos` | `payments_raw` | `id_pago` | SCD1 | Estado actual del pago |
| `pagos_historial_estados` | `payments_raw` | `id_pago` | **SCD2** sobre `estado` | Recorrido `pendiente → aprobado → reembolsado`: tiempos de aprobación, reembolsos, intentos |
| `tickets` | `support_tickets_raw` | `id_ticket` | SCD1 | Incluye los DELETE de tickets |
| `cuarentena` | todas | — | append | Filas que no pasan una regla, con la tabla y la regla. **Nada se descarta en silencio** |
| `control_totales_pedido` | `pedidos` + `items_pedido` | `id_pedido` | vista materializada | Integridad entre cabecera y detalle (total ≠ suma de líneas, pedidos sin líneas) |

### Nombres y tipos

- **snake_case en español**, como anticipa el DDL de la fuente: `CustomerID → id_cliente`, `OrderDate → fecha_pedido`, `TotalAmount → monto_total`.
- Montos en `DECIMAL(12,2)`, fechas en `TIMESTAMP` (UTC), textos de dominio en minúsculas.
- Columnas técnicas que se conservan: `_lsn`, `_op`, `_fecha_efectiva`, `_id_ejecucion` (trazabilidad hasta el archivo de bronze).

## 3. Calidad de datos

Reglas como código ([src/transformacion/silver/reglas_calidad.py](../../src/transformacion/silver/reglas_calidad.py)). Tres acciones:

| Acción | Cuándo | Efecto |
|---|---|---|
| `fail` | El dato rompe el modelo (clave nula) | Detiene el pipeline: no se publica nada inconsistente |
| `drop` + **cuarentena** | El dato es inválido y no se puede corregir | No entra a silver; queda en `silver.cuarentena` con la regla que falló |
| `warn` + **marca** | El dato es mejorable pero útil | Entra a silver con una columna `es_*` que lo señala; la métrica queda en el pipeline |

Cada caso borde que siembra el generador ([log de la carga](../../fuentes/logs/inicial_20261010_162156.json)) tiene su regla:

| Caso borde en la fuente | Regla en silver | Acción |
|---|---|---|
| Email con mayúsculas o espacios | `email_normalizado = lower(trim(email))` | Se corrige |
| Email con formato inválido | regex de email | `warn` + `es_email_valido` |
| Ciudad mal escrita (`LIMA`, `lima `, `Lima Metropolitana`) | Normalización a la forma canónica | Se corrige |
| Teléfono o ciudad nulos | Se aceptan (dato opcional) | `warn` |
| Cliente duplicado (misma persona, dos cuentas) | Mismo `email_normalizado` en más de un `id_cliente` | `warn` + `es_posible_duplicado` (no se fusionan: requiere una regla de negocio de identidad) |
| Producto sin descripción | `descripcion` nula | `warn` + `tiene_descripcion` (importa para el RAG del nivel 5) |
| Total del pedido ≠ suma de sus líneas | `control_totales_pedido` | Se marca `es_total_descuadrado`; gold usa la suma de líneas para ventas |
| Pedido sin líneas | `control_totales_pedido` | Se marca `es_sin_items` |
| Línea con pedido o producto aún no llegado | Integridad referencial contra silver | `warn` (con FK reales en la fuente, solo ocurre por orden de llegada) |
| Cantidad ≤ 0, precio o monto negativo | Rangos válidos | `drop` + cuarentena |
| Canal, estado, método de pago o prioridad fuera del dominio | Lista de valores permitidos | `drop` + cuarentena |
| Ticket con cuerpo nulo | `cuerpo` nulo | `warn` + `tiene_cuerpo` |
| Ticket muy largo o con emojis | Se conservan (texto real para el RAG) | — |
| Varios intentos de pago por pedido | Comportamiento normal, no error | Se cuentan en gold (`n_intentos_pago`) |

## 4. Gold: modelo estrella

```
                       dim_fecha
                           │
 dim_clientes (SCD2) ── fact_ventas ── dim_productos
                           │
                       dim_canal

 dim_clientes ── fact_pagos ── dim_fecha
                    │
                (id_pedido → fact_ventas)
```

| Tabla | Grano (una fila por…) | Contenido |
|---|---|---|
| `dim_clientes` | versión de cliente (SCD2) | `sk_cliente`, `id_cliente`, nombre, país, ciudad, `segmento`, `vigente_desde`, `vigente_hasta`, `es_vigente` |
| `dim_productos` | producto | `sk_producto`, `id_producto`, SKU, nombre, categoría, precio actual, estado |
| `dim_fecha` | día | `sk_fecha` (AAAAMMDD), año, trimestre, mes, día de la semana, es fin de semana |
| `dim_canal` | canal | `sk_canal`, canal (`web`, `app`, `tienda`) |
| `fact_ventas` | **línea de pedido** | claves de las dimensiones, `id_pedido`, cantidad, precio unitario, descuento, `monto_item`, estado del pedido |
| `fact_pagos` | pago (estado actual) | claves, `id_pedido`, método, monto, estado, `n_cambios_estado`, minutos hasta aprobarse, `n_intentos_pago` del pedido |

- **Grano de `fact_ventas` = línea:** permite ventas por producto y categoría y sumar sin duplicar.
- **El cliente se une a la versión SCD2 vigente en la fecha del pedido** (`fecha_pedido` entre `vigente_desde` y `vigente_hasta`): una venta de marzo cuenta para el segmento de marzo aunque hoy el cliente sea VIP.
- **Claves sustitutas deterministas** (hash de la clave natural y la vigencia): recalcular gold produce las mismas claves, así que es idempotente.
- **Ventas = suma de líneas** (`cantidad × precio_unitario − descuento`), no el total de cabecera, que en algunos pedidos legacy no cuadra.

## 5. Formato y organización física

- **Delta Lake** en todas las capas, como tablas administradas de Unity Catalog en `andina_<env>.silver` y `andina_<env>.gold`.
- **Liquid clustering en lugar de particiones:** con ~170 000 filas, particionar por fecha generaría miles de archivos diminutos. `CLUSTER BY` (por ejemplo `fact_ventas` por fecha y cliente) ordena los datos para las consultas frecuentes y se ajusta solo cuando el volumen crece.
- **Pipelines serverless en modo `triggered`:** procesan lo nuevo y se apagan. Los ejecutan las tareas `transformacion_silver` y `transformacion_gold` del `job_andina_diario`, después de la ingesta.
