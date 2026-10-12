# Estándar de documentación y código

Convenciones que sigue todo el repositorio. El objetivo: que cualquier persona del equipo entienda **qué hace** cada pieza y **por qué** está hecha así, sin tener que preguntarle a quien la escribió.

## 1. Cabecera en cada archivo

Todo archivo de código (`.py`, `.sql`, `.ps1`) y todo notebook empieza con una cabecera:

| Campo | Contenido |
|---|---|
| Archivo | Ruta relativa dentro de su carpeta |
| Proyecto | Andina Market · Reto técnico de Ingeniería de Datos |
| Capa | Fuente, bronze, silver, gold, validación o infraestructura (y nivel del reto) |
| Propósito | Qué hace, en una o dos frases |
| Entradas / Salidas | Qué lee y qué escribe (tablas, Volumes, archivos) |
| Decisiones | Lo que no es obvio y su porqué (cuando aplica) |
| Idempotencia | Por qué re-ejecutar no duplica ni pierde datos (cuando aplica) |
| Autoría | Persona responsable y si hubo asistencia de IA |
| Versión | Número, fecha y motivo del último cambio relevante |

En los notebooks la cabecera es una tabla en la primera celda de markdown.

## 2. Comentarios

- Explican el **porqué**, no el qué. Mal: `# filtra nulos`. Bien: `# Los DELETE CDC nunca se descartan: perderíamos un borrado de la fuente`.
- Cada decisión de diseño no evidente lleva su motivo y, si aplica, la alternativa descartada.
- Nada de comentarios que repiten el código ni de código comentado "por si acaso" (para eso está git).

## 3. Nombres

| Elemento | Convención | Ejemplo |
|---|---|---|
| Recursos de Azure | `tipo-proyecto-entorno-región-número` (Microsoft CAF) | `rg-andinamarket-dev-cus-001`, `kv-andina-prd-cus-001` |
| Catálogos de Unity Catalog | `andina_<entorno>` | `andina_dev`, `andina_prd` |
| Tablas de bronze | nombre de la fuente en snake_case + `_raw` | `bronze.order_items_raw` |
| Tablas de silver | entidad en español, snake_case | `silver.items_pedido`, `silver.pagos_historial_estados` |
| Tablas de gold | `dim_` / `fact_` / `agg_` + entidad | `gold.dim_clientes`, `gold.fact_ventas` |
| Columnas de negocio | snake_case en español | `id_cliente`, `fecha_pedido`, `monto_total` |
| Columnas técnicas | prefijo `_` | `_lsn`, `_op`, `_fecha_efectiva`, `_id_ejecucion` |
| Banderas de calidad | `es_*` / `tiene_*` | `es_email_valido`, `tiene_descripcion` |
| Notebooks de silver | `slv_<entidad>.ipynb` | `slv_clientes.ipynb` |
| Notebooks de gold | `dim_<entidad>.ipynb`, `fact_<entidad>.ipynb` | `fact_ventas.ipynb` |
| Notebooks | `nb_<capa>_<qué hace>` | `nb_bronze_sqlserver_ingesta` |
| Bundle, job y pipelines | `andina_market` · `job_andina_<frecuencia>` · `pl_andina_<capa>` | `job_andina_diario`, `pl_andina_silver` |

## 4. Organización

- **Un solo bundle con dos targets** (`databricks.yml` + `resources/`): qué se despliega (YAML). Un job diario con una tarea por capa.
- **El código en `src/`**: cómo se procesa. Bronze, un notebook por fuente; silver y gold, un notebook por entidad, con sus pasos explicados en celdas de markdown. Lo compartido va en módulos `.py` (`comun.py`, `reglas_calidad.py`, `comun_gold.py`).
- **Validaciones en `src/validacion/`:** no van en los jobs; se ejecutan para verificar y dejar evidencia.
- **Infraestructura de Unity Catalog en `infra/`:** se ejecuta una vez por ambiente.
- **Documentación en `docs/`:** decisiones, modelo de datos, diseños y evidencias.

## 5. Calidad de datos

- Las reglas viven como código en [src/transformacion/silver/reglas_calidad.py](../src/transformacion/silver/reglas_calidad.py), agrupadas por entidad.
- Tres acciones: **fallar** (rompe el modelo), **descartar a cuarentena** (inválido) y **advertir** (mejorable; se marca con `es_*`).
- Nada se descarta en silencio: lo rechazado queda en `silver.cuarentena` con la regla que falló.

## 6. Seguridad

- Ninguna credencial en el código ni en git: van en Key Vault y se leen con un secret scope.
- Los permisos se dan a grupos, nunca a personas.
- Databricks lee la fuente con un usuario de solo lectura.

## 7. Commits

- Mensaje en español, en imperativo o descriptivo: qué cambia y por qué.
- Un commit por unidad lógica de trabajo (por ejemplo: "Nivel 2: pipeline declarativo bronze → silver → gold").
- Si hubo asistencia de IA, se indica en el pie del commit.
