# Entregables · Nivel 2 (Transformación y modelado)

Índice de lo que pide el reto para el nivel 2 y dónde está en el repositorio. Hay **un solo código**, el de este repositorio, que es el mismo que despliega el bundle; el nivel 1 tiene su propio índice: [ENTREGABLES_NIVEL_1.md](ENTREGABLES_NIVEL_1.md).

## Lo que pide el reto y dónde está

| Requisito del reto (nivel 2) | Archivo |
|---|---|
| **Pipeline de transformación** (Lakeflow Declarative Pipelines) | Silver: un notebook por entidad en [src/transformacion/silver/](src/transformacion/silver/) · Gold: un notebook por tabla en [src/transformacion/gold/](src/transformacion/gold/) |
| **Calidad de datos como código** (duplicados, nulos, tipos, integridad) | [src/transformacion/silver/reglas_calidad.py](src/transformacion/silver/reglas_calidad.py) · [slv_cuarentena.ipynb](src/transformacion/silver/slv_cuarentena.ipynb) · [slv_controles_integridad.ipynb](src/transformacion/silver/slv_controles_integridad.ipynb) |
| **Cambios en el tiempo** (SCD: segmento del cliente, estado del pago) | [slv_clientes.ipynb](src/transformacion/silver/slv_clientes.ipynb) · [slv_pagos_historial_estados.ipynb](src/transformacion/silver/slv_pagos_historial_estados.ipynb) · decisiones D-18 y D-19 del [README](README.md) |
| **Diagrama del modelo de datos** | [docs/modelo_datos/andina-market-modelo-datos.jpg](docs/modelo_datos/andina-market-modelo-datos.jpg) · editable [.excalidraw](docs/modelo_datos/andina-market-modelo-datos.excalidraw) |
| **Justificación escrita de las etapas** | [docs/modelo_datos/modelo_datos.md](docs/modelo_datos/modelo_datos.md) · sección 5 del [README](README.md) |
| **Respuestas del nivel 2**: las decisiones (etapas, calidad, modelado y SCD, formato) y lo que se espera dominar (patrones, pipelines declarativos, bundle) | [docs/respuestas_nivel_2_transformacion.docx](docs/respuestas_nivel_2_transformacion.docx) |
| **Gobierno de datos personales** (permisos por capa, máscaras de columna en gold) | [infra/04_unity_catalog_objetos.sql](infra/04_unity_catalog_objetos.sql) (pasos 6 y 7) · [dim_clientes.ipynb](src/transformacion/gold/dim_clientes.ipynb) · decisión D-24 y evidencia en la sección 5 del [README](README.md) |
| **Formato, particionamiento y organización** | Sección 5 de [docs/modelo_datos/modelo_datos.md](docs/modelo_datos/modelo_datos.md) · decisión D-22 del [README](README.md) |
| **Despliegue entre entornos** (un bundle, targets dev y prd) | [databricks.yml](databricks.yml) · [resources/job_andina_diario.yml](resources/job_andina_diario.yml) · [resources/pl_andina_silver.yml](resources/pl_andina_silver.yml) · [resources/pl_andina_gold.yml](resources/pl_andina_gold.yml) |

## Evidencia

| Qué demuestra | Dónde |
|---|---|
| Silver cuadra con bronze; gold cuadra con silver; integridad; SCD2; idempotencia; job de 4 tareas | Sección 5 del [README](README.md), «Evidencia» |
| Las mismas pruebas, ejecutables | [src/validacion/nb_validacion_silver_gold.ipynb](src/validacion/nb_validacion_silver_gold.ipynb) |
| Las reglas de calidad actúan ante datos malos (154 pruebas unitarias, Spark local) y métricas reales de lo que la limpieza corrigió | [tests/](tests/) (`python -m pytest`) · sección 5 del [README](README.md), «Calidad de datos: pruebas y métricas reales» |

## Estándares

[docs/estandar_documentacion.md](docs/estandar_documentacion.md): cabeceras, comentarios, nombres y organización.

## Cómo revisarlo

- **Leer:** [README.md](README.md), sección 5 (transformación, calidad y gobierno) y [docs/modelo_datos/modelo_datos.md](docs/modelo_datos/modelo_datos.md).
- **Probar sin Databricks:** `pip install -r requirements-dev.txt` y `python -m pytest` (154 pruebas, ~20 s).
- **Desplegar:** `databricks bundle deploy -t dev` desde la raíz (sección 6 del README).
