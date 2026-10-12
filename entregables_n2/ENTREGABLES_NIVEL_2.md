# Entregables · Nivel 2 (Transformación y modelado)

Copia de lo entregado hasta el nivel 2, con la misma estructura de carpetas que el repositorio (los enlaces del [README](README.md) funcionan dentro de esta carpeta). El código vivo, el que despliega el bundle, está en la raíz del repositorio.

## Lo que pide el reto y dónde está

| Requisito del reto (nivel 2) | Archivo |
|---|---|
| **Pipeline de transformación** (Lakeflow Declarative Pipelines) | Silver: un notebook por entidad en [src/transformacion/silver/](src/transformacion/silver/) · Gold: un notebook por tabla en [src/transformacion/gold/](src/transformacion/gold/) |
| **Calidad de datos como código** (duplicados, nulos, tipos, integridad) | [src/transformacion/silver/reglas_calidad.py](src/transformacion/silver/reglas_calidad.py) · [slv_cuarentena.ipynb](src/transformacion/silver/slv_cuarentena.ipynb) · [slv_controles_integridad.ipynb](src/transformacion/silver/slv_controles_integridad.ipynb) |
| **Cambios en el tiempo** (SCD: segmento del cliente, estado del pago) | [slv_clientes.ipynb](src/transformacion/silver/slv_clientes.ipynb) · [slv_pagos_historial_estados.ipynb](src/transformacion/silver/slv_pagos_historial_estados.ipynb) · decisiones D-18 y D-19 del [README](README.md) |
| **Diagrama del modelo de datos** | [docs/modelo_datos/andina-market-modelo-datos.jpg](docs/modelo_datos/andina-market-modelo-datos.jpg) · editable [.excalidraw](docs/modelo_datos/andina-market-modelo-datos.excalidraw) |
| **Justificación escrita de las etapas** | [docs/modelo_datos/modelo_datos.md](docs/modelo_datos/modelo_datos.md) · sección 5 del [README](README.md) |
| **Formato, particionamiento y organización** | Sección 5 de [docs/modelo_datos/modelo_datos.md](docs/modelo_datos/modelo_datos.md) · decisión D-22 del [README](README.md) |
| **Despliegue entre entornos** (un bundle, targets dev y prd) | [databricks.yml](databricks.yml) · [resources/job_andina_diario.yml](resources/job_andina_diario.yml) · [resources/pl_andina_silver.yml](resources/pl_andina_silver.yml) · [resources/pl_andina_gold.yml](resources/pl_andina_gold.yml) |

## Evidencia

| Qué demuestra | Dónde |
|---|---|
| Silver cuadra con bronze; gold cuadra con silver; integridad; SCD2; idempotencia; job de 4 tareas | Sección 5 del [README](README.md), «Evidencia» |
| Las mismas pruebas, ejecutables | [src/validacion/nb_validacion_silver_gold.ipynb](src/validacion/nb_validacion_silver_gold.ipynb) |

## Estándares

[docs/estandar_documentacion.md](docs/estandar_documentacion.md): cabeceras, comentarios, nombres y organización.

## Cómo usar esta carpeta

- **Revisar:** abrir [README.md](README.md).
- **Desplegar:** desde la raíz del repositorio (`databricks bundle deploy -t dev`), no esta copia.
