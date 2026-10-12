# Entregables · Nivel 2 (Transformación y modelado)

Copia de lo entregado hasta el nivel 2, con la misma estructura de carpetas que el repositorio (los enlaces del [README](README.md) funcionan dentro de esta carpeta). El código vivo, el que despliega el bundle, está en la raíz del repositorio.

## Lo que pide el reto y dónde está

| Requisito del reto (nivel 2) | Archivo |
|---|---|
| **Pipeline de transformación** (Lakeflow Declarative Pipelines) | [src/transformacion/silver.py](src/transformacion/silver.py) · [src/transformacion/gold.py](src/transformacion/gold.py) |
| **Calidad de datos como código** (duplicados, nulos, tipos, integridad) | [src/transformacion/reglas_calidad.py](src/transformacion/reglas_calidad.py) · cuarentena y controles en [silver.py](src/transformacion/silver.py) |
| **Cambios en el tiempo** (SCD: segmento del cliente, estado del pago) | `AUTO CDC` SCD1/SCD2 en [silver.py](src/transformacion/silver.py) · decisiones D-18 y D-19 del [README](README.md) |
| **Diagrama del modelo de datos** | [docs/arquitectura/andina-market-modelo-datos.jpg](docs/arquitectura/andina-market-modelo-datos.jpg) · editable [.excalidraw](docs/arquitectura/andina-market-modelo-datos.excalidraw) |
| **Justificación escrita de las etapas** | [docs/modelo_datos.md](docs/modelo_datos.md) · sección 5 del [README](README.md) |
| **Formato, particionamiento y organización** | Sección 5 de [docs/modelo_datos.md](docs/modelo_datos.md) · decisión D-22 del [README](README.md) |
| **Despliegue entre entornos** (bundle dev/prd) | [databricks.yml](databricks.yml) · [resources/transformacion_pipeline.yml](resources/transformacion_pipeline.yml) · [resources/ingesta_job.yml](resources/ingesta_job.yml) (3.ª tarea del job) |

## Evidencia

| Qué demuestra | Dónde |
|---|---|
| Silver cuadra con bronze; gold cuadra con silver; integridad; SCD2; idempotencia | Sección 5 del [README](README.md), «Evidencia» |
| Las mismas pruebas, ejecutables | [src/validacion/nb_validacion_silver_gold.ipynb](src/validacion/nb_validacion_silver_gold.ipynb) |

## Nivel 1

Todo lo del nivel 1 sigue incluido (ingesta, job, diseños de streaming y SAP, muestra .jsonl); ver también [entregables_n1](../entregables_n1/ENTREGABLES_NIVEL_1.md) en el repositorio.

## Cómo usar esta carpeta

- **Revisar:** abrir [README.md](README.md).
- **Desplegar:** usar la raíz del repositorio (`databricks bundle deploy -t dev`), no esta copia.
