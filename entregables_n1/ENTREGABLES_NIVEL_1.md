# Entregables · Nivel 1 (Ingesta)

Copia de lo entregado para el nivel 1, con la misma estructura de carpetas que el repositorio (los enlaces del [README](README.md) funcionan dentro de esta carpeta). El código vivo, el que despliega el bundle, está en la raíz del repositorio.

## Lo que pide el reto y dónde está

| Requisito del reto (nivel 1) | Archivo |
|---|---|
| **Código / notebooks de ingesta** | [src/ingesta/nb_bronze_sqlserver_ingesta.ipynb](src/ingesta/nb_bronze_sqlserver_ingesta.ipynb) · [src/ingesta/nb_preparar_entorno.ipynb](src/ingesta/nb_preparar_entorno.ipynb) |
| **Jobs** (Lakeflow Jobs) | [resources/ingesta_job.yml](resources/ingesta_job.yml) |
| **Despliegue reproducible** (Declarative Automation Bundle, dev/prd) | [databricks.yml](databricks.yml) |
| **README con decisiones** (y supuestos, cómo reproducir, uso de IA) | [README.md](README.md) |
| **Diagrama propio de la arquitectura** | [docs/arquitectura/andina-market-arquitectura-v4.2.jpg](docs/arquitectura/andina-market-arquitectura-v4.2.jpg) · editable [.excalidraw](docs/arquitectura/andina-market-arquitectura-v4.2.excalidraw) · versión anterior [v3.1](docs/arquitectura/andina-market-arquitectura-v3.1.excalidraw) |
| **Diseño de streaming** (documento + diagrama) | [docs/diseno_streaming.md](docs/diseno_streaming.md) · [diagrama](docs/arquitectura/andina-market-streaming.jpg) |
| **Diseño de SAP ECC on-premise** (documento + diagrama) | [docs/diseno_sap.md](docs/diseno_sap.md) · [diagrama](docs/arquitectura/andina-market-sap.jpg) |
| **Muestra de eventos de streaming (.jsonl)** | [sample_data/clickstream_app_events.jsonl](sample_data/clickstream_app_events.jsonl) · generador [fuentes/06_generar_clickstream.py](fuentes/06_generar_clickstream.py) |

## Base de origen (kit de arranque, sección 3 del reto)

| Requisito | Archivo |
|---|---|
| Esquema de la base relacional (Azure SQL) | [fuentes/01_ddl_tablas.sql](fuentes/01_ddl_tablas.sql) |
| Datos sintéticos inyectados directamente en la base | [fuentes/02_generar_datos_andina.py](fuentes/02_generar_datos_andina.py) · [fuentes/simular_dia.ps1](fuentes/simular_dia.ps1) |
| Detección de cambios en la fuente (CDC) | [fuentes/03_habilitar_cdc.sql](fuentes/03_habilitar_cdc.sql) |

## Evidencia

| Qué demuestra | Archivo |
|---|---|
| Volumetría y casos borde de la carga inicial | [fuentes/logs/inicial_20261010_162156.json](fuentes/logs/inicial_20261010_162156.json) |
| Cambios del día simulado (inserts, updates, deletes por tabla) | [fuentes/logs/dia_20261012_003146.json](fuentes/logs/dia_20261012_003146.json) |
| Muestra del clickstream generada (12 533 eventos, casos borde) | [fuentes/logs/clickstream_2026-10-12_113727.json](fuentes/logs/clickstream_2026-10-12_113727.json) |
| Validación de bronze contra la fuente | [src/validacion/nb_validacion_bronze_sqlserver.ipynb](src/validacion/nb_validacion_bronze_sqlserver.ipynb) · resultados en la sección 4 del [README](README.md) |

## Cómo usar esta carpeta

- **Revisar:** abrir [README.md](README.md); todos sus enlaces apuntan a archivos de esta carpeta.
- **Desplegar:** usar la raíz del repositorio (`databricks bundle deploy -t dev`), no esta copia.
