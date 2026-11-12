# Entregables · Nivel 1 (Ingesta)

Índice de lo que pide el reto para el nivel 1 y dónde está en el repositorio. Hay **un solo código**, el de este repositorio, que es el mismo que despliega el bundle; el nivel 2 tiene su propio índice: [ENTREGABLES_NIVEL_2.md](ENTREGABLES_NIVEL_2.md).

## Lo que pide el reto y dónde está

| Requisito del reto (nivel 1) | Archivo |
|---|---|
| **Código / notebooks de ingesta** | [src/01_bronze/nb_bronze_sqlserver_ingesta.ipynb](src/01_bronze/nb_bronze_sqlserver_ingesta.ipynb) · [src/00_preparacion/nb_preparar_entorno.ipynb](src/00_preparacion/nb_preparar_entorno.ipynb) |
| **Jobs** (Lakeflow Jobs) | [resources/jobs/job_andina_diario.yml](resources/jobs/job_andina_diario.yml): tareas `preparar_entorno` → `ingesta_bronze` (nivel 1), seguidas de silver y gold (nivel 2) · secuencia y corridas reales de la ingesta: [job_bronze_corridas_2026-10-12.jpg](docs/evidencias/job_bronze_corridas_2026-10-12.jpg) |
| **Despliegue reproducible** (un Declarative Automation Bundle, targets dev y prd) | [databricks.yml](databricks.yml) · [resources/](resources/) |
| **Objetos de Unity Catalog por ambiente** (catálogo, esquemas, Volumes, permisos) | [infra/04_unity_catalog_objetos.sql](infra/04_unity_catalog_objetos.sql) |
| **Respuestas del nivel 1**: las decisiones (carga, cambios, frecuencia, formato, orquestación, esquema, trazabilidad) y lo que se espera dominar (Auto Loader, checkpoints, bundles, streaming, arquitectura) | [docs/respuestas_nivel_1_ingesta.docx](docs/respuestas_nivel_1_ingesta.docx) |
| **README con decisiones** (y supuestos, cómo reproducir, uso de IA) | [README.md](README.md) |
| **Diagrama propio de la arquitectura** | [docs/arquitectura/andina-market-arquitectura-iconos.png](docs/arquitectura/andina-market-arquitectura-iconos.png) · editable [.excalidraw](docs/arquitectura/andina-market-arquitectura-iconos.excalidraw) o [.drawio](docs/arquitectura/andina-market-arquitectura-iconos.drawio) · versiones anteriores [v4.3](docs/arquitectura/historico/andina-market-arquitectura-v4.3.excalidraw) y [v3.1](docs/arquitectura/historico/andina-market-arquitectura-v3.1.excalidraw) |
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
| Validación de bronze contra la fuente | [src/04_validacion/nb_validacion_bronze_sqlserver.ipynb](src/04_validacion/nb_validacion_bronze_sqlserver.ipynb) · resultados en la sección 4 del [README](README.md) |

## Estándares

[docs/estandar_documentacion.md](docs/estandar_documentacion.md): cabeceras, comentarios, nombres y organización.

## Cómo revisarlo

- **Leer:** [README.md](README.md), secciones 1 a 4 y 6 a 8 para el nivel 1.
- **Desplegar:** `databricks bundle deploy -t dev` desde la raíz (sección 6 del README).
