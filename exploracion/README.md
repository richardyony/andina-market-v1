# Exploración · notebooks de prueba

Notebooks escritos a mano en el workspace (`/Users/<usuario>/nb_pruebas`) mientras se
construía la v1. Se exportaron el 2026-10-20 en formato fuente para conservar el proceso:
no forman parte del bundle ni del job, y el código productivo vive en `src/`.

| Notebook | Para qué sirvió |
|---|---|
| `nb_test_conexion_cdc.py` | Probar la conexión JDBC a Azure SQL con credenciales del secret scope y leer las tablas de CDC |
| `nb_validacion_bronze_sqlserver.py` | Validar conteos y contenido de bronze contra la fuente |
| `nb_bronze_sqlserver_ingesta.py` | Primera versión de la ingesta a bronze (luego llevada a `src/`) |
| `enmascarando.py` | Probar las funciones de máscara de PII en Unity Catalog |
| `spark1.py` | Pruebas sueltas de Spark |

Las credenciales siempre se leen con `dbutils.secrets.get`; ningún notebook contiene contraseñas.
