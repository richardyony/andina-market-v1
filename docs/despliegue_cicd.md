# Despliegue y CI/CD

Cómo llega un cambio desde el equipo de un desarrollador hasta producción, y qué hay que configurar una sola vez para que funcione.

## 1. El flujo

```
feature/<tema> ──PR──▶ develop ──PR──▶ main ──tag v*──▶ producción
      │                  │               │                 │
      │ ci.yml           │ ci.yml        │ cd.yml          │ cd.yml (con aprobación)
      ▼                  ▼               ▼                 ▼
 deploy -t dev       pruebas +       deploy -t stg     deploy -t prd
 (a mano, propio)    validate ×3     (automático)      (Required reviewers)
```

| Ambiente | Catálogo | Quién despliega | Cuándo | Identidad de ejecución |
|---|---|---|---|---|
| `dev` | `andina_dev` | Cada desarrollador, desde su equipo | Mientras trabaja en su rama | Su usuario (`mode: development`: recursos con prefijo `[dev usuario]`, horarios en pausa) |
| `stg` | `andina_stg` | GitHub Actions (`cd.yml`) | Cada merge a `main` | Service principal `sp-andinamarket-cicd` (`run_as`) |
| `prd` | `andina_prd` | GitHub Actions (`cd.yml`) | Un tag `v*` sobre un commit de `main`, aprobado por una persona | Service principal `sp-andinamarket-cicd` (`run_as`) |

**Reglas del flujo:**
- **Nadie despliega stg ni prd desde su equipo.** Lo que está en esos ambientes es exactamente lo que hay en `main` o en el tag, y queda registrado en GitHub Actions quién, cuándo y qué.
- **Un PR solo se integra si pasa `ci.yml`.** Corre las 154 pruebas unitarias de silver y `databricks bundle validate` en los tres targets. Un error de YAML o una ruta mal escrita en prd se detecta en el PR, no al desplegar.
- **prd solo recibe código probado en stg.** `cd.yml` rechaza un tag que no apunte a un commit de `main`.
- **Por qué dev no se despliega desde CI:** en modo development cada identidad crea su propia copia de los pipelines. Si el service principal desplegara dev, habría dos pipelines publicando las mismas tablas de `andina_dev` y Databricks lo rechaza (`TABLE_ALREADY_MANAGED_BY_OTHER_PIPELINE`).

## 2. Qué valida el job en cada corrida

La última tarea del job, `validacion_final` ([src/04_validacion/nb_validacion_final.ipynb](../src/04_validacion/nb_validacion_final.ipynb)), comprueba que la corrida **hizo su trabajo**, no solo que terminó:

| Prueba | Condición de aprobación |
|---|---|
| Datos en cada capa | Las tablas principales de bronze, silver y gold tienen más de 0 filas |
| Frescura | La última ingesta de `bronze.orders_raw` es de los últimos 2 días |
| Conciliación | Por entidad: claves vivas en bronze − claves en cuarentena = filas en silver |
| Clave única | Ninguna clave repetida en las 6 tablas de silver |
| SCD2 | Una sola versión vigente por clave en `clientes_historial` y `pagos_historial_estados` |
| Gold = silver | Mismas líneas y mismo monto de ventas; mismos pagos |

Si una falla, la tarea lanza un error con la lista de pruebas fallidas: el job queda en rojo y llega el correo de alerta. Corre con `max_retries: 0`, porque reintentar no arregla un dato malo.

## 3. Configuración inicial (una sola vez)

### 3.1 Service principal (Databricks)

1. **Settings → Identity and access → Service principals → Add new**, nombre `sp-andinamarket-cicd` (gestionado por Databricks).
2. En el service principal, pestaña **Secrets → Generate secret**. Guarda el **Client ID** (es el *Application ID*) y el **Secret**: el secret se muestra una sola vez.
3. Darle acceso a lo que el job lee y escribe. En el SQL Editor, como administrador (reemplaza `<application-id>`):

   ```sql
   -- Catálogos de los ambientes que despliega (stg y prd)
   GRANT ALL PRIVILEGES ON CATALOG andina_stg TO `<application-id>`;
   GRANT ALL PRIVILEGES ON CATALOG andina_prd TO `<application-id>`;
   -- Crear los Volumes externos de landing de cada ambiente
   GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_andina_dev TO `<application-id>`;
   GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_andina_prd TO `<application-id>`;
   ```

   `ALL PRIVILEGES` sobre el catálogo es deliberado: el service principal es el **dueño operativo** de stg y prd (crea esquemas, Volumes, tablas y vistas). Las personas no escriben en esos catálogos; los analistas leen gold con sus propios permisos y máscaras.
4. Leer las credenciales de Azure SQL (secret scopes respaldados por Key Vault), desde tu equipo:

   ```powershell
   databricks secrets put-acl kv-andina-dev <application-id> READ   # stg usa la base de dev
   databricks secrets put-acl kv-andina-prd <application-id> READ
   ```

### 3.2 GitHub Environments

En el repositorio: **Settings → Environments**. Crea tres:

| Environment | Secrets | Protección |
|---|---|---|
| `ci` | `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | Ninguna (solo valida) |
| `stg` | Los mismos dos | *Deployment branches*: solo `main` |
| `prd` | Los mismos dos | *Required reviewers*: tú. *Deployment branches and tags*: solo tags `v*` |

Y una **variable de repositorio** (Settings → Secrets and variables → Actions → Variables): `DATABRICKS_HOST` = `https://adb-7405605614782968.8.azuredatabricks.net`.

> En una empresa habría un service principal por ambiente (y un workspace por ambiente). Aquí es uno solo porque los tres ambientes comparten workspace; el aislamiento lo dan el catálogo, el GitHub Environment y la aprobación de prd.

### 3.3 Catálogo de stg

Una vez, como administrador, con [infra/04_unity_catalog_objetos.sql](../infra/04_unity_catalog_objetos.sql) adaptado a stg (sección "Para stg" al final del script). stg reutiliza el storage, la ubicación externa y el Key Vault de dev, con su propio catálogo y su propia carpeta de landing (`stg/landing`), para no solapar los Volumes de dev.

### 3.4 Antes del primer despliegue de prd con el service principal

Si prd ya se desplegó antes con tu usuario, sus pipelines publican las tablas de `andina_prd` y el despliegue del service principal crearía pipelines nuevos sobre las mismas tablas (mismo error de dev). Hay que revisarlo antes del primer tag:

```powershell
databricks bundle summary -t prd     # con tu usuario: ¿hay recursos desplegados?
```

Si los hay, se decide entre retirarlos o vincularlos (`databricks bundle deployment bind`) al despliegue del service principal.

## 4. Operación diaria

```powershell
# Desarrollador, en su rama feature/<tema>
databricks bundle deploy -t dev
databricks bundle run job_andina_diario -t dev

# Integrar: PR feature → develop, y luego PR develop → main (despliega stg)
# Liberar a producción, desde main actualizado:
git tag -a v1.1 -m "Andina Market v1.1: stg + CI/CD + validación final"
git push origin v1.1         # cd.yml espera tu aprobación en GitHub y despliega prd
```
