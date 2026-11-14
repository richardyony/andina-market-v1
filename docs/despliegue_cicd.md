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
| `stg` | `andina_stg` | GitHub Actions (`cd.yml`) | Cada merge a `main` | Service principal `sp-andina-stg` (`run_as`) |
| `prd` | `andina_prd` | GitHub Actions (`cd.yml`) | Un tag `v*` sobre un commit de `main`, aprobado por una persona | Service principal `sp-andina-prd` (`run_as`) |

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

### 3.1 Service principals (Databricks): uno por ambiente

| Service principal | Ambiente | Permisos |
|---|---|---|
| `sp-andina-stg` | stg (y la validación del CI) | `andina_stg`, ubicación `loc_andina_dev`, scope `kv-andina-dev` |
| `sp-andina-prd` | prd | `andina_prd`, ubicación `loc_andina_prd`, scope `kv-andina-prd` |

Si se filtrara el secret de stg, no da acceso a prd.

1. **Settings → Identity and access → Service principals → Add new** (gestionado por Databricks). `sp-andina-prd` ya existe; falta crear `sp-andina-stg`.
2. En cada service principal, pestaña **Secrets → Generate secret**: vigencia **90 días** y scope **Other APIs → `all-apis` (Read + Write)**. Guarda el **Client ID** (es el *Application ID*) y el **Secret**: el secret se muestra una sola vez. Anota la fecha de vencimiento para rotarlo.

   > **Por qué `all-apis`** (Databricks lo marca como no recomendado): `bundle deploy` usa APIs de workspace, jobs, pipelines, Unity Catalog y permisos, y la lista exacta se descubre por prueba y error. Lo que el SP puede tocar lo limitan igual sus GRANT de Unity Catalog. **Mejora pendiente:** revisar en `system.access.audit` las APIs que usa de verdad, generar un secret solo con esos scopes y revocar el actual.
3. Darle acceso a lo que el job lee y escribe. En el SQL Editor, como administrador (reemplaza los `<application-id>`):

   ```sql
   -- stg
   GRANT ALL PRIVILEGES ON CATALOG andina_stg TO `<application-id-stg>`;
   GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_andina_dev TO `<application-id-stg>`;
   -- prd (ALL PRIVILEGES sobre andina_prd ya está otorgado)
   GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_andina_prd TO `<application-id-prd>`;
   ```

   `ALL PRIVILEGES` sobre el catálogo es deliberado: el service principal es el **dueño operativo** de su ambiente (crea esquemas, Volumes, tablas y vistas). Las personas no escriben en esos catálogos; los analistas leen gold con sus propios permisos y máscaras.
4. Leer las credenciales de Azure SQL (secret scopes respaldados por Key Vault), desde tu equipo:

   ```powershell
   databricks secrets put-acl kv-andina-dev <application-id-stg> READ   # stg usa la base de dev
   # kv-andina-prd: sp-andina-prd ya tiene READ
   ```

### 3.2 GitHub Environments

En el repositorio: **Settings → Environments**. Crea tres:

| Environment | Secrets | Protección |
|---|---|---|
| `ci` | `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` de `sp-andina-stg` | Ninguna (solo valida) |
| `stg` | Los mismos dos, de `sp-andina-stg` | *Deployment branches*: solo `main` |
| `prd` | Los mismos dos, de `sp-andina-prd` | *Required reviewers*: tú. *Deployment branches and tags*: solo tags `v*` |

> **Plan de GitHub:** en el plan gratuito, *Required reviewers*, las reglas de ramas y tags y los secrets de Environment solo se aplican en repositorios **públicos**. En un repo privado hace falta GitHub Pro o Team. Sin ellos, prd queda protegido solo por `cd.yml`: se dispara con un tag `v*` y rechaza un tag que no esté en `main`.

Y una **variable de repositorio** (Settings → Secrets and variables → Actions → Variables): `DATABRICKS_HOST` = `https://adb-7405605614782968.8.azuredatabricks.net`.

> En una empresa habría además un workspace por ambiente. Aquí los tres comparten workspace; el aislamiento lo dan el catálogo, el service principal de cada ambiente, el GitHub Environment y la aprobación de prd.

### 3.3 Catálogo de stg

Una vez, como administrador, con [infra/04_unity_catalog_objetos.sql](../infra/04_unity_catalog_objetos.sql) adaptado a stg (sección "Para stg" al final del script). stg reutiliza el storage, la ubicación externa y el Key Vault de dev, con su propio catálogo y su propia carpeta de landing (`stg/landing`), para no solapar los Volumes de dev.

### 3.4 Antes del primer despliegue de prd con el service principal

Si prd se hubiera desplegado antes con un usuario, sus pipelines serían dueños de las tablas de `andina_prd` y el service principal crearía pipelines duplicados (mismo error que en dev). Revisado el 2026-11-13: `andina_prd` tiene solo sus esquemas, sin tablas, Volumes ni pipelines, así que el primer despliegue con `sp-andina-prd` parte limpio.

**Dueño de los esquemas.** En stg y prd, el dueño de los esquemas operativos (`landing`, `control`, `bronze`, `silver`, `gold`) debe ser el service principal. Si los crea el job, lo es automáticamente. En prd ya existían, creados a mano en la v1, y la primera corrida falló en `preparar_entorno` con `PERMISSION_DENIED: ... does not have MANAGE on Schema 'andina_prd.landing'`: `COMMENT ON SCHEMA` exige ser dueño o tener MANAGE, y `ALL PRIVILEGES` no lo incluye. stg no lo detectó porque partió vacío. Corrección:

```sql
ALTER SCHEMA andina_prd.landing OWNER TO `<application-id-prd>`;   -- igual bronze, silver y gold
```

`gobierno` sigue siendo del administrador: quien define las máscaras de datos personales no es quien escribe los datos.

### 3.4.1 Máscaras de datos personales en stg y prd

`gold.dim_clientes` aplica máscaras de columna con las funciones de `<catálogo>.gobierno`. Si el esquema `gobierno` no existe en el catálogo del ambiente, el pipeline de gold falla con *function not found*. Al 2026-11-13 existía en `andina_dev` y faltaba en `andina_prd`. Se crean con el paso 7 de [infra/04_unity_catalog_objetos.sql](../infra/04_unity_catalog_objetos.sql), cambiando el catálogo.

La máscara se evalúa según **quien consulta**, no según el dueño de la tabla (el service principal). Los analistas ven el dato enmascarado y `grp-andina-pii-lectores` ve el real. `es_posible_duplicado` se calcula desde silver, que no tiene máscara, así que no se ve afectado.

**Después del primer despliegue de prd** (el esquema gold lo crea el pipeline):

```sql
GRANT USE SCHEMA, SELECT ON SCHEMA andina_prd.gold TO `grp-andina-analistas`;
```

### 3.4.2 Permisos de grupos por ambiente

stg replica los permisos de prd: solo el service principal escribe. Así lo que se prueba en stg es lo que pasará en prd.

| Grupo | dev | stg | prd |
|---|---|---|---|
| `grp-andina-data-engineers` | Leer y escribir (su entorno de trabajo) | Solo leer | Solo leer |
| `grp-andina-analistas` | Leer gold (probar máscaras) | **Sin acceso** | Leer gold |
| Service principal | No aplica (dev corre con el usuario) | Dueño (`sp-andina-stg`) | Dueño (`sp-andina-prd`) |

Los analistas no ven stg: sus datos son de prueba y pueden recrearse. Los números del negocio salen solo de prd.

### 3.5 Rotación de secrets (cada 90 días, igual en stg y prd)

Una sola regla para todos los ambientes: los secrets de los service principals duran **90 días**. Para rotar sin cortar el servicio, unos días antes del vencimiento:

1. Generar un secret **nuevo** en el service principal. Puede tener dos activos a la vez.
2. Actualizar `DATABRICKS_CLIENT_SECRET` en el GitHub Environment del ambiente (y en `ci` si es el de stg).
3. Comprobar que un despliegue funciona.
4. **Revocar** el secret anterior.

Un secret que no está guardado en ningún GitHub Environment se revoca: nadie debe tener una credencial de service principal fuera de CI/CD.

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
