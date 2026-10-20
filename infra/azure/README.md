# Infraestructura Azure · Andina Market v1

Inventario de los recursos creados desde el portal (2026-10-08 → 2026-10-14), para poder
recrearlos en una v2. Región única: **Central US**. Tags en todos los grupos:
`proyecto=andinamarket`, `owner=rhuaman`, `entorno=<shared|dev|prd|source>`.

`arm/` contiene la plantilla ARM exportada de cada grupo (`az group export`, 2026-10-20).
Sirve como referencia de configuración, no como despliegue listo: la exportación no incluye
valores de secretos, omite algunos subrecursos (endpoints privados de Databricks, políticas
internas de SQL) y las IPs del firewall de SQL se reemplazaron por marcadores `<IP_...>`.

## Recursos

| Grupo | Recurso | Tipo | Configuración clave |
|---|---|---|---|
| `rg-andinamarket-shared-cus-001` | `dbw-andinamarket-cus-001` | Databricks workspace | Premium · VNet administrada · sin IP pública (secure cluster connectivity) · grupo administrado `rg-andinamarket-dbw-managed-cus-001` |
| | `dbac-andinamarket-cus-001` | Access Connector (identidad administrada) | `Storage Blob Data Contributor` sobre las dos cuentas de storage |
| `rg-andinamarket-dev-cus-001` | `standinadevcus001` | Storage ADLS Gen2 | Standard_LRS · HNS activado · Hot · TLS 1.2 · contenedor `lakehouse` |
| | `kv-andina-dev-cus-001` | Key Vault | Standard · RBAC · soft delete 7 días · secretos `sql-usuario`, `sql-password` |
| `rg-andinamarket-prd-cus-001` | `standinaprdcus001` | Storage ADLS Gen2 | Igual que dev |
| | `kv-andina-prd-cus-001` | Key Vault | Igual que dev |
| `rg-andinamarket-sql-cus-001` | `sql-andinamarket-cus-001` | Azure SQL server | Admin SQL `sqladmin_andina` · admin Entra `grp-andina-data-engineers` · TLS 1.2 · acceso público con firewall (servicios Azure + IPs del desarrollador) |
| | `sqldb-andinamarket-cus-001` | Azure SQL database | Serverless GP_S_Gen5_2 (0.5–2 vCore) · **oferta gratuita** (al agotarse: auto-pausa) · auto-pausa 60 min · 32 GB · backup LRS · CDC habilitado (`fuentes/03_habilitar_cdc.sql`) |

Lo que vive dentro de Databricks (catálogos `andina_dev` / `andina_prd`, esquemas, external
locations, secret scopes `kv-andina-<entorno>`) está en `infra/04_unity_catalog_objetos.sql` y
en el bundle (`databricks.yml`, `resources/`).

## Costo (referencia v1)

- Total 2026-10-08 → 2026-10-17: ~32.8 USD (Databricks ~23.5, NAT Gateway ~8.1, VNet ~0.9).
- **Fijo mientras exista el workspace:** ~1.20 USD/día (NAT Gateway + IP pública del grupo
  administrado). No se puede apagar; solo desaparece al borrar el workspace.
- SQL warehouse: 2X-Small, auto-stop 5 min. Azure SQL: oferta gratuita + auto-pausa.

## Orden de creación (para recrear)

1. Grupos de recursos con sus tags.
2. Azure SQL server + base serverless (oferta gratuita) → firewall → `fuentes/01_ddl_tablas.sql`,
   `02_generar_datos_andina.py`, `03_habilitar_cdc.sql`.
3. Storage dev/prd (HNS) con contenedor `lakehouse`; Key Vault dev/prd con `sql-usuario` y `sql-password`.
4. Workspace Databricks Premium (sin IP pública) + Access Connector con `Storage Blob Data Contributor`.
5. En Databricks: storage credential, external locations, catálogos y secret scopes
   (`infra/04_unity_catalog_objetos.sql`) → `databricks bundle deploy -t dev`.
