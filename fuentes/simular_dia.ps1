# =============================================================================
#  Archivo   : simular_dia.ps1
#  Propósito : Simular un día de operación en Azure SQL (modo "dia" del generador)
#              para probar la ingesta incremental con CDC.
#  Uso       : desde la raíz del repo, en PowerShell:  .\fuentes\simular_dia.ps1
#  Seguridad : la contraseña se pide sin mostrarse, vive solo en esta sesión y
#              se borra al terminar (también si el generador falla).
#  Salida    : fuentes/logs/dia_<fecha>.json con inserts/updates/deletes por tabla.
# =============================================================================

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

& "$raiz\.venv\Scripts\Activate.ps1"
$env:ANDINA_SQL_USER = "sqladmin_andina"
$s = Read-Host "Contraseña de sqladmin_andina" -AsSecureString
$env:ANDINA_SQL_PASSWORD = (New-Object System.Management.Automation.PSCredential("x", $s)).GetNetworkCredential().Password

try {
    python "$raiz\fuentes\02_generar_datos_andina.py" --modo dia
}
finally {
    Remove-Item Env:ANDINA_SQL_PASSWORD -ErrorAction SilentlyContinue
}
