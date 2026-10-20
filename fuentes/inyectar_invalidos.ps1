# =============================================================================
#  Archivo   : inyectar_invalidos.ps1
#  Propósito : Inyectar 7 filas inválidas en Azure SQL (dev) para probar la
#              cuarentena de silver de punta a punta (05_inyectar_invalidos.py).
#  Uso       : desde la raíz del repo, en PowerShell:  .\fuentes\inyectar_invalidos.ps1
#  Seguridad : la contraseña se pide sin mostrarse, vive solo en esta sesión y
#              se borra al terminar (también si el script falla).
#  Salida    : fuentes/logs/invalidos_<fecha>.json con el id y la regla de cada fila.
# =============================================================================

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

& "$raiz\.venv\Scripts\Activate.ps1"
$env:ANDINA_SQL_USER = "sqladmin_andina"
$s = Read-Host "Contraseña de sqladmin_andina" -AsSecureString
$env:ANDINA_SQL_PASSWORD = (New-Object System.Management.Automation.PSCredential("x", $s)).GetNetworkCredential().Password

try {
    python "$raiz\fuentes\05_inyectar_invalidos.py"
}
finally {
    Remove-Item Env:ANDINA_SQL_PASSWORD -ErrorAction SilentlyContinue
}
