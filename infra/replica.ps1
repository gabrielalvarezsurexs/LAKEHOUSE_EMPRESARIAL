# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
# Prueba de replica (F8): desde un clon limpio y sin datos reales, levanta todo y lo verifica de punta a punta,
# como lo haria el profe con el README. Sin pasos manuales: el pipeline lo disparan los sensores de Dagster.
# Uso, en la raiz de un clon limpio y con el stack principal apagado (docker compose down, SIN -v):
#   pwsh -File infra/replica.ps1          # al final borra todo lo de la replica
#   pwsh -File infra/replica.ps1 -Keep    # lo deja arriba para revisarlo
# Usa el proyecto de compose "lakehouse-replica": sus volumenes no tocan los de los datos reales.
param([switch]$Keep, [int]$Rows = 3000)

$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"
$project = "lakehouse-replica"
$started = Get-Date

function Write-Step([string]$Message) { Write-Host "`n== $Message" -ForegroundColor Cyan }

function Invoke-Checked([string]$What, [scriptblock]$Block) {
    & $Block
    if ($LASTEXITCODE -ne 0) { throw "$What fallo (codigo $LASTEXITCODE)" }
}

function New-Secret {
    $bytes = New-Object byte[] 24
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    -join ($bytes | ForEach-Object { $_.ToString('x2') })
}

function Test-PortBusy([int]$Port) {
    $client = New-Object Net.Sockets.TcpClient
    try { return $client.ConnectAsync('127.0.0.1', $Port).Wait(300) } catch { return $false } finally { $client.Dispose() }
}

function Wait-Healthy([string]$Service, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $id = docker compose -p $project ps -q $Service
        $health = if ($id) { docker inspect --format '{{.State.Health.Status}}' $id } else { 'sin contenedor' }
        if ($health -eq 'healthy') { Write-Host "$Service sano"; return }
        Write-Host "Esperando a $Service ($health)..."
        Start-Sleep -Seconds 15
    }
    throw "$Service no quedo sano en $TimeoutSeconds s"
}

if (-not (Test-Path docker-compose.yml)) { throw "Corre esto en la raiz del clon (donde esta docker-compose.yml)" }
if (Test-Path .env) { throw ".env ya existe: usa un clon limpio, no tu copia de trabajo" }
$busy = 9000, 9001, 3300, 5000, 8088 | Where-Object { Test-PortBusy $_ }
if ($busy) { throw "Puertos ocupados ($($busy -join ', ')): apaga el stack principal con 'docker compose down' (sin -v)" }

$ok = $false
try {
    Write-Step "1. .env desde .env.example (contrasenas aleatorias y datos sinteticos)"
    $text = Get-Content .env.example -Raw
    $text = $text -replace '(?m)^MINIO_ROOT_PASSWORD=.*$', "MINIO_ROOT_PASSWORD=$(New-Secret)"
    $text = $text -replace '(?m)^SPOTIFY_DATA_DIR=.*$', 'SPOTIFY_DATA_DIR=./sinteticos'
    $text = $text -replace '(?m)^GENRES_SOURCE=.*$', 'GENRES_SOURCE=synthetic'
    $secrets = @('INGESTION', 'TRANSFORMATION', 'GOVERNANCE', 'ML', 'BI' | ForEach-Object { "MINIO_SECRET_$_=$(New-Secret)" })
    $secrets += "IP_HASH_SALT=$(New-Secret)", "SUPERSET_SECRET_KEY=$(New-Secret)", "SUPERSET_ADMIN_PASSWORD=$(New-Secret)"
    [IO.File]::WriteAllText("$PWD\.env", $text.TrimEnd() + "`n" + ($secrets -join "`n") + "`n")
    Write-Host ".env listo (sin imprimir secretos)"

    Write-Step "2. Dependencias, lint y pruebas sin Docker"
    Invoke-Checked "uv sync" { uv sync --frozen }
    Invoke-Checked "ruff" { uv run ruff check . }
    Invoke-Checked "pytest" { uv run pytest -q }

    Write-Step "3. Datos sinteticos ($Rows filas, mismo esquema que los reales)"
    Invoke-Checked "sinteticos" { uv run python -m lakehouse.ingestion synthetic --out sinteticos --rows $Rows --files 2 }

    Write-Step "4. Stack arriba (la primera vez construye las imagenes)"
    Invoke-Checked "docker compose up" { docker compose -p $project up -d --build }
    Wait-Healthy lakehouse 900
    Wait-Healthy superset 900

    Write-Step "5. Pipeline automatico: los sensores de Dagster corren todo solos"
    Invoke-Checked "pipeline" { docker compose -p $project exec -T lakehouse python -m lakehouse.orchestration wait --timeout 3600 }
    Invoke-Checked "status" { docker compose -p $project exec -T lakehouse python -m lakehouse.orchestration status }
    Invoke-Checked "salud" { docker compose -p $project exec -T lakehouse python -m lakehouse.orchestration health }

    Write-Step "6. Tablero: cada grafica contra el Oro sintetico"
    Invoke-Checked "tablero" { docker compose -p $project exec -T superset python /opt/lakehouse/bi/check.py }

    Write-Step "7. Pruebas con Spark dentro del contenedor"
    Invoke-Checked "pytest en el contenedor" { docker compose -p $project exec -T lakehouse pytest -q }

    $ok = $true
    $minutes = [math]::Round(((Get-Date) - $started).TotalMinutes, 1)
    Write-Host "`nREPLICA OK en $minutes min: Dagster http://localhost:3300, MLflow http://localhost:5000, Superset http://localhost:8088" -ForegroundColor Green
}
finally {
    if (-not $ok) {
        Write-Host "`nREPLICA FALLO. Ultimos logs:" -ForegroundColor Red
        docker compose -p $project logs --no-color --tail 60
    }
    if ($Keep) {
        Write-Host "Stack de la replica arriba (-Keep). Para borrarlo: docker compose -p $project down -v"
    } else {
        docker compose -p $project down -v
    }
}
