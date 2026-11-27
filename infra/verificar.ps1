# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
# Verificacion del stack principal con la salida de cada paso en un archivo (logs/verificacion/, gitignored),
# para revisarla completa despues en vez de copiar y pegar de la consola. En pantalla solo sale el resumen.
# Uso, en la raiz del repo:
#   pwsh -File infra/verificar.ps1                              # up, wait, status, health, bi
#   pwsh -File infra/verificar.ps1 -Steps run,status,bi         # solo esos pasos, en ese orden
#   pwsh -File infra/verificar.ps1 -Steps replica               # replica en un clon limpio (ver abajo)
# Pasos: up (levanta y espera a que esten sanos), wait (pipeline automatico), run (lakehouse_full a mano),
# status, health, bi (las graficas del tablero), pytest (pruebas con Spark), replica (apaga el stack
# principal SIN -v, corre infra/replica.ps1 en -ReplicaDir y lo vuelve a levantar).
# Resultado: logs/verificacion/RESUMEN.log (estado, minutos y ultimas lineas de cada paso), un log por paso
# y los logs de docker de cada servicio durante la verificacion.
param(
    # Texto separado por comas: con `pwsh -File` un arreglo llega como un solo argumento.
    [string]$Steps = 'up,wait,status,health,bi',
    [string]$ReplicaDir = 'C:\tmp\lakehouse-replica'
)

$ErrorActionPreference = 'Stop'
$valid = 'up', 'wait', 'run', 'status', 'health', 'bi', 'pytest', 'replica'
$stepList = @($Steps -split '[,\s]+' | Where-Object { $_ })
$unknown = @($stepList | Where-Object { $_ -notin $valid })
if ($unknown) { throw "Pasos desconocidos: $($unknown -join ', '). Validos: $($valid -join ', ')" }
$env:PYTHONIOENCODING = 'utf-8'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root 'logs\verificacion'
$services = 'lakehouse', 'superset', 'mlflow', 'minio'
Set-Location $root

function Assert-Exit([string]$What) {
    if ($LASTEXITCODE -ne 0) { throw "$What termino con codigo $LASTEXITCODE" }
}

function Wait-Healthy([string]$Service, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $id = docker compose ps -q $Service
        $health = if ($id) { docker inspect --format '{{.State.Health.Status}}' $id } else { 'sin contenedor' }
        if ($health -eq 'healthy') { Write-Output "$Service sano"; return }
        Write-Output "Esperando a $Service ($health)..."
        Start-Sleep -Seconds 10
    }
    throw "$Service no quedo sano en $TimeoutSeconds s"
}

function Invoke-InContainer([string]$Service, [string]$What, [string[]]$Command) {
    docker compose exec -T $Service @Command
    Assert-Exit $What
}

function Test-ReplicaClone {
    if (-not (Test-Path (Join-Path $ReplicaDir '.git'))) {
        throw "No existe el clon en $ReplicaDir. Crealo con: git clone $(git remote get-url origin) $ReplicaDir"
    }
    $dirty = git status --porcelain
    if ($dirty) { throw "Tu repo tiene cambios sin commit: la replica no los veria. Haz commit y push primero." }
    $head = git rev-parse HEAD
    $cloneHead = git -C $ReplicaDir rev-parse HEAD
    if ($head -ne $cloneHead) {
        throw "El clon esta en $($cloneHead.Substring(0, 8)) y tu repo en $($head.Substring(0, 8)) (o falta push): borra $ReplicaDir y vuelve a clonar"
    }
    if (Test-Path (Join-Path $ReplicaDir '.env')) { throw "El clon ya se uso (tiene .env): borra $ReplicaDir y vuelve a clonar" }
}

$actions = @{
    up      = {
        docker compose up -d --build
        Assert-Exit 'docker compose up'
        Wait-Healthy lakehouse 900
        Wait-Healthy superset 900
    }
    wait    = { Invoke-InContainer lakehouse 'wait' @('python', '-m', 'lakehouse.orchestration', 'wait') }
    run     = { Invoke-InContainer lakehouse 'run' @('python', '-m', 'lakehouse.orchestration', 'run', '--job', 'lakehouse_full') }
    status  = { Invoke-InContainer lakehouse 'status' @('python', '-m', 'lakehouse.orchestration', 'status', '--limit', '12') }
    health  = { Invoke-InContainer lakehouse 'health' @('python', '-m', 'lakehouse.orchestration', 'health') }
    bi      = { Invoke-InContainer superset 'check.py' @('python', '/opt/lakehouse/bi/check.py') }
    pytest  = { Invoke-InContainer lakehouse 'pytest' @('pytest', '-q') }
    replica = {
        Test-ReplicaClone
        docker compose down
        Assert-Exit 'docker compose down'
        Push-Location $ReplicaDir
        try {
            pwsh -NoProfile -File infra/replica.ps1
            Assert-Exit 'replica'
        } finally {
            Pop-Location
            docker compose up -d
        }
    }
}

function Invoke-Step([int]$Index, [string]$Name) {
    $file = Join-Path $logDir ('{0:d2}_{1}.log' -f $Index, $Name)
    Write-Host ("== {0} (log: {1})" -f $Name, [IO.Path]::GetRelativePath($root, $file))
    $t0 = Get-Date
    $writer = [IO.StreamWriter]::new($file, $false, [Text.UTF8Encoding]::new($false))
    $writer.AutoFlush = $true
    $ok = $true
    try {
        # Continue: el stderr de docker y Python va al log sin tumbar el paso; los fallos se detectan por codigo.
        $ErrorActionPreference = 'Continue'
        & $actions[$Name] *>&1 | ForEach-Object { $writer.WriteLine("$_") }
    } catch {
        $ok = $false
        $writer.WriteLine("ERROR: $_")
    } finally {
        $writer.Dispose()
        $ErrorActionPreference = 'Stop'
    }
    $tail = Get-Content $file -Tail 12 -Encoding utf8
    [pscustomobject]@{
        Name    = $Name
        Ok      = $ok
        Minutes = [math]::Round(((Get-Date) - $t0).TotalMinutes, 1)
        File    = $file
        Tail    = $tail
    }
}

if (Test-Path $logDir) { Remove-Item -Recurse -Force $logDir }
New-Item -ItemType Directory -Force $logDir | Out-Null
$started = Get-Date
$head = (git rev-parse --short HEAD)
$results = @()
$index = 1
foreach ($name in $stepList) {
    $result = Invoke-Step $index $name
    $results += $result
    $index++
    $mark = if ($result.Ok) { 'OK   ' } else { 'FALLO' }
    Write-Host ("   {0} en {1} min" -f $mark, $result.Minutes) -ForegroundColor ($result.Ok ? 'Green' : 'Red')
    if (-not $result.Ok -and $name -eq 'up') { Write-Host 'Sin stack no tiene caso seguir.'; break }
}

# Logs de docker de toda la verificacion (los pasos del daemon de Dagster no salen en la salida de exec).
$since = $started.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
foreach ($svc in $services) {
    docker compose logs --no-color --since $since $svc *>&1 |
        ForEach-Object { "$_" } |
        Out-File -FilePath (Join-Path $logDir "docker_$svc.log") -Encoding utf8
}

$failed = @($results | Where-Object { -not $_.Ok })
$summary = @("Verificacion $($started.ToString('yyyy-MM-dd HH:mm')) | commit $head | pasos: $($stepList -join ', ')", '')
$summary += $results | ForEach-Object { '{0,-6} {1,-8} {2,6} min  {3}' -f ($_.Ok ? 'OK' : 'FALLO'), $_.Name, $_.Minutes, (Split-Path -Leaf $_.File) }
$summary += '', 'Ultimas lineas de cada paso:'
foreach ($r in $results) { $summary += "--- $($r.Name)"; $summary += $r.Tail }
$summary += '', ($failed ? "FALLARON: $(($failed | ForEach-Object Name) -join ', ')" : 'TODO OK')
$summary | Out-File -FilePath (Join-Path $logDir 'RESUMEN.log') -Encoding utf8

Write-Host ''
Write-Host ($failed ? "FALLARON: $(($failed | ForEach-Object Name) -join ', ')" : 'TODO OK') -ForegroundColor ($failed ? 'Red' : 'Green')
Write-Host "Resumen y logs completos en $logDir"
exit ($failed ? 1 : 0)
