param([string]$DockerPath = 'docker')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (!(Test-Path -LiteralPath '.env')) {
    $secretBytes = New-Object byte[] 24
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($secretBytes)
    $localPassword = [Convert]::ToHexString($secretBytes)
    (Get-Content -LiteralPath '.env.example' -Raw).Replace('replace-with-a-local-password', $localPassword) | Set-Content -LiteralPath '.env'
}
& $DockerPath info
if ($LASTEXITCODE -ne 0) { throw 'Docker engine is unavailable. Start Docker Desktop with Linux containers.' }
& $DockerPath compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed.' }
& $DockerPath compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Stack startup failed. Inspect docker compose logs.' }
Write-Output 'Dashboard: http://localhost:8501 | Spark: http://localhost:8080 | HDFS: http://localhost:9870'
