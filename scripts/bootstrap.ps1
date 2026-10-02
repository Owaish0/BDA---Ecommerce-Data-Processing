param([string]$DockerPath = '')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (!$DockerPath) {
    $dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCommand) {
        $DockerPath = $dockerCommand.Source
    } else {
        $dockerCandidates = @(
            "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe",
            "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
        )
        $DockerPath = $dockerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    }
}
if (!$DockerPath) { throw 'Docker was not found. Pass -DockerPath with the full path to docker.exe.' }
if (!(Test-Path -LiteralPath '.env')) {
    $secretBytes = New-Object byte[] 24
    $randomGenerator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $randomGenerator.GetBytes($secretBytes)
    $randomGenerator.Dispose()
    $localPassword = [BitConverter]::ToString($secretBytes).Replace('-', '')
    (Get-Content -LiteralPath '.env.example' -Raw).Replace('replace-with-a-local-password', $localPassword) | Set-Content -LiteralPath '.env'
}
& $DockerPath info
if ($LASTEXITCODE -ne 0) { throw 'Docker engine is unavailable. Start Docker Desktop with Linux containers.' }
& $DockerPath compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed.' }
& $DockerPath compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Stack startup failed. Inspect docker compose logs.' }
Write-Output 'Dashboard: http://localhost:8501 | Spark: http://localhost:8080 | HDFS: http://localhost:9870'
