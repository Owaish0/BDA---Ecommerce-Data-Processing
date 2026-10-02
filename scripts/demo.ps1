param(
    [ValidateSet('Status', 'Pause', 'Resume', 'Burst', 'Duplicates', 'Invalid', 'Late', 'Batch', 'Restart')]
    [string]$Action = 'Status',
    [string]$DockerPath = ''
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (!$DockerPath) {
    $dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCommand) { $DockerPath = $dockerCommand.Source }
    else {
        $dockerCandidates = @(
            "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe",
            "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
        )
        $DockerPath = $dockerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    }
}
if (!$DockerPath) { throw 'Docker was not found. Supply -DockerPath with its full executable path.' }
function Invoke-ProjectDocker {
    param([string[]]$DockerArguments)
    & $DockerPath @DockerArguments
    if ($LASTEXITCODE -ne 0) { throw "Docker action failed with exit code $LASTEXITCODE" }
}
switch ($Action) {
    'Status' { Invoke-ProjectDocker @('compose', 'ps') }
    'Pause' { Invoke-ProjectDocker @('compose', 'stop', 'producer') }
    'Resume' { Invoke-ProjectDocker @('compose', 'start', 'producer') }
    'Restart' { Invoke-ProjectDocker @('compose', 'restart', 'streaming') }
    'Batch' { Invoke-ProjectDocker @('compose', 'run', '--rm', '--no-deps', 'batch') }
    default {
        Invoke-ProjectDocker @('compose', 'stop', 'producer')
        $scenario = $Action.ToLowerInvariant()
        Invoke-ProjectDocker @('compose', 'run', '--rm', '--no-deps', 'producer', 'python', '-m',
            'ecommerce.producer', '--rate', '100', '--count', '2000', '--scenario', $scenario)
        Write-Output 'Finite scenario finished. Allow streaming to catch up. Use -Action Resume to restart continuous traffic.'
    }
}
