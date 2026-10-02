# Run manually or from the chat heartbeat. Only reviewed project paths are staged.
param([string]$Message = 'Checkpoint verified CS404 project progress')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
git add src jobs dashboard docker db scripts tests docs .github .gitignore .dockerignore pyproject.toml compose.yaml .env.example README.md
if ($LASTEXITCODE -ne 0) { throw 'Staging failed' }
git diff --cached --quiet
if ($LASTEXITCODE -eq 1) {
    git commit -m $Message
    if ($LASTEXITCODE -ne 0) { throw 'Commit failed' }
}
git push origin main
if ($LASTEXITCODE -ne 0) { throw 'Push failed. Check GitHub authentication and repository permissions.' }
