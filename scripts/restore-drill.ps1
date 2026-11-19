$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot\..
$env:PATH = "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;" + $env:PATH
docker compose exec -T postgres bash /lab-scripts/restore-drill.sh | Set-Content -Encoding UTF8 artifacts/restore-verification.json
if ($LASTEXITCODE -ne 0) { throw 'Restore drill failed' }
docker compose cp postgres:/tmp/learning-warehouse.dump artifacts/warehouse.dump
Get-Content artifacts/restore-verification.json
