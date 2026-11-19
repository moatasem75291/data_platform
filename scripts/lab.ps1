param([ValidateSet('start','stop','status','full','sample','notebook','notebook-full','bi','cdc','orchestration','monitoring','verify','lock')][string]$Action='start')
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot\..
$labBin = "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin"
if (Test-Path $labBin) {$env:PATH = "$labBin;" + $env:PATH}
switch ($Action) {
 'start' { docker compose up -d --build postgres seaweedfs notebook trino }
 'stop' { docker compose --profile bi --profile cdc --profile orchestration --profile monitoring stop }
 'status' { docker compose --profile bi --profile cdc --profile orchestration --profile monitoring ps }
 'sample' { docker compose exec -T notebook python -m lab.cli run --limit 50000 }
 'full' { docker compose exec -T notebook python -m lab.cli run --full }
 'notebook' { docker compose exec -T notebook jupyter nbconvert --to notebook --execute data_platform_architecture_lab.ipynb --output data_platform_architecture_lab.executed.ipynb --ExecutePreprocessor.timeout=1800 }
 'notebook-full' { docker compose exec -T -e LAB_FULL=1 notebook jupyter nbconvert --to notebook --execute data_platform_architecture_lab.ipynb --output data_platform_architecture_lab.executed.ipynb --ExecutePreprocessor.timeout=1800 }
 'bi' { docker compose --profile bi up -d --build superset }
 'cdc' { docker compose --profile cdc up -d kafka connect }
 'orchestration' { docker compose --profile orchestration up -d airflow }
 'monitoring' { docker compose --profile monitoring up -d prometheus grafana }
 'verify' { docker compose exec -T notebook python -m lab.cli exercises }
 'lock' { docker compose exec -T notebook python scripts/lock-runtime.py }
}
if ($LASTEXITCODE -ne 0) {throw "Lab action failed: $Action"}
