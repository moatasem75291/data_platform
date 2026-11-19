$ErrorActionPreference='Stop'
Set-Location $PSScriptRoot\..
$labValues=@{}
Get-Content .env | ForEach-Object {if ($_ -match '^([^#=]+)=(.*)$') {$labValues[$matches[1]]=$matches[2]}}
Start-Process ('http://localhost:18888/lab/tree/data_platform_architecture_lab.ipynb?token='+$labValues['JUPYTER_TOKEN'])
