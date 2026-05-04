Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

Push-Location $repoRoot
try {
    $env:DELPHI_DEPLOYMENT_ENV = 'development'
    $env:DELPHI_RUNTIME_TARGET = 'hosted'
    $env:APP_HOST = '127.0.0.1'
    $env:APP_PORT = '5015'
    $env:APP_DISPLAY_NAME = 'Delphi 8.0.7'
    $env:APP_PAGE_KICKER = 'Delphi 8.0.7'
    $env:APP_VERSION_LABEL = 'Version 8.0.7'
    $env:SESSION_COOKIE_NAME = 'delphi5_hosted_session'
    $env:OAUTH_SESSION_NAMESPACE = 'delphi5hosted'
    $env:HOSTED_PUBLIC_BASE_URL = 'https://127.0.0.1:5015'
    $env:SCHWAB_REDIRECT_URI = 'https://127.0.0.1:5015/callback'
    Write-Host 'Delphi local startup | runtime=hosted-development | url=https://127.0.0.1:5015/hosted'
    & $pythonExe app.py
}
finally {
    Pop-Location
}