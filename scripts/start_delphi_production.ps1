Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $pythonExe)) {
    $pythonExe = Join-Path $repoRoot 'venv\Scripts\python.exe'
}

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

Push-Location $repoRoot
try {
    $env:DELPHI_RUNTIME_TARGET = 'hosted'
    if (-not $env:APP_HOST) {
        $env:APP_HOST = '0.0.0.0'
    }
    if (-not $env:APP_PORT) {
        $env:APP_PORT = '10000'
    }
    $env:HOSTED_PUBLIC_BASE_URL = 'https://eigeltrade.com'
    $env:SCHWAB_REDIRECT_URI = ''
    $env:APP_DISPLAY_NAME = 'Delphi 8.0.7'
    $env:APP_PAGE_KICKER = 'Delphi 8.0.7'
    $env:APP_VERSION_LABEL = 'Version 8.0.7'
    $env:SESSION_COOKIE_NAME = 'delphi5_hosted_session'
    $env:OAUTH_SESSION_NAMESPACE = 'delphi5hosted'
    & $pythonExe app.py
}
finally {
    Pop-Location
}