param([Parameter(Mandatory=$true)][ValidateSet('Status','Start','Stop')][string]$Action)
$ErrorActionPreference='Stop'
if ($Action -eq 'Start') { throw 'OJ_SERVICE_START_IS_ATOMIC_WITH_POOL20_START_USE_Start-N0Pool20.ps1' }
$Entry=Join-Path $PSScriptRoot 'Start-N0Pool20.ps1'
if (-not (Test-Path -LiteralPath $Entry -PathType Leaf)) { throw 'POOL20_ENTRYPOINT_MISSING' }
if ($Action -eq 'Status') { & $Entry -Action Status; exit $LASTEXITCODE }
& $Entry -Action Stop
