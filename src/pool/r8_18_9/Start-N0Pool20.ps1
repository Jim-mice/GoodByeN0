param(
  [Parameter(Mandatory=$true)][ValidateSet('ValidateOnly','Start','Status','Stop')][string]$Action
)
$ErrorActionPreference='Stop'
$Python='D:\projects\kernelagent\runtime\.venv_n0_r6\Scripts\python.exe'
$Controller=Join-Path $PSScriptRoot 'pool20_r8_controller.py'
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw 'PINNED_RUNTIME_MISSING' }
if (-not (Test-Path -LiteralPath $Controller -PathType Leaf)) { throw 'POOL20_CONTROLLER_MISSING' }
$Subcommand=switch ($Action) {
  'ValidateOnly' { 'validate' }
  'Start' { 'start' }
  'Status' { 'status' }
  'Stop' { 'stop' }
}
$Arguments=@('-u',$Controller,$Subcommand)
& $Python @Arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
