# #878 on Azure: upload the foreign ~1 GB row-group tables once, then scan each with the fixed build and (optionally)
# a build from before the fix, one table and config per process so every peak is the scan's own.
#
#     .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\rg878-azure.ps1 -Account <name> [-Before <exe>] [-Tables fat_str,huge_str]
#
# Prompts for the account key (masked; see azure-env.ps1). Results go to the console and to rg878-azure-<time>.tsv
# in the current directory. Uploading all four tables is ~5.8 GB; later runs skip tables already uploaded.
param(
    [Parameter(Mandatory)] [string] $Account,
    [string] $Container = 'ew-vs-delta-dotnet',
    [string] $Prefix = 'bench',
    [string] $LocalRoot = (Join-Path $env:TEMP 'ew-rg878'),
    [string[]] $Tables = @('fat_str', 'skew_str', 'wide_num', 'huge_str'),
    [string[]] $Configs = @('ew-default', 'ew-serial', 'ew-256MiB', 'dd-select*'),
    [string] $Before,
    [int] $Repeats = 2
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$after = Join-Path $here 'bin\Release\net10.0\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks.exe'
if (-not (Test-Path $after)) { throw "Build first: dotnet build -c Release $here" }

. (Join-Path $here 'azure-env.ps1') -Account $Account -Container $Container -Prefix $Prefix
try {
    & $after --rg878-upload $LocalRoot ($Tables -join ',')
    if ($LASTEXITCODE -ne 0) { throw 'upload failed' }

    $out = "rg878-azure-$(Get-Date -Format yyyyMMdd-HHmmss).tsv"
    "build`ttable`tconfig`tseconds`trows`tbatches`tmaxBatchRows`tmaxBatchMiB`tpeakPrivateMiB`terror" | Tee-Object -FilePath $out
    $builds = [ordered]@{ after = $after }
    if ($Before) { $builds['before'] = $Before }
    foreach ($table in $Tables) {
        foreach ($config in $Configs) {
            foreach ($build in $builds.Keys) {
                # DataFusion does not depend on EW's build: run it once.
                if ($config -like 'dd-*' -and $build -eq 'before') { continue }
                for ($i = 0; $i -lt $Repeats; $i++) {
                    $line = & $builds[$build] --rg878 - $table $config | Select-Object -Last 1
                    "$build`t$line" | Tee-Object -FilePath $out -Append
                }
            }
        }
    }
    Write-Host "Results in $out"
}
finally {
    . (Join-Path $here 'azure-env.ps1') -Clear
}
