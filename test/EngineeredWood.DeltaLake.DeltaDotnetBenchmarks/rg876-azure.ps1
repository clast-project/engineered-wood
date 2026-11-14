# #876 on Azure: upload #652's row-group-size layouts once, then scan them under several largest-request caps
# (CoalescingOptions.MaxRequestBytes), one process per cap. Rounds interleave the caps, so a slow patch of network
# lands on every cap rather than on one.
#
#     .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\rg876-azure.ps1 -Account <name>
#
# Build the layouts first (dotnet run -c Release -f net10.0 --project test\EngineeredWood.DeltaLake.Benchmarks --
# rg652 build layouts=w100k,w1m,w6m) and this project (dotnet build -c Release). Prompts for the account key (masked;
# see azure-env.ps1). Results go to the console and to rg876-azure-<time>.tsv in the current directory.
param(
    [Parameter(Mandatory)] [string] $Account,
    [string] $Container = 'ew-vs-delta-dotnet',
    [string] $Prefix = 'bench',
    [string] $LocalRoot = (Join-Path $env:TEMP 'ew-rg652'),
    [string[]] $Layouts = @('w100k', 'w1m', 'w6m'),
    [string[]] $CapsMiB = @('64', '16', '8', '4'),
    [int] $Rounds = 3,
    [int] $Repeats = 3
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$exe = Join-Path $here 'bin\Release\net10.0\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks.exe'
if (-not (Test-Path $exe)) { throw "Build first: dotnet build -c Release $here" }

. (Join-Path $here 'azure-env.ps1') -Account $Account -Container $Container -Prefix $Prefix
try {
    & $exe --rg876-upload $LocalRoot ($Layouts -join ',')
    if ($LASTEXITCODE -ne 0) { throw 'upload failed' }

    $out = "rg876-azure-$(Get-Date -Format yyyyMMdd-HHmmss).tsv"
    "round`tcapMiB`tlayout`tquery`tbest_s`tmedian_s`trows" | Tee-Object -FilePath $out
    for ($round = 1; $round -le $Rounds; $round++) {
        foreach ($cap in $CapsMiB) {
            $env:EWDD_AZURE_MAX_REQUEST_MIB = $cap
            & $exe --rg876 ($Layouts -join ',') 'all,q1' $Repeats | ForEach-Object { "$round`t$_" | Tee-Object -FilePath $out -Append }
            if ($LASTEXITCODE -ne 0) { throw "scan failed at cap $cap" }
        }
    }
    Write-Host "Results in $out"
}
finally {
    Remove-Item Env:EWDD_AZURE_MAX_REQUEST_MIB -ErrorAction SilentlyContinue
    . (Join-Path $here 'azure-env.ps1') -Clear
}
