# Points the EW-vs-delta-dotnet benchmarks at an Azure storage account, for THIS PowerShell session only.
#
# Dot-source it so the variables land in your session (and so in BenchmarkDotNet's child processes):
#
#     . .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\azure-env.ps1
#     . .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\azure-env.ps1 -Account mystorageacct -Container bench
#
# The account key is read with a masked prompt, so it never appears in the command line or the shell history.
# Nothing is written to disk. Run with -Clear to remove the variables again (and go back to local disk).
param(
    [string] $Account,
    [string] $Container = 'ew-vs-delta-dotnet',
    [string] $Prefix = 'bench',
    [switch] $Clear
)

$names = 'EWDD_STORAGE', 'EWDD_AZURE_ACCOUNT', 'EWDD_AZURE_KEY', 'EWDD_AZURE_CONTAINER', 'EWDD_AZURE_PREFIX', 'EWDD_AZURE_EMULATOR'
if ($Clear) {
    foreach ($name in $names) { Remove-Item "Env:$name" -ErrorAction SilentlyContinue }
    Write-Host 'Benchmark storage reset to local disk.'
    return
}

if (-not $Account) { $Account = Read-Host 'Storage account name' }
$secure = Read-Host 'Storage account key' -AsSecureString
$key = [System.Net.NetworkCredential]::new('', $secure).Password
if (-not $Account -or -not $key) { throw 'Both the account name and the key are required.' }

$env:EWDD_STORAGE = 'azure'
$env:EWDD_AZURE_ACCOUNT = $Account
$env:EWDD_AZURE_KEY = $key
$env:EWDD_AZURE_CONTAINER = $Container
$env:EWDD_AZURE_PREFIX = $Prefix
Remove-Item Env:EWDD_AZURE_EMULATOR -ErrorAction SilentlyContinue

Write-Host "Benchmarks will use https://$Account.blob.core.windows.net/$Container/$Prefix/ (key set for this session)."
