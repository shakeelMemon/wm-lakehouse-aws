# setup-athena-dsn.ps1 - create the Athena ODBC DSN that Power BI Desktop uses
#
# Why a script: on Windows on ARM, "ODBC Data Sources (64-bit)" is an ARM64 program and cannot
# load the x64-only Athena driver's setup dialog. Power BI Desktop runs as x64 under emulation and
# reads the same registry, so writing the DSN directly to the registry works.
#
# Usage (normal PowerShell window, no admin needed for a User DSN):
#     .\scripts\setup-athena-dsn.ps1                 # dev, profile wm-powerbi
#     .\scripts\setup-athena-dsn.ps1 -Env prod -AwsProfile wm-powerbi-prod

param(
    [string]$Env = "dev",
    [string]$AwsProfile = "wm-powerbi",
    [string]$DsnName = ""
)

if (-not $DsnName) { $DsnName = "WM-Athena-$Env" }
$account = (aws sts get-caller-identity --profile $AwsProfile --query Account --output text)
if (-not $account) { throw "Could not read the AWS account ID for profile $AwsProfile." }
$driverName = "Amazon Athena ODBC (x64)"

$drvKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\$driverName"
if (-not (Test-Path $drvKey)) {
    throw "Driver '$driverName' not found. Install the Amazon Athena ODBC 2.x driver (.msi) first."
}
$driverPath = (Get-ItemProperty $drvKey).Driver

$dsnKey = "HKCU:\Software\ODBC\ODBC.INI\$DsnName"
if (-not (Test-Path $dsnKey)) { New-Item -Path $dsnKey | Out-Null }

$props = [ordered]@{
    Driver             = $driverPath
    Description        = "WM lakehouse ($Env) - semantic layer for Power BI"
    AwsRegion          = "ca-central-1"
    Catalog            = "AwsDataCatalog"
    Schema             = "wm_semantic"
    Workgroup          = "wm-$Env-analytics"
    S3OutputLocation   = "s3://wm-$Env-athena-$account/results/"
    AuthenticationType = "IAM Profile"
    AWSProfile         = $AwsProfile
}
foreach ($k in $props.Keys) {
    New-ItemProperty -Path $dsnKey -Name $k -Value $props[$k] -PropertyType String -Force | Out-Null
}

$listKey = "HKCU:\Software\ODBC\ODBC.INI\ODBC Data Sources"
if (-not (Test-Path $listKey)) { New-Item -Path $listKey | Out-Null }
New-ItemProperty -Path $listKey -Name $DsnName -Value $driverName -PropertyType String -Force | Out-Null

Write-Host "User DSN '$DsnName' created:" -ForegroundColor Green
Get-ItemProperty $dsnKey | Select-Object Driver, AwsRegion, Schema, Workgroup, S3OutputLocation, AuthenticationType, AWSProfile |
    Format-List
