# wm-support.ps1 - support helpers for the WM lakehouse (Glue, Athena, CloudWatch Logs)
#
# Load into the current PowerShell window (from the repo root):
#     . .\scripts\wm-support.ps1            # dev (default)
#     . .\scripts\wm-support.ps1 -Env prod  # another environment
#
# Commands
#     Get-WmStatus                          last run of every job
#     Invoke-WmJob gold-build               start a job and wait for it
#     Invoke-WmJob silver-build @{ '--tables' = 'transactions' }
#     Invoke-WmPipeline                     daily order: landing -> bronze -> silver -> gold -> semantic + health
#     Invoke-WmPipeline -From silver-build  restart the chain from a failed step
#     Get-WmError gold-build                error message of the latest run
#     Get-WmLog gold-build                  error lines from CloudWatch for the latest run
#     Get-WmLog gold-build -All             every log line for the latest run
#     Invoke-WmAthena "SELECT ..."          run a query and print the result as a table
#     Get-WmHealth                          latest data-health results (FAIL rows first)

param([string]$Env = "dev")

$script:WmEnv = $Env
$script:WmWorkgroup = "wm-$Env-analytics"
$env:AWS_DEFAULT_REGION = "ca-central-1"

$script:WmJobs = @("generate-landing", "bronze-ingest", "silver-build", "gold-build",
                   "publish-semantic", "data-health", "incremental-impact")

function Resolve-WmJobName([string]$Name) {
    if ($Name -like "wm-*") { return $Name }
    return "wm-$script:WmEnv-$Name"
}

function Get-WmLatestRunId([string]$Job) {
    $j = Resolve-WmJobName $Job
    $id = aws glue get-job-runs --job-name $j --max-items 1 --query "JobRuns[0].Id" --output text
    if (-not $id -or $id -eq "None") { throw "No runs found for $j" }
    return ($id -split "\s+")[0]
}

function Get-WmStatus {
    foreach ($name in $script:WmJobs) {
        $j = Resolve-WmJobName $name
        $raw = aws glue get-job-runs --job-name $j --max-items 1 --output json
        $run = if ($raw) { (($raw | Out-String) | ConvertFrom-Json).JobRuns | Select-Object -First 1 } else { $null }
        if (-not $run) {
            [pscustomobject]@{ Job = $j; State = "NEVER RUN"; Started = ""; Seconds = ""; RunId = ""; Error = "" }
            continue
        }
        [pscustomobject]@{
            Job     = $j
            State   = $run.JobRunState
            Started = $run.StartedOn
            Seconds = $run.ExecutionTime
            RunId   = $run.Id
            Error   = $run.ErrorMessage
        }
    }
}

function Invoke-WmJob([string]$Name, [hashtable]$Arguments) {
    $j = Resolve-WmJobName $Name
    if ($Arguments -and $Arguments.Count -gt 0) {
        # Pass arguments through a file so PowerShell 5.1 does not strip the JSON quotes
        $tmp = Join-Path $env:TEMP "wm-glue-args.json"
        [IO.File]::WriteAllText($tmp, ($Arguments | ConvertTo-Json -Compress))
        $id = aws glue start-job-run --job-name $j --arguments "file://$tmp" --query JobRunId --output text
    } else {
        $id = aws glue start-job-run --job-name $j --query JobRunId --output text
    }
    if (-not $id) { throw "Could not start $j (see the error above)" }
    Write-Host "$j started: $id"
    do {
        Start-Sleep 20
        $s = aws glue get-job-run --job-name $j --run-id $id --query JobRun.JobRunState --output text
        Write-Host "  $(Get-Date -Format T)  $s"
    } while ($s -in "STARTING", "RUNNING", "STOPPING", "WAITING")
    if ($s -ne "SUCCEEDED") {
        $err = aws glue get-job-run --job-name $j --run-id $id --query JobRun.ErrorMessage --output text
        Write-Host "ERROR: $err" -ForegroundColor Red
        Write-Host "Next: Get-WmLog $Name -RunId $id" -ForegroundColor Yellow
        throw "$j ended in $s"
    }
}

# Alias kept for older notes
function Invoke-GlueJob($name) { Invoke-WmJob $name }

function Invoke-WmPipeline([string]$From = "generate-landing", [switch]$WithImpact) {
    $order = @("generate-landing", "bronze-ingest", "silver-build", "gold-build", "publish-semantic", "data-health")
    $start = [Array]::IndexOf($order, $From)
    if ($start -lt 0) { throw "Unknown step '$From'. Use one of: $($order -join ', ')" }
    foreach ($step in $order[$start..($order.Count - 1)]) { Invoke-WmJob $step }
    if ($WithImpact -or (Get-Date).DayOfWeek -eq "Monday") { Invoke-WmJob "incremental-impact" }
    Write-Host "Pipeline finished." -ForegroundColor Green
}

function Get-WmError([string]$Job, [string]$RunId) {
    $j = Resolve-WmJobName $Job
    if (-not $RunId) { $RunId = Get-WmLatestRunId $Job }
    aws glue get-job-run --job-name $j --run-id $RunId `
        --query "JobRun.{State:JobRunState,Started:StartedOn,Seconds:ExecutionTime,Error:ErrorMessage}" --output table
}

function Get-WmLog([string]$Job, [string]$RunId, [switch]$All, [int]$Max = 200) {
    if (-not $RunId) { $RunId = Get-WmLatestRunId $Job }
    Write-Host "Run: $RunId" -ForegroundColor Cyan
    $groups = aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs `
        --query "logGroups[].logGroupName" --output text
    $found = $false
    foreach ($g in ($groups -split "\s+" | Where-Object { $_ })) {
        $params = @("logs", "filter-log-events", "--log-group-name", $g,
                    "--log-stream-name-prefix", $RunId, "--max-items", "$Max",
                    "--query", "events[].message", "--output", "json")
        if (-not $All) { $params += @("--filter-pattern", "?ERROR ?Error ?Exception ?Traceback ?FAILED") }
        $raw = & aws @params
        $msgs = if ($raw) { ($raw | Out-String) | ConvertFrom-Json } else { @() }
        if ($msgs) {
            $found = $true
            Write-Host "== $g" -ForegroundColor Cyan
            $msgs | ForEach-Object { $_.TrimEnd() }
        }
    }
    if (-not $found) {
        Write-Host "No matching log lines. Try: Get-WmLog $Job -RunId $RunId -All" -ForegroundColor Yellow
    }
}

function Invoke-WmAthena([string]$Sql) {
    $q = aws athena start-query-execution --work-group $script:WmWorkgroup --query-string $Sql `
        --query QueryExecutionId --output text
    do {
        Start-Sleep 2
        $st = aws athena get-query-execution --query-execution-id $q --query "QueryExecution.Status.State" --output text
    } while ($st -in "QUEUED", "RUNNING")
    if ($st -ne "SUCCEEDED") {
        $why = aws athena get-query-execution --query-execution-id $q --query "QueryExecution.Status.StateChangeReason" --output text
        Write-Host "$st : $why" -ForegroundColor Red
        return
    }
    $res = aws athena get-query-results --query-execution-id $q --output json | ConvertFrom-Json
    $cols = @($res.ResultSet.Rows[0].Data | ForEach-Object { $_.VarCharValue })
    $res.ResultSet.Rows | Select-Object -Skip 1 | ForEach-Object {
        $o = [ordered]@{}
        for ($i = 0; $i -lt $cols.Count; $i++) { $o[$cols[$i]] = $_.Data[$i].VarCharValue }
        [pscustomobject]$o
    } | Format-Table -AutoSize
}

function Get-WmHealth {
    Invoke-WmAthena @"
SELECT check_name, severity, status, observed_value, threshold, details, checked_at
FROM wm_ops.dq_check_results
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM wm_ops.dq_check_results)
ORDER BY CASE status WHEN 'FAIL' THEN 0 ELSE 1 END, severity, check_name
"@
}

Write-Host "WM support helpers loaded for env '$script:WmEnv'. Try: Get-WmStatus" -ForegroundColor Green
