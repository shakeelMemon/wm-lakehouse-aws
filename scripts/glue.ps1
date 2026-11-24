function Invoke-GlueJob($name) {
  $env:AWS_DEFAULT_REGION = "ca-central-1"
  $id = aws glue start-job-run --job-name $name --query JobRunId --output text
  Write-Host "$name started: $id"
  do {
    Start-Sleep 20
    $s = aws glue get-job-run --job-name $name --run-id $id --query JobRun.JobRunState --output text
    Write-Host "  $(Get-Date -Format T)  $s"
  } while ($s -in "STARTING","RUNNING","STOPPING","WAITING")
  if ($s -ne "SUCCEEDED") {
    aws glue get-job-run --job-name $name --run-id $id --query JobRun.ErrorMessage --output text
    throw "$name ended in $s"
  }
}
