$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ConfiguredHadoopHome = $env:HADOOP_HOME
$ProfileHadoopHome = Join-Path $env:USERPROFILE "hadoop"
$HadoopCandidates = [System.Collections.Generic.List[string]]::new()

if ($ConfiguredHadoopHome) {
    $HadoopCandidates.Add($ConfiguredHadoopHome)
}
if (-not $ConfiguredHadoopHome -or $ConfiguredHadoopHome -ne $ProfileHadoopHome) {
    $HadoopCandidates.Add($ProfileHadoopHome)
}

$HadoopHome = $null
foreach ($CandidateHome in $HadoopCandidates) {
    $CandidateBin = Join-Path $CandidateHome "bin"
    $HasWinutils = Test-Path (Join-Path $CandidateBin "winutils.exe") -PathType Leaf
    $HasHadoopDll = Test-Path (Join-Path $CandidateBin "hadoop.dll") -PathType Leaf

    if ($HasWinutils -and $HasHadoopDll) {
        $HadoopHome = $CandidateHome
        break
    }
}

if (-not $HadoopHome) {
    $HadoopHome = if ($ConfiguredHadoopHome) {
        $ConfiguredHadoopHome
    }
    else {
        $ProfileHadoopHome
    }
}

$HadoopBin = Join-Path $HadoopHome "bin"
$env:HADOOP_HOME = $HadoopHome
if (($env:PATH -split ";") -notcontains $HadoopBin) {
    $env:PATH = "$HadoopBin;$env:PATH"
}

$Failures = [System.Collections.Generic.List[string]]::new()

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        $Failures.Add("Command not found: $Name")
        return $false
    }
    return $true
}

Write-Host "Checking local PySpark environment..." -ForegroundColor Cyan

if ($ConfiguredHadoopHome -and $ConfiguredHadoopHome -ne $HadoopHome) {
    Write-Warning "Configured HADOOP_HOME '$ConfiguredHadoopHome' is incomplete; using '$HadoopHome'."
}

if (-not (Test-Path $VenvPython -PathType Leaf)) {
    $Failures.Add("Virtual-environment Python not found: $VenvPython")
}
else {
    Write-Host "[OK] Virtual-environment Python: $VenvPython" -ForegroundColor Green
    $env:PYSPARK_PYTHON = $VenvPython
    $env:PYSPARK_DRIVER_PYTHON = $VenvPython
    Write-Host "[OK] PySpark driver/worker Python: $VenvPython" -ForegroundColor Green
    & $VenvPython -c "import pyspark; print('[OK] PySpark ' + pyspark.__version__)"
    if ($LASTEXITCODE -ne 0) {
        $Failures.Add("PySpark cannot be imported from the virtual environment.")
    }
}

if (Test-CommandAvailable "java") {
    Write-Host "[OK] Java is available." -ForegroundColor Green
}

$SparkSubmit = Join-Path $ProjectRoot ".venv\Scripts\spark-submit.cmd"
if (Test-Path $SparkSubmit -PathType Leaf) {
    Write-Host "[OK] spark-submit: $SparkSubmit" -ForegroundColor Green
}
else {
    $Failures.Add("spark-submit not found: $SparkSubmit")
}

foreach ($RequiredFile in @("winutils.exe", "hadoop.dll")) {
    $Candidate = Join-Path $HadoopBin $RequiredFile
    if (Test-Path $Candidate -PathType Leaf) {
        Write-Host "[OK] $RequiredFile`: $Candidate" -ForegroundColor Green
    }
    else {
        $Failures.Add("Required Windows Hadoop file not found: $Candidate")
    }
}

if ($Failures.Count -gt 0) {
    Write-Host "`nEnvironment check failed:" -ForegroundColor Red
    foreach ($Failure in $Failures) {
        Write-Host " - $Failure" -ForegroundColor Red
    }
    throw "Fix the items above before running the pipeline."
}

Write-Host "`nEnvironment is ready." -ForegroundColor Green
