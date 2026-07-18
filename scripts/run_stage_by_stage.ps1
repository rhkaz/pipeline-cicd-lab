$ErrorActionPreference = "Stop"

# Resolve project and local runtime paths.
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvRoot = Join-Path $ProjectRoot ".venv"
$PythonExe = Join-Path $VenvRoot "Scripts\python.exe"
$script:SparkSubmit = Join-Path $VenvRoot "Scripts\spark-submit.cmd"
$HadoopHome = Join-Path $env:USERPROFILE "hadoop"
$HadoopBin = Join-Path $HadoopHome "bin"

# Fail early with a clear message when local prerequisites are missing.
if (-not (Test-Path $PythonExe -PathType Leaf)) {
    throw "Virtual-environment Python not found: $PythonExe"
}

if (-not (Test-Path $script:SparkSubmit -PathType Leaf)) {
    throw "spark-submit not found: $script:SparkSubmit"
}

if (-not (Test-Path (Join-Path $HadoopBin "hadoop.dll") -PathType Leaf)) {
    throw "hadoop.dll not found in: $HadoopBin"
}

if (-not (Test-Path (Join-Path $HadoopBin "winutils.exe") -PathType Leaf)) {
    throw "winutils.exe not found in: $HadoopBin"
}

# Configure PySpark and Hadoop for this PowerShell session.
$env:VIRTUAL_ENV = $VenvRoot
$env:HADOOP_HOME = $HadoopHome
$env:Path = "$VenvRoot\Scripts;$HadoopBin;$env:Path"
$env:JAVA_TOOL_OPTIONS = "-Djava.library.path=$HadoopBin"
$env:PYSPARK_PYTHON = $PythonExe
$env:PYSPARK_DRIVER_PYTHON = $PythonExe

$SparkHome = & $PythonExe -c "import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)"
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($SparkHome)) {
    throw "Unable to resolve SPARK_HOME from the virtual environment."
}
$env:SPARK_HOME = $SparkHome.Trim()

$SourcePath = Join-Path $ProjectRoot "src"
if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
    $env:PYTHONPATH = $SourcePath
}
elseif (($env:PYTHONPATH -split ";") -notcontains $SourcePath) {
    $env:PYTHONPATH = "$SourcePath;$env:PYTHONPATH"
}

# Execute spark-submit and stop immediately when it returns an error.
function Invoke-SparkSubmitChecked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Step,

        [Parameter(Mandatory = $true)]
        [object[]]$Arguments
    )

    & $script:SparkSubmit @Arguments
    $ExitCode = $LASTEXITCODE

    if ($ExitCode -ne 0) {
        throw "$Step failed with exit code $ExitCode."
    }
}

# Pipeline configuration.
$Main = Join-Path $ProjectRoot "src\network_metrics\main.py"
$Preview = Join-Path $ProjectRoot "scripts\preview_output.py"
$Config = Join-Path $ProjectRoot "config\pipeline.example.json"
$InputPath = Join-Path $ProjectRoot "sample_data"
$OutputRoot = Join-Path $ProjectRoot "output"
$ProcessingDate = "2025-07-23"
$RunId = "local-20250723-001"

$CommonArgs = @(
    "--config", $Config,
    "--input-path", $InputPath,
    "--output-root", $OutputRoot,
    "--processing-date", $ProcessingDate,
    "--run-id", $RunId
)

Write-Host "`nRun ID used by every stage: $RunId" -ForegroundColor Cyan
Write-Host "Output root: $OutputRoot" -ForegroundColor Cyan

Write-Host "`n[1/5] BRONZE" -ForegroundColor Yellow
Invoke-SparkSubmitChecked -Step "Bronze stage" -Arguments `
    (@($Main) + $CommonArgs + @("--stage", "bronze"))
Invoke-SparkSubmitChecked -Step "Bronze preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "bronze", "--rows", "10")

Write-Host "`n[2/5] SILVER" -ForegroundColor Yellow
Invoke-SparkSubmitChecked -Step "Silver stage" -Arguments `
    (@($Main) + $CommonArgs + @("--stage", "silver"))
Invoke-SparkSubmitChecked -Step "Silver preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "silver", "--rows", "10")
Invoke-SparkSubmitChecked -Step "Silver quality-log preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "quality-logs", "--rows", "20")

Write-Host "`n[3/5] GOLD HOURLY" -ForegroundColor Yellow
Invoke-SparkSubmitChecked -Step "Gold-hourly stage" -Arguments `
    (@($Main) + $CommonArgs + @("--stage", "gold-hourly"))
Invoke-SparkSubmitChecked -Step "Gold-hourly preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "gold-hourly", "--rows", "20")

Write-Host "`n[4/5] GOLD DAILY" -ForegroundColor Yellow
Invoke-SparkSubmitChecked -Step "Gold-daily stage" -Arguments `
    (@($Main) + $CommonArgs + @("--stage", "gold-daily"))
Invoke-SparkSubmitChecked -Step "Gold-daily preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "gold-daily", "--rows", "20")

Write-Host "`n[5/5] MONITORING" -ForegroundColor Yellow
Invoke-SparkSubmitChecked -Step "Monitoring stage" -Arguments `
    (@($Main) + $CommonArgs + @("--stage", "monitoring"))
Invoke-SparkSubmitChecked -Step "Monitoring quality-log preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "quality-logs", "--rows", "50")
Invoke-SparkSubmitChecked -Step "Pipeline run-log preview" -Arguments `
    @($Preview, "--output-root", $OutputRoot, "--dataset", "run-logs", "--rows", "50")

Write-Host "`nAll stages and previews completed successfully." -ForegroundColor Green
