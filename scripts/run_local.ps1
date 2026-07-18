$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$ProjectRoot\src;$env:PYTHONPATH"

spark-submit `
  "$ProjectRoot\src\network_metrics\main.py" `
  --config "$ProjectRoot\config\pipeline.example.json" `
  --input-path "$ProjectRoot\sample_data" `
  --output-root "$ProjectRoot\output" `
  --processing-date "2025-07-23" `
  --stage all
