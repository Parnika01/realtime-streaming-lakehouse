# Real-Time Streaming Lakehouse - Windows Quickstart Script

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " ⚡ Real-Time Streaming Financial Lakehouse Quickstart     " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check Python
if (-not (Get-Command "python" -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] Python is not found in PATH." -ForegroundColor Red
    exit 1
}

Write-Host "[1/3] Installing Python dependencies..." -ForegroundColor Yellow
python -m pip install -r requirements.txt --quiet

Write-Host "[2/3] Checking Docker Desktop status..." -ForegroundColor Yellow
$dockerProcess = Get-Process "Docker Desktop" -ErrorAction SilentlyContinue
if ($dockerProcess) {
    Write-Host "  Docker Desktop is running. Starting Redpanda (Kafka) & MinIO..." -ForegroundColor Green
    docker compose -f docker/docker-compose.yml up -d
} else {
    Write-Host "  Docker Desktop is not currently running." -ForegroundColor Gray
    Write-Host "  Pipeline will run in high-performance Local Buffer & DuckDB Lakehouse mode." -ForegroundColor Green
    Write-Host "  (To use Redpanda Kafka & MinIO, launch Docker Desktop and run: docker compose -f docker/docker-compose.yml up -d)" -ForegroundColor Gray
}

Write-Host "[3/3] Ready to Launch!" -ForegroundColor Green
Write-Host ""
Write-Host "To run the Real-time Pipeline:" -ForegroundColor White
Write-Host "   python run_pipeline.py --rate 3.0" -ForegroundColor Cyan
Write-Host ""
Write-Host "To launch the Interactive Analytics Dashboard in another terminal:" -ForegroundColor White
Write-Host "   python -m streamlit run src/analytics/dashboard.py" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
