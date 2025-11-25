# run_slow_query.ps1
# Выполняет slow_query.sql внутри контейнера mysql в БД appdb

$ErrorActionPreference = "Stop"

$Password = $env:MYSQL_ROOT_PASSWORD
if (-not $Password) {
    Write-Error "Переменная MYSQL_ROOT_PASSWORD не задана. Сначала сделай: `$env:MYSQL_ROOT_PASSWORD='rootpass'"
    exit 1
}

$scriptDir = Split-Path -Parent $PSCommandPath
$sqlPath   = Join-Path $scriptDir "slow_query.sql"

if (-not (Test-Path $sqlPath)) {
    Write-Error "Не найден файл slow_query.sql по пути: $sqlPath"
    exit 1
}

Write-Host "Использую SQL-файл: $sqlPath"
Write-Host "Выполняю slow query внутри контейнера mysql в БД appdb..."

Get-Content $sqlPath -Raw | docker compose exec -T -e MYSQL_PWD=$Password mysql mysql -uroot appdb
