Write-Host "[demo] stopping mysql for 70s..."
docker compose stop mysql
Start-Sleep -Seconds 70
Write-Host "[demo] starting mysql..."
docker compose start mysql
