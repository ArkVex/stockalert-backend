@echo off
echo Starting Flask app in the background...
start "StockAlert Backend" cmd /c "python wsgi.py"

echo Waiting for server to start...
timeout /t 5

echo Testing the API...
python test_filings.py

echo Done