#!/bin/bash
echo "Starting Flask app in the background..."
python wsgi.py &
SERVER_PID=$!

echo "Waiting for server to start..."
sleep 5

echo "Testing the API..."
python test_filings.py

echo "Shutting down server..."
kill $SERVER_PID

echo "Done"