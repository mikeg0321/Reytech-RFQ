web: find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null; gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
