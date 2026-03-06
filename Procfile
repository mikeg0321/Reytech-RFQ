web: PYTHONDONTWRITEBYTECODE=1 gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --max-requests 100 --max-requests-jitter 25 --worker-tmp-dir /dev/shm --preload
