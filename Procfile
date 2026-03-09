web: PYTHONDONTWRITEBYTECODE=1 gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --worker-tmp-dir /dev/shm --preload
