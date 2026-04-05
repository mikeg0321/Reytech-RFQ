FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

# Copy the scraper modules from the main app
COPY scprs_browser.py .
COPY scprs_public_search.py .
COPY app.py .

EXPOSE 8001

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8001", "--workers", "1", "--threads", "2", "--timeout", "300"]
