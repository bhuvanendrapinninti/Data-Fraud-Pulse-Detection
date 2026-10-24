FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt
COPY producer ./producer
COPY dashboard ./dashboard
