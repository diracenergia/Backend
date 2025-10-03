FROM python:3.11-slim

# Evitar bytecode y buffering
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Render expone $PORT automáticamente
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
