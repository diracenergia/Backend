# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Argentina/Buenos_Aires

# Usamos /code porque tu docker-compose setea working_dir=/code
WORKDIR /code

# Paquetes de sistema (curl para healthcheck; libpq-dev para psycopg)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl \
 && rm -rf /var/lib/apt/lists/*

# Dependencias de Python
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
 && pip install -r requirements.txt

# Código de la app (en dev lo montás como volumen; en prod queda embebido)
COPY app ./app
COPY web ./web

EXPOSE 8000

# En prod se usa esto; en dev lo sobreescribe docker-compose
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
