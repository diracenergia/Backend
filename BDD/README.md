# Base local: Usuarios, Bombas y Tanques (PostgreSQL + FastAPI)

Este kit te deja TODO listo para probar en local.

## Requisitos
- Docker Desktop o Docker Engine + docker compose
- Python 3.10+
- `curl` (o Postman/Insomnia)

## 1) Levantar PostgreSQL y Adminer
```bash
cd esp32-tank-pump-starter
docker compose up -d
# Esperá ~5-10s a que inicialice
```

La DB queda en `localhost:5432` (usuario `postgres`, pass `postgres`, DB `munirdls`).  
Adminer: http://localhost:8080 → System: `PostgreSQL`, Server: `db`, User: `postgres`, Password: `postgres`, Database: `munirdls`.

> Al primer arranque se ejecuta `initdb/01-schema.sql` y `initdb/02-seed.sql` automáticamente.

**API key demo (solo local):** `devkey-123456`

## 2) Arrancar la API (FastAPI)
```bash
cd app
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```
La API queda en http://127.0.0.1:8000 (Docs: http://127.0.0.1:8000/docs).

## 3) Probar lecturas con curl

### Tanque
```bash
curl -X POST http://127.0.0.1:8000/ingest/tank   -H "Content-Type: application/json"   -H "X-API-Key: devkey-123456"   -d '{"tank_id":1,"level_percent":73.4,"temperature_c":21.8}'
```

### Bomba
```bash
curl -X POST http://127.0.0.1:8000/ingest/pump   -H "Content-Type: application/json"   -H "X-API-Key: devkey-123456"   -d '{"pump_id":1,"is_on":true,"pressure_bar":1.6}'
```

### Consultar últimas lecturas
```bash
curl http://127.0.0.1:8000/tanks/1/latest
curl http://127.0.0.1:8000/pumps/1/latest
```

## 4) Conectar el ESP32 (HTTP)
En tu sketch, usa el header `X-API-Key` con el valor `devkey-123456` y envía JSON al endpoint correspondiente.

## 5) Migrar a producción (resumen)
- Cambiá `devices.api_key_sha256` por bcrypt (columna `api_key_hash`) y ajustá el código.
- Poné HTTPS (reverse proxy con Caddy/Nginx o un PaaS).
- Crea usuarios/dispositivos reales y quita la seed de demo.

---

### Estructura
```
esp32-tank-pump-starter/
├─ docker-compose.yml
├─ initdb/
│  ├─ 01-schema.sql
│  └─ 02-seed.sql
└─ app/
   ├─ main.py
   ├─ requirements.txt
   └─ .env
```

¡Listo! Si algo falla, revisá los logs:
```bash
docker compose logs -f db
```

y para la API:
```bash
uvicorn main:app --reload
```