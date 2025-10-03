# Backend MIN

## Env requeridas
- `DATABASE_URL` (cadena de Supabase, por ej:
  `postgresql://postgres.<hash>:<PASSWORD>@aws-1-us-east-2.pooler.supabase.com:5432/postgres?sslmode=require`)

Opcional:
- `LOG_LEVEL` (INFO/DEBUG/WARN)

## Run local
```bash
export DATABASE_URL=postgresql://...
uvicorn app.main:app --reload
