@echo off
setlocal ENABLEDELAYEDEXPANSION
echo === Iniciando SCADA (modo HIBRIDO: DB en Docker, API y Front locales) ===

REM === CONFIGURAR ESTAS RUTAS ===
set REPO_ROOT=C:\Users\victo\OneDrive\Escritorio\Dirac\BDD
set FRONT_DIR=C:\Users\victo\OneDrive\Escritorio\Dirac\webapp
set VENV_DIR=%REPO_ROOT%\.venv

REM 1) Levantar la base de datos (Docker) + Adminer
start cmd /k "cd /d %REPO_ROOT% && docker compose up -d db adminer && echo DB lista && echo Adminer en http://localhost:8080"

REM 2) Backend FastAPI LOCAL (desde la RAIZ del repo)
IF NOT EXIST "%VENV_DIR%\Scripts\activate.bat" (
  echo Creando venv en %VENV_DIR%...
  python -m venv "%VENV_DIR%"
)
start cmd /k "cd /d %REPO_ROOT% && call %VENV_DIR%\Scripts\activate.bat && ^
  python -m pip install --upgrade pip && ^
  pip install -r requirements.txt && ^
  uvicorn app.main:app --reload --port 8000"

REM 3) Front Vite (React) LOCAL
start cmd /k "cd /d %FRONT_DIR% && npm install && npm run dev"

REM 4) Abrir navegador en la UI del front
timeout /t 5 >nul
start http://localhost:5173

echo === Todo lanzado (HIBRIDO) ===
pause
