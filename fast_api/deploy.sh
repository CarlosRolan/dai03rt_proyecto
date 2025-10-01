#!/bin/bash
set -e  # si algo falla, aborta

# Crear carpetas necesarias
mkdir -p logs

# Crear venv con python3.11 si no existe
if [ ! -d ".venv" ]; then
  python3.11 -m venv .venv
fi

# Activar venv
source .venv/bin/activate

# Instalar dependencias desde requirements (r.txt en tu carpeta)
pip install --upgrade pip
pip install -r r.txt

# Parar cualquier servidor previo en el puerto 8000
fuser -k 8000/tcp || true

# Lanzar uvicorn en segundo plano
nohup .venv/bin/uvicorn fast_api:app --host 0.0.0.0 --port 8000 >> logs/fastapi.log 2>&1 &
echo "FastAPI server started en background (puerto 8000). Logs en logs/fastapi.log"
