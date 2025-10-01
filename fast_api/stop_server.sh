#!/bin/bash
set -e

# Detener servidor en puerto 8000 si existe
if fuser 8000/tcp > /dev/null 2>&1; then
  echo "Deteniendo servidor en puerto 8000..."
  fuser -k 8000/tcp
  echo "Servidor detenido."
else
  echo "No hay servidor corriendo en el puerto 8000."
fi
