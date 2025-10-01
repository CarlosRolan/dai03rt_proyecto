
# FastAPI en EC2 – Guía de uso

La carpeta `fast_api/` contiene el código de la API y dos scripts para gestionar el servidor FastAPI en una instancia EC2.



## 📂 Estructura de la carpeta


fast\_api/
├── fast\_api.py       # Código principal con `app = FastAPI(...)`
├── r.txt             # Lista de dependencias mínimas
├── logs/             # Carpeta donde se guardan los logs
├── deploy.sh         # Script para arrancar el servidor
└── stop\_server.sh    # Script para detener el servidor



> El entorno virtual `.venv/` se creará automáticamente la primera vez que ejecutes `deploy.sh`.



## 🔐 Permisos de ejecución

Después de subir los scripts a tu instancia EC2, dales permisos de ejecución (solo una vez):

```bash
cd ~/fast_api
chmod +x deploy.sh stop_server.sh
````

---

## ▶️ Arrancar el servidor (`deploy.sh`)

El script `deploy.sh` realiza estas tareas:

1. Crea el entorno virtual `.venv` con **Python 3.11** si no existe.
2. Activa el entorno virtual.
3. Instala las dependencias desde `r.txt`.
4. Mata cualquier proceso que esté usando el puerto **8000**.
5. Lanza **uvicorn** en segundo plano y guarda los logs en `logs/fastapi.log`.

Ejemplo de ejecución:

```bash
cd ~/fast_api
./deploy.sh
```

---

### Comprobaciones útiles:


```bash
# Ver si escucha en el puerto 8000
ss -ltnp | grep :8000

# Probar desde la propia EC2
curl http://localhost:8000/docs

# Revisar últimos logs
tail -n 50 logs/fastapi.log
```

```


Para acceder desde tu PC:

```
http://<IP_PUBLICA_EC2>:8000/docs
```


## ⏹️ Detener el servidor (`stop_server.sh`)

El script `stop_server.sh`:

1. Mata cualquier proceso que esté usando el puerto **8000**.
2. Si estabas trabajando con el entorno virtual activado, lo desactiva para dejar la consola limpia.

Ejemplo de ejecución:

```bash
cd ~/fast_api
./stop_server.sh
```


## 🔄 Flujo recomendado de trabajo

Cuando subas cambios en el código o el modelo, lo más seguro es reiniciar el servidor:

```bash
cd ~/fast_api
./stop_server.sh
./deploy.sh
tail -f logs/fastapi.log    # observar logs en tiempo real
```

## ⚙️ Notas

* Ejecuta siempre los scripts desde la carpeta del proyecto: `~/fast_api`.
* El servidor escucha en el puerto **8000**. Si cambias el puerto, actualiza ambos scripts.
* Asegúrate de que el **Security Group** de tu instancia EC2 permite tráfico entrante en **TCP 8000**.
* Los logs se guardan en `logs/fastapi.log`.
