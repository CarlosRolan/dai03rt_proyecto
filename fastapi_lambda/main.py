
from fastapi import FastAPI
from mangum import Mangum

app = FastAPI()

# Ruta pprincipal
@app.get("/")
def root():
    return {"message": "¡Hola desde Lambda y FastAPI (local de momento)! 🚀"}

# Endpoint que recibirá datos de la peli para predecir si tendrá éxito
@app.post("/predict")
def predict_placeholder():
    return {"success": "Predicción éxito"}

# Endpoint que recibirá pregunta en texto como películas más taquilleras del 2023
@app.post("/ask-text")
def ask_text_placeholder():
    return {"answer": "Aquí irá la respuesta en texto"}

# Endpoint para gráficos
@app.post("/ask-visual")
def ask_visual_placeholder():
    return {"chart": "Aquí irá la visualización"}

# Para AWS 
handler = Mangum(app)


# Para comprobar en navegador , uvicorn fastapi_lambda.main:app --reload

# http://127.0.0.1:8000/ , saldrá el mensaje de bienvenida
# http://127.0.0.1:8000/docs , saldrán los endpoint

# En EC2  ,  uvicorn fastapi_lambda.main:app --host 0.0.0.0 --port 8000, no se necesita magnum
# http://<IP_PUBLICA>:8000/  ,  http://54.246.30.204:8000/
# http://<IP_PUBLICA>:8000/docs  , http://54.246.30.204:8000/docs
