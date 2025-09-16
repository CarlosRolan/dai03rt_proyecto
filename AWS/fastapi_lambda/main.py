from fastapi import FastAPI
from mangum import Mangum
from pydantic import BaseModel #Para la prediccion
import matplotlib.pyplot as plt
import io, base64 #Para graficar
from fastapi.responses import StreamingResponse # Para ver gráficos en el navegador


# Crear la aplicación FastAPI
app = FastAPI()

# Modelo de datos para la entrada de la película
class MovieInput(BaseModel):
    title: str
    budget: int
    revenue: int

# Modelo de datos para las preguntas
class QuestionInput(BaseModel):
    question: str

# Ruta pprincipal
@app.get("/")
def root():
    return {"message": "¡Hola desde Lambda y FastAPI (local de momento)! 🚀"}

# Endpoint que recibirá datos de la peli para predecir si tendrá éxito
""" @app.post("/predict")
def predict_placeholder():
    return {"success": "Predicción éxito"} """
# metiendo un poco de lógica para probar en postman
@app.post("/predict")
def predict(movie: MovieInput):
    if movie.revenue > movie.budget:
        status = "Éxito"
    else:
        status = "Fracaso"
    return {"title": movie.title, "resultado": status}



# Endpoint que recibirá pregunta en texto como películas más taquilleras del 2023
""" @app.post("/ask-text")
def ask_text_placeholder():
    return {"answer": "Aquí irá la respuesta en texto"} """
# metiendo un poco de lógica para probar en postman
@app.post("/ask-text")
def ask_text(input: QuestionInput):
    q = input.question.lower()
    if "2023" in q and "taquilleras" in q:
        return {"answer": "Ejemplo: Oppenheimer, Barbie, Super Mario Bros"}
    elif "presupuesto" in q:
        return {"answer": "La película más cara fue Avatar: The Way of Water"}
    else:
        return {"answer": f"No tengo respuesta para: {input.question}"}
    

# Endpoint para gráficos
""" @app.post("/ask-visual")
def ask_visual_placeholder():
    return {"chart": "Aquí irá la visualización"} """
@app.post("/ask-visual") # postman
@app.get("/ask-visual") # navegador
def ask_visual_placeholder():

    # Datos ficticios (géneros y popularidad)
    genres = ["Acción", "Drama", "Comedia", "Animación", "Terror"]
    popularity = [80, 70, 65, 50, 40]

    # Crear gráfico de barras
    plt.figure(figsize=(6, 4))
    plt.bar(genres, popularity, color="skyblue")
    plt.title("Top 5 géneros por popularidad")
    plt.xlabel("Género")
    plt.ylabel("Popularidad")

    # Guardar en memoria y convertir a base64
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()

    return {"chart": img_base64}

# Para ver el gráfico en el navegador
@app.get("/ask-visual-html")
def ask_visual_html():
    genres = ["Acción", "Drama", "Comedia", "Animación", "Terror"]
    popularity = [80, 70, 65, 50, 40]

    plt.figure(figsize=(6, 4))
    plt.bar(genres, popularity, color="skyblue")
    plt.title("Top 5 géneros por popularidad")
    plt.xlabel("Género")
    plt.ylabel("Popularidad")

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)

    return StreamingResponse(buf, media_type="image/png")

# Para AWS 
handler = Mangum(app)


# Para comprobar en local , uvicorn fastapi_lambda.main:app --reload
# http://127.0.0.1:8000/ , saldrá el mensaje de bienvenida
# http://127.0.0.1:8000/docs , saldrán los endpoint
# http://127.0.0.1:8000/ask-visual , ver los gráficos en el navegador en json
# http://127.0.0.1:8000/ask-visual-html , ver gráficos en el navegador como imagen


# En EC2  ,  uvicorn fastapi_lambda.main:app --host 0.0.0.0 --port 8000, no se necesita magnum
# http://<IP_PUBLICA>:8000/  ,  http://54.246.30.204:8000/
# http://<IP_PUBLICA>:8000/docs  , http://54.246.30.204:8000/docs
