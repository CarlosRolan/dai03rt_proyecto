# Propuesta de Proyecto: Pipeline y API de Análisis de Películas con TMDB

## 👥 Roles y Responsabilidades

### 🏗️ Data Architect
- **Responsable del Diseño en AWS:** Definir y configurar la arquitectura en la nube, incluyendo el bucket S3 para el Data Lake, la base de datos PostgreSQL en RDS y los permisos necesarios para los servicios.

### ⚙️ Data Engineer
- **Responsable del Pipeline de Datos:** Implementar las funciones AWS Lambda para la extracción masiva y diaria de datos de la API de TMDB, y para la carga y transformación de estos datos desde S3 hacia la base de datos en RDS.

### 🔬 Data Scientist
- **Responsable del Análisis y Modelado:**
  - Analizar los datos en PostgreSQL para definir una métrica de "éxito" para una película.
  - Entrenar un modelo de clasificación para predecir dicho éxito.
  - Diseñar la lógica para los endpoints de Q&A, seleccionando los modelos apropiados de Hugging Face.

### 🚀 ML Engineer
- **Responsable del Despliegue y la API:**
  - Envolver el modelo predictivo y la lógica de los endpoints en una API robusta utilizando FastAPI.
  - Desplegar la aplicación FastAPI en una instancia de AWS EC2, asegurando que todos los endpoints sean funcionales.

---

## 📝 Fases del Proyecto

### **Fase 01: Infraestructura y Pipeline de Datos**
**Objetivo:** Construir un sistema automatizado que extraiga datos de la API de TMDB y los almacene de forma estructurada en una base de datos en la nube.

**Tareas Clave:**
1.  **Extracción de Datos (Data Engineer, Data Architect):**
    - Obtener una API Key de TMDB.
    - Crear una **AWS Lambda** para la extracción masiva inicial de todas las películas y guardarlas en un **bucket S3**.
    - Configurar la Lambda con **EventBridge** para que se ejecute diariamente y obtenga las películas actualizadas recientemente usando el endpoint `/changes`.

2.  **Procesamiento y Carga (Data Engineer, Data Architect):**
    - Desarrollar una segunda **AWS Lambda** que se active mediante un **trigger de S3** al recibir nuevos datos.
    - Esta función procesará los ficheros JSON y los cargará de forma estructurada en la base de datos **PostgreSQL en AWS RDS**.

**Entregables de esta fase:**
- Infraestructura en AWS (S3, RDS) configurada.
- Pipelines de datos automáticos (masivo y diario) funcionando.
- Base de datos poblada y actualizada con los datos de TMDB.

### **Fase 02: Modelado, API y Despliegue**
**Objetivo:** Desarrollar una API multifuncional que prediga el éxito de una película y permita hacer consultas complejas.

**Tareas Clave:**
1.  **Desarrollo del Modelo (Data Scientist):**
    - Utilizar los datos de PostgreSQL para entrenar un modelo de Machine Learning (ej. con Scikit-learn) que prediga si una película será un "éxito".

2.  **Desarrollo de la API (ML Engineer, Data Scientist):**
    - Crear una aplicación con **FastAPI** que incluya los siguientes endpoints:
      - `/predict`: Recibe los datos de una película y devuelve la predicción del modelo.
      - `/ask-text`: Recibe una pregunta en texto (ej. "¿Películas de mayor presupuesto de 2023?"), la convierte a SQL usando un modelo de **Hugging Face**, consulta la base de datos y devuelve una respuesta en texto.
      - `/ask-visual`: Recibe una pregunta orientada a visualización (ej. "Top 5 géneros por popularidad"), consulta la base de datos y devuelve un gráfico generado con **Matplotlib/Seaborn**.

3.  **Despliegue (ML Engineer):**
    - Desplegar la aplicación FastAPI completa en una instancia **AWS EC2** para que sea accesible públicamente.

**Entregables de esta fase:**
- Un modelo de clasificación entrenado.
- API funcional desplegada en EC2 con los tres endpoints implementados.
- Documentación de la API (generada por FastAPI).