# Usa una imagen base con Python
FROM python:3.9-alpine

# Establece el directorio de trabajo
WORKDIR /workdir

# Copia el código fuente y el archivo de requisitos
COPY src/ /workdir
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5432

# Define el comando por defecto para ejecutar tu aplicación
CMD ["python", "src/run_main.py"]