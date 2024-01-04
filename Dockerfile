# Usa una imagen base con Python
FROM python:3.9-alpine

# Establece el directorio de trabajo
WORKDIR /workdir

RUN apk add --no-cache \
    python3-dev \
    libpq \
    musl-dev \
    gcc \
    libxml2-dev \
    libxslt-dev \
    postgresql-dev \
    git

# Copia el código fuente y el archivo de requisitos
COPY src/ /workdir/src/
COPY config/ /workdir/config/
COPY requirements.txt .


# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5432

# Define el comando por defecto para ejecutar tu aplicación
CMD ["tail", "-f", "/dev/null"]