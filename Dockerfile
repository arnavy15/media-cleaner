FROM python:3.12-slim
ARG APP_VERSION=1.0.2

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_VERSION=${APP_VERSION}

LABEL org.opencontainers.image.version="${APP_VERSION}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends mkvtoolnix ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY web_app.py /app/web_app.py
COPY VERSION /app/VERSION

EXPOSE 5050

CMD ["python", "/app/web_app.py"]
