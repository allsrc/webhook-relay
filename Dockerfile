FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install . && useradd -u 10001 -m relay && mkdir /data && chown relay /data
USER 10001
EXPOSE 8080
CMD ["uvicorn", "webhook_relay.app:from_env", "--factory", "--host", "0.0.0.0", "--port", "8080"]
