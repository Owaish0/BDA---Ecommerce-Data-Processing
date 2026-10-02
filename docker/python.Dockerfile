FROM python:3.11.11-slim-bookworm
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[pipeline,dashboard]'
COPY dashboard ./dashboard
COPY scripts ./scripts
CMD ["python", "-m", "ecommerce.producer"]
