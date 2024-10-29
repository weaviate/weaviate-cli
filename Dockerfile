FROM python:3.12-slim

ARG ENVIRONMENT=development

WORKDIR /app

COPY . .

RUN if [ "$ENVIRONMENT" = "development" ]; then \
        pip install .; \
    else \
        pip install weaviate-cli==v3.0.0-alpha.2; \
    fi

ENTRYPOINT ["python", "cli.py"]
