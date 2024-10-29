FROM python:3.12-slim

ARG ENVIRONMENT=development

WORKDIR /app

COPY . .

RUN if [ "$ENVIRONMENT" = "development" ]; then \
        pip install .; \
    else \
        pip install weaviate-cli; \
    fi

ENTRYPOINT ["python", "cli.py"]
