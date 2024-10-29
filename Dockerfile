FROM python:3.12-slim

ARG ENVIRONMENT=development

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y git

RUN if [ "$ENVIRONMENT" = "development" ]; then \
        pip install .; \
    else \
        export LATEST_RELEASE=$(git describe --tags --abbrev=0 --match "v*"); \
        pip install weaviate-cli==$LATEST_RELEASE; \
    fi

ENTRYPOINT ["python", "cli.py"]
