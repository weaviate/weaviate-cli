FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y git
RUN  pip install .

ENTRYPOINT ["python", "cli.py"]
