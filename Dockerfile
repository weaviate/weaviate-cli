FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install .

ENTRYPOINT ["python", "cli.py"]