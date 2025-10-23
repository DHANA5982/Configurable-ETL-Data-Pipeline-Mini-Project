FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src
COPY config/ ./config
COPY data/ ./data
COPY logs/ ./logs

ENV PYTHONPATH=/app/src

CMD ["python", "src/pipeline.py"]
