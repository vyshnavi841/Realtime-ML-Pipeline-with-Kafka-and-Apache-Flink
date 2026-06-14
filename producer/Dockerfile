FROM python:3.9-slim
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir kafka-python

COPY producer.py .
CMD ["python", "-u", "producer.py"]