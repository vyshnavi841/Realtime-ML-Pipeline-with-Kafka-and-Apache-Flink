FROM python:3.9-slim
WORKDIR /app

# Update the base system package list and add network recovery flags
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Run pip with extended timeout thresholds and fallback configurations
RUN pip install --no-cache-dir --default-timeout=100 streamlit kafka-python pandas requests

COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]