# src/agent/Dockerfile
FROM python:3.10-slim

# Cài docker cli
RUN apt-get update && apt-get install -y docker.io 

WORKDIR /app
RUN pip install quart httpx python-dotenv
CMD ["python", "app.py"]