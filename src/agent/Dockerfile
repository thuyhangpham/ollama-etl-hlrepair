FROM python:3.10-slim

WORKDIR /app

# Cài đặt thư viện cho Agent (Quart là web framework async nhẹ)
RUN pip install --no-cache-dir quart httpx python-dotenv

CMD ["python", "app.py"]