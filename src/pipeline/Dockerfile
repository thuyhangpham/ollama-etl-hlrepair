# Dùng python slim cho nhẹ
FROM python:3.10-slim

WORKDIR /app

# Cài đặt các thư viện cần thiết
# Chúng ta cài trực tiếp để đỡ phải tạo file requirements.txt lắt nhắt
RUN pip install --no-cache-dir python-dotenv httpx

# Lệnh chạy mặc định
CMD ["python", "main.py"]