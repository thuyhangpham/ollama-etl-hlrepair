import os
import sys
import logging
from dotenv import load_dotenv

# Load biến môi trường từ file .env (nếu có)
load_dotenv()

# Import các thành phần của Pipeline
from pipeline import Pipeline
from transformer import Transformer
from loader import Loader
from agent_hook import AgentHook

# Import Subscriber phiên bản Local mà ta vừa sửa
from subscriber import LocalFileSubscriber

def main():
    # 1. Cấu hình Logging (Standard Python Logging)
    # Không dùng google.cloud.logging nữa để tránh lỗi credentials
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s', 
        stream=sys.stdout
    )
    logging.info("Starting Local ETL Pipeline...")

    # 2. Khởi tạo Subscriber (Đọc từ File Queue)
    # Mặc định đọc từ queue/messages.json
    queue_path = os.getenv("QUEUE_FILE_PATH", "queue/messages.json")
    subscriber = LocalFileSubscriber(queue_file_path=queue_path)
    logging.info(f"Subscriber connected to local queue: {queue_path}")

    # 3. Khởi tạo Transformer (Dynamic Loading)
    # File function.py cần nằm cùng thư mục hoặc được mount vào container
    transformer = Transformer(function_path="function.py")

    # 4. Khởi tạo Loader (Ghi ra File)
    # Mặc định ghi ra output/data_warehouse.jsonl
    output_path = os.getenv("OUTPUT_FILE_PATH", "output/data_warehouse.jsonl")
    loader = Loader(output_path=output_path)

    # 5. Khởi tạo Agent Hook (Giao tiếp với AI Agent)
    # AGENT_SERVICE_URL sẽ là địa chỉ của container Agent (ví dụ: http://agent:5000/webhook)
    agent_url = os.getenv("AGENT_SERVICE_URL", "http://localhost:5000/webhook")
    agent_hook = AgentHook(webhook_url=agent_url)

    # 6. Khởi tạo Pipeline chính
    pipeline = Pipeline(
        subscriber=subscriber,
        transformer=transformer,
        loader=loader,
        agent_hook=agent_hook,
        # Thời gian chờ nếu gặp lỗi trước khi thử lại (giây)
        error_delay=int(os.getenv("ERROR_DELAY", 5))
    )

    # 7. Bắt đầu chạy Pipeline
    logging.info("Pipeline initialized successfully. Waiting for messages...")
    pipeline.start()

if __name__ == "__main__":
    main()