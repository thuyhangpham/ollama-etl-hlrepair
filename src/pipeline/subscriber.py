import logging
import json
import time
import os
from typing import Any, Callable, Optional, List

# --- CLASS GIẢ LẬP MESSAGE CỦA PUBSUB ---
class MockMessage:
    """
    Class này giả lập behavior của Google Pub/Sub Message.
    Giúp logic chính không bị lỗi khi gọi .ack() hoặc .data
    """
    def __init__(self, data_dict: dict):
        # PubSub trả về data dưới dạng bytes, nên ta encode lại
        self.data = json.dumps(data_dict).encode("utf-8")

    def ack(self):
        # Ở local file, việc lấy message ra khỏi list đã coi như là ack rồi
        logging.info("MockMessage: Acknowledged (Auto-removed from queue file)")

# --- BASE CLASS (GIỮ NGUYÊN) ---
class Subscriber:
    def __init__(self):
        raise NotImplementedError("This class should not be instantiated directly. Use a subclass instead.")
    
    def subscribe(self, callback: Callable) -> None:
        raise NotImplementedError("The 'subscribe' method must be implemented in the subclass.")
    
    def parse_message(self, message: Any) -> dict:
        raise NotImplementedError("The 'parse_message' method is not implemented in the base class.")
    
    def acknowledge_message(self, message: Any) -> None:
        raise NotImplementedError("The 'acknowledge_message' method is not implemented in the base class.")
    
    def handle_error_message(self, message: Any) -> None:
        raise NotImplementedError("The 'handle_error_message' method is not implemented in the base class.")

# --- LOCAL FILE SUBSCRIBER (THAY THẾ PUBSUB) ---
class LocalFileSubscriber(Subscriber):
    def __init__(self, queue_file_path: str = "queue/messages.json", timeout: Optional[int] = None):
        """
        :param queue_file_path: Đường dẫn đến file JSON đóng vai trò là hàng đợi.
        """
        self.queue_file = queue_file_path
        self.timeout = timeout
        
        # Tạo file queue nếu chưa tồn tại
        if not os.path.exists(self.queue_file):
            os.makedirs(os.path.dirname(self.queue_file), exist_ok=True)
            with open(self.queue_file, 'w') as f:
                json.dump([], f)

    def subscribe(self, callback: Callable):
        """
        Thay vì streaming từ Google, ta dùng vòng lặp để đọc file JSON.
        """
        logging.info(f"Watching local file queue: {self.queue_file}...")
        
        while True:
            try:
                # 1. Đọc file
                if not os.path.exists(self.queue_file):
                    time.sleep(1)
                    continue

                with open(self.queue_file, 'r') as f:
                    try:
                        messages = json.load(f)
                    except json.JSONDecodeError:
                        messages = []

                # 2. Kiểm tra có tin nhắn không
                if not messages:
                    time.sleep(1) # Nghỉ 1 giây rồi quét tiếp
                    continue

                # 3. Lấy tin nhắn đầu tiên (FIFO)
                raw_data = messages.pop(0)

                # 4. Ghi lại file (đã loại bỏ tin nhắn vừa lấy)
                # Đây là hành động mô phỏng việc "nhận" message
                with open(self.queue_file, 'w') as f:
                    json.dump(messages, f, indent=2)

                # 5. Đóng gói vào MockMessage và gọi Callback
                mock_msg = MockMessage(raw_data)
                
                # Gọi hàm xử lý logic chính (của ETL)
                logging.info(f"Processing message: {raw_data}")
                callback(mock_msg)

            except Exception as e:
                logging.error(f"Error in local file poll: {e}")
                time.sleep(1)

    def parse_message(self, message: MockMessage) -> dict:
        """
        Giải mã message từ MockMessage (bytes -> dict)
        """
        try:
            return json.loads(message.data.decode("utf-8")) if message.data else {}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse message: {e}")
            raise e
        
    def acknowledge_message(self, message: MockMessage):
        """
        Gọi hàm ack của MockMessage
        """
        try:
            message.ack()
        except Exception as e:
            logging.error(f"Failed to acknowledge message: {e}")
            raise e

    def handle_error_message(self, message: MockMessage):
        """
        Nếu lỗi, ta có thể ghi lại message vào cuối file queue (Re-queue)
        """
        try:
            logging.error("Handling error message - Re-queueing to file...")
            
            # Decode lại data để ghi vào JSON
            data_dict = json.loads(message.data.decode("utf-8"))
            
            # Đọc queue hiện tại
            current_messages = []
            if os.path.exists(self.queue_file):
                with open(self.queue_file, 'r') as f:
                    try:
                        current_messages = json.load(f)
                    except:
                        current_messages = []
            
            # Thêm lại vào cuối hàng đợi
            current_messages.append(data_dict)
            
            with open(self.queue_file, 'w') as f:
                json.dump(current_messages, f, indent=2)
                
            message.ack() # Ack để báo là đã xử lý việc lỗi xong
            
        except Exception as e:
            logging.error(f"Failed to handle error message: {e}")
            raise e