import os
import httpx
import logging
import traceback
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Default config
DEFAULT_TIMEOUT = 60

class AgentHook:
    def __init__(self, webhook_url: str):
        """
        Khởi tạo AgentHook.
        Validate URL ngay lập tức để tránh lỗi runtime muộn.
        """
        self.webhook_url = webhook_url
        if not self.webhook_url or not self.webhook_url.strip():
            logging.warning("Agent Hook URL is not set. Self-healing capability will be DISABLED.")
            self.webhook_url = None
        
        # Lấy timeout từ env, fallback về default
        try:
            self.timeout = int(os.environ.get("REQUEST_TIMEOUT", DEFAULT_TIMEOUT))
        except ValueError:
            self.timeout = DEFAULT_TIMEOUT

    def call_agent_hook(self, error: str, payload_data: Dict[str, Any]) -> None:
        """
        Gửi tín hiệu lỗi tới Agent AI để kích hoạt quy trình sửa code.

        :param error: Thông điệp lỗi (str).
        :param payload_data: Dữ liệu gây ra lỗi (dict).
        """
        if not self.webhook_url:
            logging.error("Cannot call Agent: Webhook URL is missing.")
            return

        # Lấy traceback hiện tại (chỉ hoạt động đúng khi hàm này được gọi trong block except)
        tb_str = traceback.format_exc()

        payload = {
            "error": str(error),
            "payload_data": payload_data,
            "traceback": tb_str,
        }

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "ETL-Pipeline-Service/1.0"
        }

        logging.info(f"Contacting Agent at {self.webhook_url}...")

        try:
            # Sử dụng Context Manager để quản lý connection pool hiệu quả
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    self.webhook_url,
                    json=payload,
                    headers=headers
                )
                
                # Kiểm tra HTTP Status (4xx, 5xx sẽ raise exception)
                response.raise_for_status()
                
                logging.info(f"Agent acknowledged receipt. Status: {response.status_code}")
                # Có thể log thêm response body nếu cần debug: logging.debug(response.json())

        except httpx.ConnectError:
            # Lỗi này rất thường gặp ở Local Docker nếu Agent chưa start xong
            logging.error(f"Connection Refused: Could not connect to Agent at {self.webhook_url}. Is the Agent container running?")
        
        except httpx.TimeoutException:
            logging.error(f"Timeout: Agent took longer than {self.timeout}s to respond.")

        except httpx.HTTPStatusError as e:
            logging.error(f"Agent returned error {e.response.status_code}: {e.response.text}")

        except Exception as e:
            logging.error(f"Unexpected error calling Agent: {e}")