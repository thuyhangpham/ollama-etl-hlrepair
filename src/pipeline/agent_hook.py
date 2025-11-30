import os
import httpx
import logging
import traceback
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 60))

class AgentHook():
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def call_agent_hook(self, error: str, payload_data: dict) -> None:
        """
        Call the agent hook webhook with the provided message and payload data.

        :param error: The error message to be sent.
        :param payload_data: The payload data associated with the error.
        """
        if self.webhook_url is None or self.webhook_url.strip() == "":
            logging.error("Agent hook URL is not set. Skipping the call.")
            return
        
        try:
            headers = {
                "Content-Type": "application/json"
            }
            payload = {
                "error": error,
                "payload_data": payload_data,
                "traceback": traceback.format_exc(),
            }
            
            response = httpx.post(
                self.webhook_url,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            
            response.raise_for_status()  # Raise an error for bad responses
            logging.info(f"Agent hook called successfully: {response.status_code}")
        except httpx.RequestError as e:
            logging.error(f"Request error while calling agent hook: {e}")
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error while calling agent hook: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logging.error(f"Unexpected error while calling agent hook: {e}")
