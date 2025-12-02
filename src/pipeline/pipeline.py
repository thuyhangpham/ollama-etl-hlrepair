import logging
import time
from loader import Loader
from subscriber import Subscriber
from transformer import Transformer
from agent_hook import AgentHook

class Pipeline:
    def __init__ (self, subscriber: Subscriber, transformer: Transformer, loader: Loader, agent_hook: AgentHook, error_delay: int=-1):
        """
        Initialize the ETL Pipeline with subscriber, transformer, and loader components.

        :param subscriber: The subscriber component to receive messages.
        :param transformer: The transformer component to process messages.
        :param loader: The loader component to store processed messages.
        :param agent_hook: The agent hook component for error handling.
        :param error_delay: Delay in seconds before retrying on error (-1 for infinite wait).
        """
        self.subscriber = subscriber
        self.transformer = transformer
        self.loader = loader
        self.agent_hook = agent_hook
        self.error_delay = error_delay

    def _initialize(self):
        # KHÔNG load transform ở đây nữa
        # transform = self.transformer.create() <-- XÓA DÒNG NÀY
        
        logging.info("Pipeline initialized in Hot-Reload mode.")

        def wrapped_callback(message):
            parsed_message = self.subscriber.parse_message(message)

            try:
                # CÁCH 2: Luôn load code mới nhất trước khi chạy
                # Điều này đảm bảo nếu Agent vừa sửa file, ta sẽ chạy code mới ngay
                current_transform_func = self.transformer.create()
                
                # Chạy transform
                transformed_data = current_transform_func(parsed_message)
                
            except Exception as e:
                # ... (Logic xử lý)
                logging.error(f"Loading error: {e}")
                self.subscriber.handle_error_message(message)
                return

            # time.sleep(2)
            # Acknowledge the message only after successful loading
            self.subscriber.acknowledge_message(message)
            return

        self.wrapped_callback = wrapped_callback

    def start(self):
        self._initialize()
        self.subscriber.subscribe(self.wrapped_callback)
