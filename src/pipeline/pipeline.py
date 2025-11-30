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
        # Create transform function
        transform = self.transformer.create()

        def wrapped_callback(message):
            parsed_message = self.subscriber.parse_message(message)

            try:
                transformed_data = transform(parsed_message)
            except Exception as e:
                logging.error(f"Transformation error: {e}")
                self.agent_hook.call_agent_hook(
                    error=str(e),
                    payload_data=parsed_message
                )

                if self.error_delay < 0:
                    # infinite wait
                    time.sleep(10**9)
                elif self.error_delay > 0:
                    time.sleep(self.error_delay)
                self.subscriber.handle_error_message(message)
                return
            
            try:
                self.loader.load(transformed_data)
            except Exception as e:
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
