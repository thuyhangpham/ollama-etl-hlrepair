import logging
import boto3
import json
from typing import Any
from google.cloud import pubsub_v1
from typing import Callable, Optional
from botocore.exceptions import ClientError


class Subscriber:
    def __init__(self):
        """
        Base class for message consumers.

        :param callback: Callable that will process each message.
        """
        raise NotImplementedError("This class should not be instantiated directly. Use a subclass instead.")
    
    def subscribe(self, callback: Callable) -> None:
        """
        Subscribe to a message queue or topic.
        To be implemented in subclass.

        :param callback: A callable that will process each message.
        """
        raise NotImplementedError("The 'subscribe' method must be implemented in the subclass.")
    
    def parse_message(self, message: Any) -> dict:
        """
        Parse the message into a dictionary.
        This method should be overridden in subclasses if needed.

        :param message: The message to be parsed.
        :return: Parsed message as a dictionary.
        """
        raise NotImplementedError("The 'parse_message' method is not implemented in the base class. Use a subclass if needed.")
    
    def acknowledge_message(self, message: Any) -> None:
        """
        Acknowledge the message to mark it as processed.
        This method is not implemented in the base class.

        :param message: The message to be acknowledged.
        """
        raise NotImplementedError("The 'acknowledge_message' method is not implemented in the base class. Use a subclass if needed.")
    
    def handle_error_message(self, message: Any) -> None:
        """
        Publish a message to the topic or queue.
        This method is not implemented in the base class.

        :param message: The message to be published.
        """
        raise NotImplementedError("The 'handle_error_message' method is not implemented in the base class. Use a subclass if needed.")
    

class PubSubSubscriber(Subscriber):
    def __init__(self, project_id: str, subscription_id: str, topic_id: str, timeout: Optional[int] = None):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        self.timeout = timeout

    def subscribe(self, callback: Callable):
        """
        Subscribe to a Google Cloud Pub/Sub subscription.

        :param callback: A callable that will process each message.
        """
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(self.project_id, self.subscription_id)

        # Define flow control to limit concurrent messages
        # for testing purposes, we set max_messages to 1
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=1
        )

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
        logging.info(f"Listening for messages on {subscription_path}...")

        with subscriber:
            try:
                streaming_pull_future.result(timeout=self.timeout)
            except Exception as e:
                logging.error(f"Error in streaming pull: {e}")
                streaming_pull_future.cancel()
                streaming_pull_future.result()

    def parse_message(self, message: pubsub_v1.subscriber.message.Message) -> dict:
        """
        Parse the Pub/Sub message into a dictionary.

        :param message: The Pub/Sub message to be parsed.
        :return: Parsed message as a dictionary.
        """
        try:
            return json.loads(message.data.decode("utf-8")) if message.data else {}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse message: {e}")
            raise e
        
    def acknowledge_message(self, message: pubsub_v1.subscriber.message.Message):
        """
        Acknowledge the Pub/Sub message to mark it as processed.

        :param message: The Pub/Sub message to be acknowledged.
        """
        try:
            message.ack()
            logging.info("Message acknowledged successfully.")
        except Exception as e:
            logging.error(f"Failed to acknowledge message: {e}")
            raise e

    def handle_error_message(self, message: pubsub_v1.subscriber.message.Message):
        """
        Handle error messages by publishing them to the topic.

        :param message: The Pub/Sub message to be published.
        """
        try:
            # Here you can implement your logic to handle error messages
            logging.error(f"Handling error message: {message.data.decode('utf-8')}")
            self.publisher.publish(self.topic_path, message.data)
            message.ack()
        except Exception as e:
            logging.error(f"Failed to handle error message: {e}")
            raise e

