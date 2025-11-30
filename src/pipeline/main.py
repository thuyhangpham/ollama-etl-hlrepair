import os
import sys
import logging
from dotenv import load_dotenv
load_dotenv()
from pipeline import Pipeline
from transformer import Transformer
from loader import Loader
from agent_hook import AgentHook



def main():
    CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER")
    if not CLOUD_PROVIDER:
        raise ValueError("CLOUD_PROVIDER environment variable is not set.")
    
    subscriber = None

    if CLOUD_PROVIDER == "GOOGLE":
        import google.cloud.logging
        client = google.cloud.logging.Client()
        client.setup_logging()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
        
        from subscriber import PubSubSubscriber as Subscriber
        subscriber = Subscriber(
            project_id=os.getenv("GCP_PROJECT_ID"),
            subscription_id=os.getenv("GCP_SUBSCRIPTION_ID"),
            topic_id=os.getenv("GCP_TOPIC_ID"),
        )

    elif CLOUD_PROVIDER == "AWS":
        raise NotImplementedError("AWS Subscriber is not implemented yet.")
    
    else:
        raise ValueError(f"Unsupported CLOUD_PROVIDER: {CLOUD_PROVIDER}. Supported providers are GOOGLE and AWS.")

    transformer = Transformer()
    loader = Loader()
    agent_hook = AgentHook(
        webhook_url=os.getenv("AGENT_SERVICE_URL"),
    )

    pipeline = Pipeline(
        subscriber=subscriber,
        transformer=transformer,
        loader=loader,
        agent_hook=agent_hook,
        error_delay=int(os.getenv("ERROR_DELAY", -1))
    )

    # Start the pipeline
    pipeline.start()

if __name__ == "__main__":
    main()