import logging

class Loader:
    def __init__(self):
        pass

    def load(self, data: dict):
        """
        Load the transformed data to the destination.

        :param data: The transformed data to be loaded.
        :raises NotImplementedError: If the load method is not implemented.
        """
        logging.info(f"Data loaded: {data}")