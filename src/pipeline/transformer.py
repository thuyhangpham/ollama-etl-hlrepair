from typing import Callable
from transform_tpl import transform 

class Transformer:
    def __init__(self):
        pass

    def create(self) -> Callable:
        """
        Create a callable transformer function that processes input data.

        :return: A callable function that takes input data and returns transformed data.
        """
        return transform