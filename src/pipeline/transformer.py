import importlib.util
import sys
import os
import logging
from typing import Callable

class Transformer:
    def __init__(self, function_path: str = "function.py"):
        """
        :param function_path: Đường dẫn tới file chứa hàm transform (file này sẽ bị thay đổi bởi Agent).
        """
        # Nếu chạy trong Docker/Local, ta cần đảm bảo đường dẫn đúng.
        # Ở đây giả định file function.py nằm cùng thư mục với script này.
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.module_path = os.path.join(current_dir, function_path)

    def create(self) -> Callable:
        """
        Load hàm 'transform' từ file python bên ngoài một cách động (Dynamic Import).
        Điều này cho phép thay đổi logic code mà không cần build lại Image.
        """
        if not os.path.exists(self.module_path):
            raise FileNotFoundError(f"Transformation file not found at: {self.module_path}")

        try:
            # 1. Tạo spec để load module từ đường dẫn file
            spec = importlib.util.spec_from_file_location("dynamic_transform_module", self.module_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load spec from {self.module_path}")

            # 2. Tạo module từ spec
            module = importlib.util.module_from_spec(spec)
            
            # 3. Thực thi module (để định nghĩa các hàm bên trong nó)
            sys.modules["dynamic_transform_module"] = module
            spec.loader.exec_module(module)

            # 4. Lấy hàm 'transform' ra
            if not hasattr(module, "transform"):
                raise AttributeError(f"Function 'transform' not found in {self.module_path}")

            logging.info(f"Successfully loaded transformation logic from {self.module_path}")
            return module.transform

        except Exception as e:
            logging.error(f"Failed to load transformation function: {e}")
            raise e