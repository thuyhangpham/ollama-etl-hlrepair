import logging
import json
import os

class Loader:
    def __init__(self, output_path: str = "output/data_warehouse.jsonl"):
        """
        Khởi tạo Loader.
        :param output_path: Đường dẫn file lưu kết quả (giả lập Data Warehouse).
        """
        self.output_path = output_path
        
        # Tạo thư mục output nếu chưa có
        if not os.path.exists(os.path.dirname(self.output_path)):
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    def load(self, data: dict):
        """
        Ghi dữ liệu đã transform vào file local (append mode).
        
        :param data: Dữ liệu dictionary sau khi đã transform.
        """
        try:
            # Ghi vào file dưới dạng JSON Lines (mỗi dòng 1 json)
            with open(self.output_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
            
            logging.info(f"Data loaded to {self.output_path}: {data}")
            
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise e