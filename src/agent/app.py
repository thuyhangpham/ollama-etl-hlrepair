import os
import logging
import asyncio
import subprocess
import httpx
import re
from quart import Quart, request, jsonify

app = Quart(__name__)

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - AGENT - %(levelname)s - %(message)s')

# Cấu hình Ollama & Docker
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3") # Hoặc 'mistral', 'phi3'
ETL_CONTAINER_NAME = os.getenv("ETL_CONTAINER_NAME", "etl") # Tên service trong docker-compose
FUNCTION_FILE_PATH = "/app/function.py" # Đường dẫn file code được mount vào Agent

# Lock để tránh 2 lỗi xảy ra cùng lúc làm Agent loạn
processing_lock = asyncio.Lock()

# --- HELPER FUNCTIONS ---

def read_current_code():
    """Đọc nội dung file function.py hiện tại."""
    try:
        with open(FUNCTION_FILE_PATH, "r") as f:
            return f.read()
    except Exception as e:
        logging.error(f"Failed to read function file: {e}")
        return ""

def write_new_code(code_content):
    """Ghi đè code mới vào file."""
    try:
        with open(FUNCTION_FILE_PATH, "w") as f:
            f.write(code_content)
        logging.info("File function.py has been updated.")
        return True
    except Exception as e:
        logging.error(f"Failed to write function file: {e}")
        return False

def extract_python_code(llm_response: str):
    """
    Llama3 thường trả về markdown ```python ... ```. 
    Hàm này giúp lọc bỏ text thừa, chỉ lấy code.
    """
    pattern = r"```python(.*?)```"
    match = re.search(pattern, llm_response, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Nếu không có markdown, trả về nguyên gốc (có thể model trả code trần)
    return llm_response.strip()

async def call_ollama_to_fix(error_msg, traceback_str, bad_code, payload):
    """Gửi Prompt tới Ollama."""
    
    # Prompt được tối ưu cho Local Model (yêu cầu rõ ràng, ngắn gọn)
    prompt = f"""
    You are an expert Python Data Engineer. The following ETL transformation code failed.
    
    --- FAILED DATA INPUT ---
    {payload}
    
    --- ERROR MESSAGE ---
    {error_msg}
    
    --- TRACEBACK ---
    {traceback_str}
    
    --- CURRENT BROKEN CODE ---
    {bad_code}
    
    --- YOUR TASK ---
    1. Analyze why the code failed with the given input.
    2. Fix the python code to handle this edge case (e.g., use try-except, data validation, or type conversion).
    3. RETURN ONLY THE FULL VALID PYTHON CODE. DO NOT EXPLAIN. DO NOT RETURN MARKDOWN TEXT OUTSIDE THE CODE BLOCK.
    """

    logging.info(f"Sending request to Ollama ({OLLAMA_MODEL})...")
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.2 # Giữ nhiệt độ thấp để code chính xác
                    }
                }
            )
            response.raise_for_status()
            result = response.json()
            return result.get("response", "")
        except Exception as e:
            logging.error(f"Ollama connection failed: {e}")
            raise e

# async def restart_etl_container():
#     """Restart container ETL để load code mới."""
#     logging.info("Restarting ETL container...")
#     # Lưu ý: Container Agent cần mount docker socket (-v /var/run/docker.sock:/var/run/docker.sock) 
#     # để lệnh này hoạt động. Nếu không, ta cần cơ chế khác.
#     # Ở môi trường dev local, cài docker cli vào image agent là cách nhanh nhất.
#     try:
#         proc = await asyncio.create_subprocess_exec(
#             "docker", "restart", ETL_CONTAINER_NAME,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE
#         )
#         stdout, stderr = await proc.communicate()
#         if proc.returncode == 0:
#             logging.info(f"ETL Container restarted successfully.")
#         else:
#             logging.error(f"Failed to restart container: {stderr.decode()}")
#     except Exception as e:
#         logging.error(f"Error executing docker restart: {e}")

# --- API ROUTES ---

@app.route('/health', methods=['GET'])
async def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/transformation_error', methods=['POST'])
async def transformation_error():
    # 1. Check Lock
    if processing_lock.locked():
        logging.warning("Request ignored: Agent is already fixing code.")
        return jsonify({"status": "busy", "message": "Already processing a fix"}), 429

    data = await request.get_json()
    
    # Validation cơ bản
    if not data or 'error' not in data:
        return jsonify({"status": "error", "message": "Invalid payload"}), 400

    # 2. Define Background Task
    async def run_self_healing():
        async with processing_lock:
            logging.info("Starting Self-Healing Process...")
            
            # Bước A: Đọc code cũ
            current_code = read_current_code()
            if not current_code:
                logging.error("Aborting: Cannot read function.py")
                return

            # Bước B: Hỏi Ollama
            try:
                raw_response = await call_ollama_to_fix(
                    error_msg=data['error'],
                    traceback_str=data.get('traceback', ''),
                    bad_code=current_code,
                    payload=data.get('payload_data', {})
                )
            except Exception:
                return # Đã log lỗi bên trong hàm call_ollama

            # Bước C: Lọc lấy code sạch
            fixed_code = extract_python_code(raw_response)
            
            if not fixed_code or "def transform" not in fixed_code:
                logging.error("AI did not return valid python code.")
                logging.info(f"AI Response: {raw_response}")
                return

            # Bước D: Ghi code mới
            if write_new_code(fixed_code):
                # Bước E: Restart ETL Service
                await restart_etl_container()
            
            logging.info("Self-Healing Process Completed!")

    # 3. Start Task (Fire and Forget)
    asyncio.create_task(run_self_healing())

    return jsonify({"status": "accepted", "message": "Agent is working on the fix"}), 202

if __name__ == '__main__':
    # Chạy trên port 5000
    app.run(debug=True, host='0.0.0.0', port=5000)