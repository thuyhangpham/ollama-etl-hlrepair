import atexit
import asyncio
from quart import Quart, request, jsonify
from mcp_client import MCPClient

app = Quart(__name__)
mcp_client = MCPClient()
PROMPT_TEMPLATE_FILE = "./prompt.tpl"
MCP_SERVER_SCRIPT = "./mcp_server.py"

with open(PROMPT_TEMPLATE_FILE, "r") as f:
    prompt_template = f.read()

processing_lock = asyncio.Lock()

# Initialize MCP client on startup - now just storing the server path
@app.before_serving
async def init_mcp():
    try:
        await mcp_client.initialize(MCP_SERVER_SCRIPT)
        print("MCP client initialized.")
    except Exception as e:
        print(f"Failed to initialize MCP client: {e}")

# Clean up resources on shutdown
@atexit.register
def shutdown():
    print("Resources cleaned up on shutdown.")

@app.route('/transformation_error', methods=['POST'])
async def transformation_error():
    if processing_lock.locked():
        return jsonify({"status": "busy", "message": "Already processing a request"}), 201

    data = await request.get_json()

    async def handle_error(data):
        async with processing_lock:
            prompt = f"""
            Received Error:
            Payload: {data['payload_data']}
            Error: {data['error']}
            Traceback: {data['traceback']}
            {prompt_template}
            """
            print("_______________________________________________________________________________________________")
            print(prompt, flush=True)

            try:
                # Each call to process_query will create its own session
                result = await mcp_client.process_query(prompt)
                print(f"Result: {result}", flush=True)
            except Exception as e:
                print(f"Error processing query: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500
            print("_______________________________________________________________________________________________")
            
    # Start background task
    asyncio.create_task(handle_error(data))

    return jsonify({"status": "success", "message": "Alert accepted and processing"}), 200


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)