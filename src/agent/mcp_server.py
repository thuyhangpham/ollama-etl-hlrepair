import json
import sys
import logging
from kubecontrol import KubeControl
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

# Initialize FastMCP server
mcp = FastMCP("etl_transformation_bugfix")
kubecontrol = KubeControl(namespace="default")

@mcp.tool()
async def get_transformation_function(configmap: str) -> str:
    """Load the transformation function from the specified kube configmap.
    
    Args:
        configmap (str): The name of the Kubernetes ConfigMap to load the transformation function from.
    """
    try:
        # Retrieve the transformation function code from the specified ConfigMap
        cm_data = kubecontrol.get_configmap_data(configmap)
        if not cm_data or 'code' not in cm_data:
            raise ValueError(f"ConfigMap '{configmap}' does not contain data.")
        
        code = cm_data['code']
        logging.info("Transformation function loaded successfully from ConfigMap.")
        return code
    except Exception as e:
        logging.error(f"Error loading transformation function from ConfigMap: {e}")
        return str(e)
    
@mcp.tool()
async def test_transformation_function(code: str, input_data: dict) -> str:
    """Test the transformation function with the provided input data.
    
    Args:
        code (str): The transformation function code as a string.
        input_data (dict): The input data to be transformed, in JSON format.
    """

    try:
        # Create a local context to execute the transformation code
        local_context = {}
        exec(code, local_context)  # Execute the transformation code
        
        # Get the transformation function from the local context
        transform_func = local_context.get('transform')
        if not transform_func:
            raise ValueError("No 'transform' function found in the provided code.")
        
        # Call the transformation function with input data
        result = transform_func(input_data)
        logging.info("Transformation function executed successfully.")
        return json.dumps(result)  # Return result as JSON string
    except Exception as e:
        logging.error(f"Error in transformation function: {e}")
        return str(e)
    
@mcp.tool()
async def deploy_change(code: str, configmap: str, deployment_name: str) -> str:
    """Deploy the new transformation function code to kube configmap. And trigger a rolling restart of the deployment.
    
    Args:
        code (str): The new transformation function code as a string.
        configmap (str): The name of the Kubernetes ConfigMap to update with the new code.
        deployment_name (str): The name of the Kubernetes Deployment to restart.
    """
    try:
        # Update the specified ConfigMap with the new transformation function code
        updated_cm = kubecontrol.set_configmap_data(configmap=configmap, data={"code": code})
        if updated_cm:
            logging.info(f"Transformation function deployed successfully to ConfigMap '{configmap}'.")
        else:
            raise ValueError(f"Failed to update ConfigMap '{configmap}'.")
    except Exception as e:
        logging.error(f"Error deploying transformation function to ConfigMap: {e}")
        return str(e)
    
    try:
        # Trigger a rolling restart of the deployment to apply the new changes
        kubecontrol.restart_deployment(deployment_name)
        logging.info(f"Deployment '{deployment_name}' restarted successfully.")
        return "Configmap updated and Deployment restarted successfully."
    except Exception as e:
        logging.error(f"Error restarting Deployment '{deployment_name}': {e}")
        return str(e)


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')