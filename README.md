# Self-Healing ETL Pipelines with LLM-Driven Transformation Repair

This repository contains the code for my blog:
[Self-Healing ETL Pipelines with LLM-Driven Transformation Repair](https://medium.com/@pateljheel/self-healing-etl-pipelines-with-llm-driven-transformation-repair-f02ee089b89c).

In this project, I demonstrate how to build a self-healing ETL pipeline that uses LLM agents to automatically fix transformation logic during schema drift events in a cloud-native environment.

## Setup

1. Deploy the GKE Cluster and Configure Resources
  
    Use the provided Terraform scripts in the `infra/gke` directory. These scripts set up the GKE cluster, service accounts, and necessary permissions.

    ```bash
    cd infra/gke
    terraform apply
    ```
    Make sure to update `terraform.tfvars` with your project-specific configurations before deploying.

2. Create a Kubernetes Secret for the Anthropic API Key

    ```bash
    kubectl create secret generic anthropic-api-key \
      --from-literal=ANTHROPIC_API_KEY='your-real-api-key'
    ```

3. Update Kubernetes Manifests

    Modify the environment variables in the Kubernetes manifests under the k8s directory to match your project setup.

4. Deploy the ETL and Agent Components

    ```bash
    cd k8s
    kubectl apply -f agent.yaml
    kubectl apply -f etl.yaml
    ```

5. Publish Test Messages

    Use the provided test script to publish test messages and observe how the agent autonomously updates the deployment when a schema change causes a transformation failure.

    ```bash
    cd test
    export GCP_PROJECT_ID=YOUR_GCP_PROJECT_ID
    export GCP_TOPIC_ID=mcp-etl-test-topic
    python3 publish.py
    ```

## Sample Data

For testing, I used publicly available sample JSON data from: https://learn.microsoft.com/en-us/microsoft-edge/web-platform/json-viewer