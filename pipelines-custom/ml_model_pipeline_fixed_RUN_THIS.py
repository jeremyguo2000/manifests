from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client
import time

# --- Configuration ---
# Ensure this MinIO endpoint includes http:// or https://
MINIO_ENDPOINT = "http://minio-service.kubeflow:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "mlpipeline"
# Pipeline root for KFP artifacts. Use s3:// for MinIO.
PIPELINE_ROOT = f"s3://{MINIO_BUCKET}/artifacts"
MODEL_SAVE_PATH_PREFIX = f"s3://{MINIO_BUCKET}/models" # This is used by train_and_save_model for consistency
KUBEFLOW_PIPELINES_HOST = "http://10.254.50.50:8080/pipeline"
KUBEFLOW_NAMESPACE = "kubeflow-user"

# --- IMPORTANT: REPLACE THIS PLACEHOLDER WITH YOUR ACTUAL COOKIE VALUE ---
# You can find this in your browser's developer tools under Application > Cookies
# when logged into Kubeflow. Look for 'oauth2_proxy_kubeflow'.
OAUTH2_PROXY_COOKIE = "q5vRYGQQ1paoxMi-t0wEjivQRVqdh1YP287bKGGsRoydj-5a0m2GI31cWZ8D1tL1j8i4M7ZPhwDWo-hvkvkjvsKwzPzI0o_U-a05LqxiwC6wj-UTwHa0P-a3bzfxTUQJUWq88qsgQ9vWt_logTrgPgM-da5RwYuFWW3BhqKDHeAhsaxF4AFNswbj_7APNAoFWhdqbVk6Xzfn-LhHb_5XveYgkVlSFUwhKDI4wrFrSvKsVeutjhS9Utj4QCCUDSpkhIGMa4-YFn7t8I6pJy2Aeqp45IbAcwc1pnwfi2j3a7r59dTXTbdcX3H0F_4iH-sI2_MkWqAachaPLOSeSrZ-vSLA4URjaGG63inqF9a1rNtT8ik_s_wllyM_xREOx4tsVKbtv7Z1xht_0epwj0EZObjJorhpfCQULe-yN5y67hyZ7-kys7tk68-4hIZbwD3nEXJk9wA-b-hukE2rFljSXHFBCX26JR169FGBqWCB6rvO_sNpLeTEDxGZ_cFbq3XJUTdCopj9nlc8jd2K14rHelZN6FahVNVf6Cy9VfpUhcJttc7LkfpLlHpXGdApNKlHXRqbVK7I85eUUBE7dBKfYQ83WxYI2M0cpiDRibTn0ytMlAhlaatP1mC2HfNyysOnIuG0gcrWEfXNmoHVZzyF2tV1tfoYnvC00LDR0MbBmO7dguXS9NE2aekU_IWzfKXhFdB70G8dlFswt7QWlfVvUMi8tCL1sWw_V0HOTxzZ7fCg8sYMLXoEpqcT4s129KrOC9-1M4kffE-E1Iad4E4363PxqmJ3viZ0AV4JdG1lMJy6XZJh7k1aEMJjUR2FMphGCQDvHHuo4ZZTDh6ftbH8OvtzGINeU22LGK8is2ggpz4H2W9akdahhVqSPxybu8kBzme4EUGM6DF9q0she8QU3cuJfqefsmoP3Y23fon-JSGUvEGoGzhDrmFZYjNWbSjGDl71-bIRbiCqZ0lqeMDebNhQlyj3aFdi-FY1DEDNNCKi8M2zvJiz11YsDjAmqJNNE5xM6qOD64y0i6AHQKnQhc8KijNveSQ-sqPmpHkD99d5xp1Ca9OtLRiJtXzQVn8sQn0ptqCRQTC4_UllJnUfDuCo2TdFQq2VdzNbgcqH8IwLaZqOOWdOSg0IYYuG0JtvXHK8ATDWaNckJ85tlG0x_6vyOZ5ZdsAZHCHzDj3Dokl035edh7kvUOzd3mywO54ig0qHkv10nNQFUJh_fwPM7PqIBsJD2EztHOjYhKnJ2YLWyuErbopMXRrYv41RdL3CARZtriHFe-n6uCQQiNXY2DDBDDfLBur85LSCYPK82fF4dG2TvcQDGVlOIJHR-H6TQWjxO7b0K91dJJhXLee1IlYn9I0AHoYBE3zMypJmvbuaibFwAirdO48FtoYh0GbJKt3ar5fATFUkcBxc0dCKL_jpj3U9xpCz_Cwfwu_L24suFyi0F6Y2JqNS4upZc12ADu-CCOtN0j32RKcNHESunI__4rx2bTDW88ENNTLkJ42-JjUJ39vSEqEY91hBcHYrIgUnxYoBKX42XWGQl3RfG6jOyz8u60BdW5fs1Q7S8Gql_R66Vy3Ae2AXwDQdWLSU-Ur5-og54NdKoOSnEl4M30e4rc4GS6tH4FvgUUhxhRxKyrCdZf_oRDT14YtWltnleEsdLY1_pn4eumPrBzTxhHBp7n-nwR7IfUO5R5OTdKVdN6B0QMO4zys6oeMMmkh1qAdCqTBI-0P34X93-iDGMAJyL2HHjUADiL6U9QRe0xJaaA8cF4X3EoLwjvpMjQtJaMnfSe3Bxke6XZth3elFu8dWqwSlfL8UywSqiKYitOWJtkrVBnskAGGP7zFV6j-rTQQB35dfc-EFfhGSi151f5UfCiurYwcrQX1VgedpaVhKgWRyuJVqyqawTETpTahz_HTo9UkoXiX5JrmAJ6ZHBkYgPbpf73qtf2wpzwpCsrEd7yMfbbqchWgg0EBSQaDDgPzoefBDiTj6DYUqn5DcPe3U-URc4bE4caMqebpldh3HGPMUwVerhC98_yKmDDn6hpYkgW8XueP1GljsnDnaqxJiIksqfZkiSUIMmlakFcsFFzm3cMYccDsBwXKb14IZrJK-31rYNSI0phuq-hDYh-WV50bBS60ZHlbUjk3iXeeJK41g4EJMpo9REHFn_cC8J38hQX3C3MmK--9stroEVIARluwS7-LjgkLVkpjX7r1AkuzZ8wdeEeAdPfzpQkglsH1QvBws0Mk=|1750131241|PXfmDgUa8oKrN3gc-IioayQPXIWnwUwrgMpJBqWkItg="

# --- Kubeflow Pipeline Components ---

@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib"]
)
def prepare_data(data: dsl.OutputPath(dsl.Dataset)):
    """
    Generates a simple synthetic dataset and saves it.
    """
    import joblib
    from sklearn.datasets import make_regression
    import os

    X, y = make_regression(n_samples=100, n_features=1, noise=20, random_state=42)
    dataset_content = {'X': X, 'y': y}

    joblib.dump(dataset_content, data)
    print(f"Synthetic data prepared and saved to {data}")


@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib", "minio"]
)
def train_and_save_model(
    data: dsl.InputPath(dsl.Dataset),
    model_name: str,
    minio_endpoint: str, # Full URL, e.g., http://minio-service.kubeflow:9000
    minio_access_key: str,
    minio_secret_key: str,
    minio_bucket: str,
    model_save_path_prefix: str, # Not directly used for KServe URI, but good for clarity
) -> str:
    """
    Trains a simple Scikit-learn Linear Regression model and saves it to MinIO.
    Returns the s3:// URI for KServe deployment.
    """
    import joblib
    from sklearn.linear_model import LinearRegression
    from minio import Minio
    from minio.error import S3Error
    import os

    dataset_content = joblib.load(data)
    X = dataset_content['X']
    y = dataset_content['y']

    model = LinearRegression()
    model.fit(X, y)
    print("Model training complete.")

    local_model_filename = "model.joblib"
    local_model_path = os.path.join("/tmp", local_model_filename)
    joblib.dump(model, local_model_path)
    print(f"Model saved locally to {local_model_path}")

    try:
        # MinIO client needs the endpoint without http/https prefix for its constructor
        minio_client_endpoint = minio_endpoint.replace("http://", "").replace("https://", "")
        client = Minio(
            minio_client_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False # Set to True if your MinIO uses HTTPS
        )

        object_name = f"{model_name}/model.joblib" # KServe expects model artifact directly under this path.
                                                  # You might need to adjust if your model structure is different.

        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)
            print(f"Bucket '{minio_bucket}' created.")

        client.fput_object(minio_bucket, object_name, local_model_path)
        print(f"Model '{object_name}' uploaded successfully to MinIO bucket '{minio_bucket}'.")

        # KServe storage URI should point to the directory containing the model artifact
        kserve_storage_uri = f"s3://{minio_bucket}/{model_name}"
        print(f"KServe storage URI for deployment: {kserve_storage_uri}")
        return kserve_storage_uri

    except S3Error as err:
        print(f"Error uploading to MinIO: {err}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during model saving: {e}")
        raise

# This is now a Kubeflow Pipeline Component
# Updated deploy_model component with correct KServe configuration
@dsl.component(
    base_image="python:3.10-slim-buster",
    packages_to_install=[
        "kubernetes==29.0.0",
        "PyYAML==6.0.1"
    ]
)
def deploy_model(
    model_name: str,
    model_storage_uri: str,
    namespace: str,
    minio_endpoint: str,
    kserve_group: str = "serving.kserve.io",
    kserve_version: str = "v1beta1",
    minio_secret_name: str = "minio-credentials"
) -> str:
    """
    Deploys a machine learning model using KServe with proper MinIO configuration.
    """
    from kubernetes import client, config
    import json
    import time
    import yaml

    try:
        # Load in-cluster config
        try:
            config.load_incluster_config()
            print("Loaded in-cluster Kubernetes config")
        except config.config_exception.ConfigException:
            try:
                config.load_kube_config()
                print("Loaded local Kubernetes config")
            except Exception as e:
                print(f"Failed to load any Kubernetes config: {e}")
                raise

        api_client = client.ApiClient()

        # Define the corrected KServe InferenceService manifest
        inferenceservice_manifest = {
            "apiVersion": f"{kserve_group}/{kserve_version}",
            "kind": "InferenceService",
            "metadata": {
                "name": model_name,
                "namespace": namespace
            },
            "spec": {
                "predictor": {
                    "serviceAccountName": "default",  # Ensure service account has access
                    "model": {  # Changed from "sklearn" to "model" with modelFormat
                        "modelFormat": {
                            "name": "sklearn"
                        },
                        "storageUri": model_storage_uri,
                        "resources": {
                            "requests": {
                                "cpu": "100m",
                                "memory": "512Mi"
                            },
                            "limits": {
                                "cpu": "1",
                                "memory": "1Gi"
                            }
                        },
                        "env": [
                            {
                                "name": "AWS_ACCESS_KEY_ID",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": minio_secret_name,
                                        "key": "AWS_ACCESS_KEY_ID"
                                    }
                                }
                            },
                            {
                                "name": "AWS_SECRET_ACCESS_KEY",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": minio_secret_name,
                                        "key": "AWS_SECRET_ACCESS_KEY"
                                    }
                                }
                            },
                            {
                                "name": "AWS_ENDPOINT_URL",
                                "value": minio_endpoint
                            },
                            {
                                "name": "AWS_REGION",
                                "value": "us-east-1"
                            }
                        ]
                    }
                }
            }
        }

        print(f"Attempting to deploy InferenceService: {model_name} in namespace: {namespace}")
        print(f"Model Storage URI: {model_storage_uri}")

        custom_api = client.CustomObjectsApi(api_client)

        try:
            # Try to create the InferenceService
            custom_api.create_namespaced_custom_object(
                group=kserve_group,
                version=kserve_version,
                namespace=namespace,
                plural="inferenceservices",
                body=inferenceservice_manifest
            )
            print(f"InferenceService {model_name} created successfully.")
        except client.exceptions.ApiException as e:
            if e.status == 409:  # Conflict - resource already exists
                print(f"InferenceService {model_name} already exists. Attempting to update...")
                custom_api.patch_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    namespace=namespace,
                    plural="inferenceservices",
                    name=model_name,
                    body=inferenceservice_manifest
                )
                print(f"InferenceService {model_name} updated successfully.")
            else:
                print(f"Error creating/updating InferenceService: {e}")
                raise

        # Wait for the InferenceService to become ready (simplified)
        print(f"Waiting for InferenceService {model_name} to be ready...")
        timeout_seconds = 600
        poll_interval_seconds = 30
        start_time = time.time()
        service_url = None

        while True:
            if (time.time() - start_time) > timeout_seconds:
                print(f"InferenceService {model_name} did not become ready within {timeout_seconds} seconds.")
                # Don't raise error, return a default URL for testing
                break

            try:
                status_response = custom_api.get_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    namespace=namespace,
                    plural="inferenceservices",
                    name=model_name
                )

                is_ready = False
                if 'status' in status_response:
                    status = status_response['status']
                    if 'conditions' in status:
                        conditions = status['conditions']
                        for condition in conditions:
                            if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                                is_ready = True
                                break

                    if 'address' in status and 'url' in status['address']:
                        service_url = status['address']['url']

                if is_ready and service_url:
                    print(f"InferenceService {model_name} is ready.")
                    break
                else:
                    print(f"Still waiting for {model_name} to be ready...")

                time.sleep(poll_interval_seconds)

            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(f"InferenceService {model_name} not found yet, retrying...")
                    time.sleep(poll_interval_seconds)
                else:
                    print(f"Kubernetes API error: {e}")
                    break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

        # Construct service URL if not available
        if not service_url:
            service_url = f"http://{model_name}-predictor-default.{namespace}.svc.cluster.local/v1/models/{model_name}:predict"
            print(f"Using constructed service URL: {service_url}")

        print(f"Model serving URL: {service_url}")
        return service_url

    except Exception as e:
        print(f"An unexpected error occurred during model deployment: {e}")
        raise


@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["requests", "numpy"]
)
def test_prediction(
    model_serving_url: str,
    prediction_result_path: dsl.OutputPath(str)
):
    """
    Tests the deployed model by sending a sample prediction request.
    """
    import requests
    import numpy as np
    import json
    import time

    print(f"Testing model at URL: {model_serving_url}")

    # Sample input data for a single-feature regression model
    sample_input = [[5.0]]
    headers = {'Content-Type': 'application/json'}
    payload = {
        "instances": sample_input
    }

    # Retry mechanism for prediction testing
    max_retries = 10 # Increased retries
    retry_delay = 15 # Reduced delay slightly

    for attempt in range(max_retries):
        try:
            print(f"Prediction attempt {attempt + 1}/{max_retries} to {model_serving_url}")
            response = requests.post(model_serving_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()  # Raise an exception for HTTP errors

            predictions = response.json().get('predictions')

            if predictions is None:
                raise ValueError("No 'predictions' key found in the response JSON.")

            print(f"Prediction successful!")
            print(f"Input: {sample_input}")
            print(f"Predictions: {predictions}")

            with open(prediction_result_path, "w") as f:
                f.write(json.dumps({
                    "input": sample_input,
                    "predictions": predictions,
                    "status": "success"
                }))
            
            return  # Success, exit the function

        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Error during prediction request (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("All prediction attempts failed.")
                # Write failure result
                with open(prediction_result_path, "w") as f:
                    f.write(json.dumps({
                        "input": sample_input,
                        "predictions": None,
                        "status": "failed",
                        "error": str(e)
                    }))
                raise
        except Exception as e:
            print(f"An unexpected error occurred during prediction testing: {e}")
            with open(prediction_result_path, "w") as f:
                f.write(json.dumps({
                    "input": sample_input,
                    "predictions": None,
                    "status": "failed",
                    "error": str(e)
                }))
            raise


# --- Kubeflow Pipeline Definition ---

@dsl.pipeline(
    name="ML Model Lifecycle Pipeline",
    description="A pipeline to prepare data, train, save, deploy, and test a simple ML model.",
    pipeline_root=PIPELINE_ROOT # This sets the default artifact storage
)
def ml_model_lifecycle_pipeline(
    model_name: str = "my-sklearn-regression-model",
    namespace: str = KUBEFLOW_NAMESPACE,
    minio_endpoint: str = MINIO_ENDPOINT # Pass MinIO endpoint to the pipeline
):
    """
    Defines the end-to-end ML model lifecycle pipeline.
    """
    # Step 1: Prepare data
    prepare_data_task = prepare_data()

    # Step 2: Train and save model
    train_and_save_model_task = train_and_save_model(
        data=prepare_data_task.outputs['data'],
        model_name=model_name,
        minio_endpoint=minio_endpoint, # Use the pipeline parameter
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_bucket=MINIO_BUCKET,
        model_save_path_prefix=MODEL_SAVE_PATH_PREFIX
    )

    # Step 3: Deploy model
    deploy_model_task = deploy_model(
        model_storage_uri=train_and_save_model_task.output,
        model_name=model_name,
        namespace=namespace,
        minio_endpoint=minio_endpoint # Pass MinIO endpoint to the deploy_model component
    )

    # Step 4: Test prediction
    test_prediction_task = test_prediction(
        model_serving_url=deploy_model_task.output
    )
    test_prediction_task.after(deploy_model_task) # Ensure test runs only after deployment succeeds


# --- Main execution block ---
if __name__ == "__main__":
    # Check if cookie is set
    if OAUTH2_PROXY_COOKIE == "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE":
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value.     ")
        print("!!!          You can find this in your browser's developer tools        ")
        print("!!!          under Application > Cookies when logged into Kubeflow.     \n")
        exit(1)

    # Compile pipeline
    pipeline_filename = "ml_model_lifecycle_pipeline.yaml"
    Compiler().compile(ml_model_lifecycle_pipeline, package_path=pipeline_filename)
    print(f"Kubeflow Pipeline compiled to '{pipeline_filename}'")

    # Connect to Kubeflow Pipelines
    print(f"Connecting to Kubeflow Pipelines at: {KUBEFLOW_PIPELINES_HOST}")
    try:
        # Client constructor takes host and then cookies directly
        client = Client(host=KUBEFLOW_PIPELINES_HOST, cookies=f"oauth2_proxy_kubeflow={OAUTH2_PROXY_COOKIE}")
    except Exception as e:
        print(f"Error connecting to KFP client. Check your host URL and cookie: {e}")
        exit(1)

    # Create or get experiment
    experiment_name = "Model Lifecycle Demo"
    try:
        experiment = client.get_experiment(experiment_name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Using existing experiment: '{experiment_name}' (ID: {experiment.experiment_id})")
    except:
        experiment = client.create_experiment(name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Created new experiment: '{experiment_name}' (ID: {experiment.experiment_id})")

    # Submit pipeline run
    run = client.create_run_from_pipeline_package(
        pipeline_file=pipeline_filename,
        arguments={
            # Dynamically set model name to avoid conflicts on repeated runs
            'model_name': f'my-sklearn-regression-model-{int(time.time())}',
            'namespace': KUBEFLOW_NAMESPACE,
            'minio_endpoint': MINIO_ENDPOINT # Pass MinIO endpoint as an argument
        },
        experiment_id=experiment.experiment_id,
        namespace=KUBEFLOW_NAMESPACE
    )
    print(f"Pipeline run submitted: {run.run_id}")
    print(f"View run details in Kubeflow Dashboard: {KUBEFLOW_PIPELINES_HOST}/#/runs/details/{run.run_id}")