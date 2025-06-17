import kfp
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
OAUTH2_PROXY_COOKIE = "lnfhtV6KznX0yj_Su-GwbkZMH3sfJsleROg4SrVrkZoUwlByIsFWYGaobvKbln4uj1fLbWUrTnUPDyN9k_MeF9DevT2V6__Zl7mnxQwiZeMtXrOzS_RSc4k8PeZpfszFvDrT4E3_Dh_0Br2PbNZP7kg_7koTkhYwR0nOrkeni91IsAjipv9_tn153JpAhD9PCiy3DwFRDNqJF9glJOXfkDWmQ2_o1NsKFLJZ75FdXosMiQ6zKdpzHSW6hXPsUCLwvjm6PlJ1mHvgVEpxZczMvFbu4t5KzZTLEZjTAytduBVypRmd4r_ITexq2zkRJNgxumRpmIxq_y7tJ6wInlph3ZkM90-dSgxCeYlfDuGDiiGGMOJkAPuO0S1C5NGusjdLTrDlCIVHwflEyVnWra17fDTLqD5dPG8KPTUt3TFsVApHg_oySiJtaG-FUXXJtncdwyfe5xURti8UZADQiqc_s-wFoJcs9o11GgkYqtfYtAJvkMAiHArfrzjbKW_DViOK7o8dBkV1jP62CvgW3x7P_SOnIU_1E-76IE-Ca_DuXqP2hjUHzBhEWJbtBkJivg1z_xZZ086K7zw29O2mRz0Vqfjj3ThRmtNVAsBNRwvC1pVMvrsSia876mhG5jepEVuZwAOR6r9lcuPMNs4GfOJ3YUCIJVEi-aQrhH4QPJ1msAoudzA2pY4xK1xV0qzZo3YLuZvWV2tJyD82FmX_eNuA5F_-hWE_UrzUQi6s96rR4sC4EleaYGyvZYjucnjg_JRaYu4rpYl4Ysmr8SlxTo85yqFZhuYlrMtuVzb-yQmT-MUCa-pFRkQn_PXHR5A7SmpERXpn2MZTJvYaExGtFvEZyU08weeLPdxF3_CwUqQI6JxPjQlTE8Mhi8bovDpwrPTKNKJd99tH4cotGQ2AnCV_PWFsaupA7WvKDysdZjC76dk_mTwEGBWCEnYo-NjEtfGxEuPP7KaCpy31-6dpnot1Kc_zlM5HtThFxSwFL20NNwNJAi5tpbi7uW_A6rl8vLWZ3COCkhbzsuGeebLeEEk_HCmqPY4psqmtvrjz5xB7EaQKapecs3I9a3LI021Lh0H-xoJ4em0eFeXI8msCkCBhGHntZ7n6ApVIRZWdnH9z2R3URexQiKG8Z4PRoZQx6BMUZzwsEMRb58vXxOUqKqjPlgAqq7-tLfyerkH9zc7H9fyZI3VR8brQO1BeiC-qYjAn3oeyWKJVJiPzg5uGr3jh6kBBs5iF1flTYLxnyeEEOiJOj1bUoRSEWKnKuWKze2CMItpY9eXIAk6ehnr258rINEOAv8-DYHLACmDBKIRr3qxzU3KAlh1iQMCwIm0QoWxRPZNb17_mCVe5wchjRHEmgsLVi1lWdeOxjVaeJ1-LeLUOGZXXB71D2Jl_daXSwYIbwlhOc56kiq_cnJQNBZKwhBlY27HMy-tWP371PK43GJIiXV2dngVtw0X9hSOSrc7hCvz-POS5FePjE1XmtPCYTGdQE7UkHjQN2ScLOwLlRNPfmd6D_oVSzZJMaeztpbT-YenKrff-I1zh9euZbZ-lJViVYKvT8PNIKwMv9eXguoKhf0DmjPhvD9o_5sPDOzbUMywDYarH_ifxq74tQ1Ge9-nStoladFVU6AkPopkhQpnQ3QU-2C-QBVKTsbSBIgi273_JS4raDh_JLmJaQXs6s5EoVsk4SHhRzAcelFuZ_j0Qkfsg2mE4SU0m2psvOwdO6gJ8lBIg400qJqbQJX0nSR6-16djD__od1BOezXqfFdVJ_IGTD5ECIWrAed1J9TdXyoevMZaqwF6THUI2kxJqyA1Z_dQOQ4ogXcsE37mWbKphanw_WetVMSOv4e5Vg2UcrR47Mn9NQhufWqOIj7_VmkblKDloIycwoF5MRF0U-vAO6VeCrt7tCxblvWSckGSnn5acm_XUjvzPpGqp9vUVWZgPhFdJwQTENFTevDXA5ESZ-RUAWjTgaUcXOLj_d4aobxINhyaj2XpSUqJTdlObQG23mVf7A7JiSMqNxc0T11KjWrBFL2o_qHLEkgeHPxFIlxe29zdu96qvVcNSO7VL6e15wmhOwRDWnEtcOhfM84Q1RboVErbb2HjqNk18Pj2lP5MhZzQ5lbfETrz8jA6sYPY2dUiSfJ3xbDCcAPS-lKiFDWwFP9m32wOEXP7KRjlZKwVj3xBCxezsslnEvO8VUlnYw0sgLIsJXFcNyZCy4iLuYfVO2tXl5f9hBlQfDzpA9ly_hPQhbPsFUOkxm_2Ian6pwyJwfWH|1750042849|t2vd2m3DJk91YE14oGTfkTzX3YrR32QdEoGnnPOnq3I="

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
@dsl.component(
    base_image="python:3.10-slim-buster", # Ensure this image has kubectl/kubernetes client if needed, or proper python libs
    packages_to_install=[
        "kubernetes==29.0.0", # Adjust version if needed for your K8s cluster
        "PyYAML==6.0.1"
    ]
)
def deploy_model(
    model_name: str,
    model_storage_uri: str,
    namespace: str,
    minio_endpoint: str, # This should be the full URL (e.g., http://minio-service.kubeflow:9000)
    kserve_group: str = "serving.kserve.io",
    kserve_version: str = "v1beta1",
    minio_secret_name: str = "minio-credentials"
) -> str:
    """
    Deploys a machine learning model using KServe.
    This function's code will run INSIDE a Kubernetes pod as a KFP component.
    """
    # All imports required by this component function MUST be inside the function
    from kubernetes import client, config
    import json
    import time
    import yaml
    import base64

    try:
        # Load in-cluster config for KFP component running inside Kubernetes.
        try:
            config.load_incluster_config()
            print("Loaded in-cluster Kubernetes config")
        except config.config_exception.ConfigException:
            # Fallback for local testing or specific environments where in-cluster config isn't available
            try:
                config.load_kube_config()
                print("Loaded local Kubernetes config")
            except Exception as e:
                print(f"Failed to load any Kubernetes config: {e}")
                raise

        api_client = client.ApiClient()
        v1 = client.CoreV1Api(api_client)

        # Retrieve MinIO credentials from the Kubernetes secret (minio-credentials)
        minio_access_key = ""
        minio_secret_key = ""
        try:
            minio_secret = v1.read_namespaced_secret(name=minio_secret_name, namespace=namespace)
            minio_access_key = base64.b64decode(minio_secret.data['AWS_ACCESS_KEY_ID']).decode('utf-8')
            minio_secret_key = base64.b64decode(minio_secret.data['AWS_SECRET_ACCESS_KEY']).decode('utf-8')
            print("Successfully retrieved MinIO credentials from secret.")
        except Exception as e:
            print(f"Error retrieving MinIO credentials from secret '{minio_secret_name}': {e}")
            # If the secret is not found, KServe's storage initializer will also fail unless
            # it's configured differently. So we raise to make it explicit.
            raise

        # Define the KServe InferenceService manifest
        inferenceservice_manifest = {
            "apiVersion": f"{kserve_group}/{kserve_version}",
            "kind": "InferenceService",
            "metadata": {
                "name": model_name,
                "namespace": namespace,
                "annotations": {
                    "serving.kserve.io/s3-secret-name": minio_secret_name,
                    "serving.kserve.io/s3-usehttps": "0", # Assuming MinIO is not using HTTPS
                    "s3.endpoint": minio_endpoint,        # Use the full MinIO endpoint (e.g., http://host:port)
                    "s3.accessKeyID": minio_access_key,   # MinIO Access Key
                    "s3.secretAccessKey": minio_secret_key, # MinIO Secret Key
                    "s3.region": "us-east-1"              # Dummy region required by botocore/KServe
                }
            },
            "spec": {
                "predictor": {
                    "sklearn": { # Replace with your model framework (e.g., tensorflow, pytorch, triton)
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
                        "env": [ # Environment variables for the *model server* container
                            # These are passed to the KServe predictor container, which is where
                            # model loading would happen. This is often redundant if annotations work.
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
                                "value": minio_endpoint # This should also be the full URL
                            },
                            {
                                "name": "AWS_REGION",
                                "value": "us-east-1" # Match the annotation
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
                # Update the existing resource
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

        # Wait for the InferenceService to become ready
        print(f"Waiting for InferenceService {model_name} to be ready...")
        timeout_seconds = 600 # Wait up to 10 minutes
        poll_interval_seconds = 15
        start_time = time.time()
        service_url = None

        while True:
            if (time.time() - start_time) > timeout_seconds:
                raise TimeoutError(f"InferenceService {model_name} did not become ready within {timeout_seconds} seconds.")

            try:
                status_response = custom_api.get_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    namespace=namespace,
                    plural="inferenceservices",
                    name=model_name
                )

                is_ready = False
                current_conditions = 'N/A'

                if 'status' in status_response:
                    status = status_response['status']
                    if 'conditions' in status:
                        conditions = status['conditions']
                        current_conditions = conditions

                        for condition in conditions:
                            if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                                is_ready = True
                                break

                    if 'address' in status and 'url' in status['address']:
                        service_url = status['address']['url']

                if is_ready:
                    print(f"InferenceService {model_name} is ready.")
                    if service_url:
                        break # Exit loop if ready and URL found
                    else:
                        print("Service is ready but URL not available yet, continuing to wait...")
                else:
                    print(f"Still waiting for {model_name}. Current status: {current_conditions}")

                time.sleep(poll_interval_seconds)

            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(f"InferenceService {model_name} not found yet, retrying...")
                    time.sleep(poll_interval_seconds)
                else:
                    print(f"Kubernetes API error while waiting for InferenceService: {e}")
                    raise
            except Exception as e:
                print(f"An unexpected error occurred while waiting for InferenceService: {e}")
                raise

        if not service_url:
            # Fallback to retrieve URL one last time or construct default
            try:
                final_status = custom_api.get_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    namespace=namespace,
                    plural="inferenceservices",
                    name=model_name
                )
                if 'status' in final_status and 'address' in final_status['status'] and 'url' in final_status['status']['address']:
                    service_url = final_status['status']['address']['url']
                else:
                    # Construct default service URL if not found in status
                    service_url = f"http://{model_name}-predictor-default.{namespace}.svc.cluster.local/v1/models/{model_name}:predict"
                    print(f"Using constructed service URL: {service_url}")
            except Exception as e:
                print(f"Warning: Could not retrieve service URL, using default: {e}")
                service_url = f"http://{model_name}-predictor-default.{namespace}.svc.cluster.local/v1/models/{model_name}:predict"

        print(f"Model serving URL: {service_url}")
        return service_url # The URL is returned as the component's output

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