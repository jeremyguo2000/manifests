import kfp
from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client
import time

# --- Configuration ---
MINIO_ENDPOINT = "minio-service.kubeflow:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "mlpipeline"
PIPELINE_ROOT = f"minio://{MINIO_BUCKET}/artifacts"
MODEL_SAVE_PATH_PREFIX = f"s3://{MINIO_BUCKET}/models"
KUBEFLOW_PIPELINES_HOST = "http://10.254.50.50:8080/pipeline"
KUBEFLOW_NAMESPACE = "kubeflow-user"

# --- IMPORTANT: REPLACE THIS PLACEHOLDER WITH YOUR ACTUAL COOKIE VALUE ---
OAUTH2_PROXY_COOKIE = "L2x3oXCE81tKY8WVYgnUzCkBcZrPc39kfxbM03YFGdG_7WwoIyCcEi7UIE7rG8s2vYJoISnfOZ6Vx_ETk98TrVvNOoEd1a_Zmi_8IZljE5GFnNiGOt_vlBiqDXc3-mMCd4sUIzIL7B9L9XD4RiqmdXDIg2HEY5tgS1RmwV4H3DsoFRCBK7wv_asdhcVdB0CmfKGKOlvVTNhCE09DHt1XPS2PGHH9zzytjG2oyZ5_PGRjJdsswbhh7NAXK_0ZjK4f82iJu152CnVpnjkdm-NjkAtoWgl4IJZVAMwZ0-GI39DGeh3rnfNpor6Fi7jKpAbZ7LyBEq3jvbrEHgGwAhVb0xJ7ONymzbNaiQCxhEZOT8r_4GEXzluuSyxIZ22BWJ0V9vT34jt5LJimAFfJN_eL6iKH8WTrn8OL7wxsZzq1kf3Ng9NpRQ9G9MX71ViexaC-T6HIrXhyhbesKYLblW_dG5DeF9dYCapiYXAzziVei2jNlPsrRpYDMgzgpWhJKSm0zmvOy9_xGdJ1bhQKERwbAw3yuYo4R4aWmGbViYzxW89fzsO2gVTCQ9ro1x1YDaXKUop23wWD56IyXKNkpnqcBz7EfytaRpAwNKO8xSrjV-pQCjsqlx3-Kap_vHZZ_blA6mhHzoPvA3mY-V4as9eAXXa152XioBlm4yUKykaN5rUKVEfv94ClbzYBdv4o_Clbo1E5odV-9atapfza6lQ9DVFPfC47OLRpohDLn1FJrkbRzpfXT-10Yg6_qgKYX0uZjO5HTwJ9TIgG-fVB1abP20KGubureCay5FnkTj15jOLHGT0nZXWyfC4niPsJJOb2k51Q8PAxIS07x23gN_jvl9m4UsOpv2O-LQQx97tYplZdXazD94upEVhAnFxgJBQVnA9cQWNcqI7gEul4fUe5Hio0xlatUnM0lOCdnyert0oSR6ocDZ-qBlnqNaT7bKwwF1bkrqhKwZRboZRWZ-ByRQ4F6j1OG8Ah74SJhqXfRtvRr4WEeQFkM4wlVZyh0uJ6rW39qRIer--cIap-coVSgNEkHeyBkeTNlNTETlhNwkqf9LA0LOyNMRCrnJFpGM-W7wtTM_fdkuCC1ko3LGXgA5--2s-s9l0UunYkVuHqvg9nvMoWyvhCMIti0oKcJTYwdpskRUOZwHlbUu7Y6fOgaEDwAU3T-xOovaGEJ-dWKEVZ4MAEp-RRZcGhsBltsulB91r1pVW6PbgZuy-GpNh_qdq-tnLjfOpoiZFLlaLnqcLRFPP5XY__-EUvMeLvW4Y9GNszouNho6eCnVapZdnvWFzkzoX2KjkG3wYv-2VPf6QbiSVVFbwWmL0DVraMv-l9yF9lAtwn7PNQflZMOklQx61SCUfq77brMMVqwmNxTWK2QMSMnuxErYI9aSc2NX43MtENyi4kxugcNlPffNXU6xdrN-SMtucPmcf84P65i2gsr_fBguTomebM-7SOnUgEQeZRw6UUtYALdrgKNuuHYBmdCeCi07uhB-KuElok0RaIbZTGCIbgufyCa3qaxrO7apS_IT7cDTCMXzkS_4BgfNZPDDkNUVGZz5UTSVTZNwgTwP7O9GpE7pF6HJMGrTz2hzrIRF1bkKUai00u42S8uPMa6r5zwM1t295M3E22IqL2xVTi3EEln4wHMQpikuh9V3siCDGgKhT2YnL7KZPfa-hsRqCUGzPDozXDNt8pZpYdXpIOCssNh650ROJ7O1XtB7zrn4P6eKuVQ3eUagL2njZRXneo-Fz1oboFfPBeLl12dYKnBZIpVuFwPcsp7nOJfGcwXgzm5hLkkdn2HJCL8pSGYtkYj-qoHs6RJISUidgtNwwJiOijIv17bUfWQlb0OgmQM8sFk-iAbyk6M9PuNH4w4898BoymmWY0ggJjyPBDgeYGZ3N54Lv2k_N80pt2dWBbfFspmBDITHyMozC0jHZvacJBAexkcfOclXXGEcuWBax5fVQh7Up58l8uyVUa0zbS7qtvAKp8VHHgMe0JrLmeMAweN7rfj1A3qnZnzdWCDCksncRp3m9vQuASPT0M-AXGhPQecZuH0vkdnRDqp-K9HDUFXbVsu9iPtPd1vI3akM_AqSL-pBCpQKOKpefjUatw569hAyODtwc-0QK7C5Z2kPyi3JmhDTOrUibuORzuKePfTDekI-W8Z_3Ad-LrulAMY08hF-kXr2LXosAif4dz72ONHPm7s-gDNjFMHJTt20O1qS_6Aot7xO4Viej3qXUbqIdoFA54Y4saaD-9eVxDqY_NI1z4|1748227448|epQvNv1_TcHhNfKy5pf0w8dM12hTAMaB59OFySfTldI="

# --- Kubeflow Pipeline Components ---

@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib"]
)
def prepare_data(data: dsl.OutputPath(dsl.Dataset)):
    """
    Generates a simple synthetic dataset and saves it.
    """
    from sklearn.datasets import make_regression
    import pandas as pd
    import numpy as np
    import joblib
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
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_bucket: str,
    model_save_path_prefix: str,
) -> str:
    """
    Trains a simple Scikit-learn Linear Regression model and saves it to MinIO.
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
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )

        object_name = f"{model_name}/{local_model_filename}"

        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)
            print(f"Bucket '{minio_bucket}' created.")

        client.fput_object(minio_bucket, object_name, local_model_path)
        print(f"Model '{object_name}' uploaded successfully to MinIO bucket '{minio_bucket}'.")

        kserve_storage_uri = f"s3://{minio_bucket}/{model_name}"
        print(f"KServe storage URI for deployment: {kserve_storage_uri}")
        return kserve_storage_uri

    except S3Error as err:
        print(f"Error uploading to MinIO: {err}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during model saving: {e}")
        raise


@dsl.component(
    base_image="python:3.9",
    packages_to_install=[
        "kubernetes>=28.0.0,<31.0.0",
        "kserve>=0.11.0"
    ]
)
def deploy_model(
    model_name: str,
    model_storage_uri: str,
    namespace: str,
    kserve_group: str = "serving.kserve.io",
    kserve_version: str = "v1beta1",
    minio_secret_name: str = "minio-credentials" # Add this parameter for clarity
) -> str:
    """
    Deploy model using KServe with proper object creation and MinIO credentials.
    """
    from kubernetes import client, config
    import json
    import time
    import yaml

    try:
        # Try to load in-cluster config first, then local config
        try:
            config.load_incluster_config()
            print("Loaded in-cluster Kubernetes config")
        except:
            try:
                config.load_kube_config()
                print("Loaded local Kubernetes config")
            except Exception as e:
                print(f"Failed to load Kubernetes config: {e}")
                raise

        # Create InferenceService manifest
        inferenceservice_manifest = {
            "apiVersion": f"{kserve_group}/{kserve_version}",
            "kind": "InferenceService",
            "metadata": {
                "name": model_name,
                "namespace": namespace,
                "annotations": {
                    "serving.kserve.io/s3-secret-name": minio_secret_name, # Use the parameter
                    "serving.kserve.io/s3-usehttps": "0"
                }
            },
            "spec": {
                "predictor": {
                    "sklearn": {
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
                        }
                    },
                    # --- ADD THIS SECTION ---
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
                        }
                    ]
                    # --- END ADDITION ---
                }
            }
        }

        print(f"Creating InferenceService: {model_name} in namespace: {namespace}")
        print(f"Storage URI: {model_storage_uri}")

        # Use Kubernetes API directly to create the InferenceService
        api_client = client.ApiClient()
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
                print(f"InferenceService {model_name} already exists. Attempting to patch...")
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
                raise

        # Wait for InferenceService to be ready (rest of your wait logic is fine)
        print(f"Waiting for InferenceService {model_name} to be ready...")
        timeout_seconds = 600
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
                        break
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
                    service_url = f"http://{model_name}-predictor-default.{namespace}.svc.cluster.local/v1/models/{model_name}:predict"
                    print(f"Using constructed service URL: {service_url}")
            except Exception as e:
                print(f"Warning: Could not retrieve service URL, using default: {e}")
                service_url = f"http://{model_name}-predictor-default.{namespace}.svc.cluster.local/v1/models/{model_name}:predict"

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

    # Sample input data
    sample_input = [[5.0]]
    headers = {'Content-Type': 'application/json'}
    payload = {
        "instances": sample_input
    }

    # Retry mechanism for prediction testing
    max_retries = 5
    retry_delay = 30  # seconds

    for attempt in range(max_retries):
        try:
            print(f"Prediction attempt {attempt + 1}/{max_retries}")
            response = requests.post(model_serving_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()  # Raise an exception for HTTP errors

            predictions = response.json().get('predictions')

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

        except requests.exceptions.RequestException as e:
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
    pipeline_root=PIPELINE_ROOT
)
def ml_model_lifecycle_pipeline(
    model_name: str = "my-sklearn-regression-model",
    namespace: str = KUBEFLOW_NAMESPACE
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
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_bucket=MINIO_BUCKET,
        model_save_path_prefix=MODEL_SAVE_PATH_PREFIX
    )

    # Step 3: Deploy model
    deploy_model_task = deploy_model(
        model_storage_uri=train_and_save_model_task.output,
        model_name=model_name,
        namespace=namespace
    )

    # Step 4: Test prediction
    test_prediction_task = test_prediction(
        model_serving_url=deploy_model_task.output
    )
    test_prediction_task.after(deploy_model_task)


# --- Main execution block ---
if __name__ == "__main__":
    # Check if cookie is set
    if OAUTH2_PROXY_COOKIE == "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE":
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value.   ")
        print("!!!          You can find this in your browser's developer tools     ")
        print("!!!          under Application > Cookies when logged into Kubeflow.  \n")
        exit(1)

    # Compile pipeline
    pipeline_filename = "ml_model_lifecycle_pipeline.yaml"
    Compiler().compile(ml_model_lifecycle_pipeline, package_path=pipeline_filename)
    print(f"Kubeflow Pipeline compiled to '{pipeline_filename}'")

    # Connect to Kubeflow Pipelines
    print(f"Connecting to Kubeflow Pipelines at: {KUBEFLOW_PIPELINES_HOST}")
    try:
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
            'model_name': f'my-first-kserve-model-{int(time.time())}',
            'namespace': KUBEFLOW_NAMESPACE
        },
        experiment_id=experiment.experiment_id,
        namespace=KUBEFLOW_NAMESPACE
    )
    print(f"Pipeline run submitted: {run.run_id}")
    print(f"View run details in Kubeflow Dashboard: {KUBEFLOW_PIPELINES_HOST}/#/runs/details/{run.run_id}")