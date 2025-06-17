import kfp
from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client
import subprocess
import os
import time

# --- START DIAGNOSTIC CODE ---
print(f"--- KFP Runtime Check ---")
print(f"KFP module loaded from: {kfp.__file__}")
print(f"KFP version according to module: {kfp.__version__}")

# Check if dsl.OutputPath is callable or subscriptable
if hasattr(kfp, 'dsl'):
    if hasattr(kfp.dsl, 'OutputPath'):
        output_path_obj = kfp.dsl.OutputPath
        print(f"kfp.dsl.OutputPath object: {output_path_obj}")
        try:
            # Attempt to subscript to check if it causes an error
            # This is specifically what causes your TypeError
            dummy_type = output_path_obj[str]
            print(f"kfp.dsl.OutputPath is subscriptable (as expected for v2).")
        except TypeError as e:
            print(f"kfp.dsl.OutputPath is NOT subscriptable (TypeError: {e}). This means it's likely KFP v1.x behavior.")
        except Exception as e:
            print(f"Unexpected error when checking subscriptability: {e}")
    else:
        print(f"kfp.dsl.OutputPath not found within kfp.dsl.")
else:
    print(f"kfp.dsl module not found within kfp.")
print(f"--- End KFP Runtime Check ---")
# --- END DIAGNOSTIC CODE ---

# --- Configuration ---
# Your MinIO details
# Use the in-cluster service name for MinIO for components within the cluster
MINIO_ENDPOINT = "minio-service.kubeflow:9000" 
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "mlpipeline" # As per your MinIO URL: http://10.254.50.50:9000/minio/mlpipeline/

# Base path for pipeline artifacts and model storage within MinIO
# KFP uses 'minio://' prefix for its root
PIPELINE_ROOT = f"minio://{MINIO_BUCKET}/artifacts"
# KServe uses 's3://' prefix for its storageUri
MODEL_SAVE_PATH_PREFIX = f"s3://{MINIO_BUCKET}/models" 

# Kubeflow Pipelines client host (adjust if your ingress/port-forward is different)
KUBEFLOW_PIPELINES_HOST = "http://10.254.50.50:8080/pipeline"
KUBEFLOW_NAMESPACE = "kubeflow-user" # Choose your active user namespace

# IMPORTANT: Replace with your actual 'oauth2_proxy_kubeflow' cookie value!
# See instructions above. This cookie is short-lived.
OAUTH2_PROXY_COOKIE = "L2x3oXCE81tKY8WVYgnUzCkBcZrPc39kfxbM03YFGdG_7WwoIyCcEi7UIE7rG8s2vYJoISnfOZ6Vx_ETk98TrVvNOoEd1a_Zmi_8IZljE5GFnNiGOt_vlBiqDXc3-mMCd4sUIzIL7B9L9XD4RiqmdXDIg2HEY5tgS1RmwV4H3DsoFRCBK7wv_asdhcVdB0CmfKGKOlvVTNhCE09DHt1XPS2PGHH9zzytjG2oyZ5_PGRjJdsswbhh7NAXK_0ZjK4f82iJu152CnVpnjkdm-NjkAtoWgl4IJZVAMwZ0-GI39DGeh3rnfNpor6Fi7jKpAbZ7LyBEq3jvbrEHgGwAhVb0xJ7ONymzbNaiQCxhEZOT8r_4GEXzluuSyxIZ22BWJ0V9vT34jt5LJimAFfJN_eL6iKH8WTrn8OL7wxsZzq1kf3Ng9NpRQ9G9MX71ViexaC-T6HIrXhyhbesKYLblW_dG5DeF9dYCapiYXAzziVei2jNlPsrRpYDMgzgpWhJKSm0zmvOy9_xGdJ1bhQKERwbAw3yuYo4R4aWmGbViYzxW89fzsO2gVTCQ9ro1x1YDaXKUop23wWD56IyXKNkpnqcBz7EfytaRpAwNKO8xSrjV-pQCjsqlx3-Kap_vHZZ_blA6mhHzoPvA3mY-V4as9eAXXa152XioBlm4yUKykaN5rUKVEfv94ClbzYBdv4o_Clbo1E5odV-9atapfza6lQ9DVFPfC47OLRpohDLn1FJrkbRzpfXT-10Yg6_qgKYX0uZjO5HTwJ9TIgG-fVB1abP20KGubureCay5FnkTj15jOLHGT0nZXWyfC4niPsJJOb2k51Q8PAxIS07x23gN_jvl9m4UsOpv2O-LQQx97tYplZdXazD94upEVhAnFxgJBQVnA9cQWNcqI7gEul4fUe5Hio0xlatUnM0lOCdnyert0oSR6ocDZ-qBlnqNaT7bKwwF1bkrqhKwZRboZRWZ-ByRQ4F6j1OG8Ah74SJhqXfRtvRr4WEeQFkM4wlVZyh0uJ6rW39qRIer--cIap-coVSgNEkHeyBkeTNlNTETlhNwkqf9LA0LOyNMRCrnJFpGM-W7wtTM_fdkuCC1ko3LGXgA5--2s-s9l0UunYkVuHqvg9nvMoWyvhCMIti0oKcJTYwdpskRUOZwHlbUu7Y6fOgaEDwAU3T-xOovaGEJ-dWKEVZ4MAEp-RRZcGhsBltsulB91r1pVW6PbgZuy-GpNh_qdq-tnLjfOpoiZFLlaLnqcLRFPP5XY__-EUvMeLvW4Y9GNszouNho6eCnVapZdnvWFzkzoX2KjkG3wYv-2VPf6QbiSVVFbwWmL0DVraMv-l9yF9lAtwn7PNQflZMOklQx61SCUfq77brMMVqwmNxTWK2QMSMnuxErYI9aSc2NX43MtENyi4kxugcNlPffNXU6xdrN-SMtucPmcf84P65i2gsr_fBguTomebM-7SOnUgEQeZRw6UUtYALdrgKNuuHYBmdCeCi07uhB-KuElok0RaIbZTGCIbgufyCa3qaxrO7apS_IT7cDTCMXzkS_4BgfNZPDDkNUVGZz5UTSVTZNwgTwP7O9GpE7pF6HJMGrTz2hzrIRF1bkKUai00u42S8uPMa6r5zwM1t295M3E22IqL2xVTi3EEln4wHMQpikuh9V3siCDGgKhT2YnL7KZPfa-hsRqCUGzPDozXDNt8pZpYdXpIOCssNh650ROJ7O1XtB7zrn4P6eKuVQ3eUagL2njZRXneo-Fz1oboFfPBeLl12dYKnBZIpVuFwPcsp7nOJfGcwXgzm5hLkkdn2HJCL8pSGYtkYj-qoHs6RJISUidgtNwwJiOijIv17bUfWQlb0OgmQM8sFk-iAbyk6M9PuNH4w4898BoymmWY0ggJjyPBDgeYGZ3N54Lv2k_N80pt2dWBbfFspmBDITHyMozC0jHZvacJBAexkcfOclXXGEcuWBax5fVQh7Up58l8uyVUa0zbS7qtvAKp8VHHgMe0JrLmeMAweN7rfj1A3qnZnzdWCDCksncRp3m9vQuASPT0M-AXGhPQecZuH0vkdnRDqp-K9HDUFXbVsu9iPtPd1vI3akM_AqSL-pBCpQKOKpefjUatw569hAyODtwc-0QK7C5Z2kPyi3JmhDTOrUibuORzuKePfTDekI-W8Z_3Ad-LrulAMY08hF-kXr2LXosAif4dz72ONHPm7s-gDNjFMHJTt20O1qS_6Aot7xO4Viej3qXUbqIdoFA54Y4saaD-9eVxDqY_NI1z4|1748227448|epQvNv1_TcHhNfKy5pf0w8dM12hTAMaB59OFySfTldI=" 

# --- Kubeflow Pipeline Components ---

@dsl.component(
    base_image="python:3.9-slim", # Use a slim image for efficiency
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib"]
)
def prepare_data(data_path: dsl.OutputPath[str]):
    """
    Generates a simple synthetic dataset and saves it.
    """
    from sklearn.datasets import make_regression
    import pandas as pd
    import numpy as np
    import joblib
    import os

    X, y = make_regression(n_samples=100, n_features=1, noise=20, random_state=42)
    data = {'X': X, 'y': y}

    # Save the data to the provided output path
    joblib.dump(data, data_path)
    print(f"Synthetic data prepared and saved to {data_path}")


@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib", "minio"]
)
def train_and_save_model(
    data_path: dsl.InputPath[str],
    model_name: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_bucket: str,
    model_save_path_prefix: str,
) -> str: # Returns the full MinIO path where the model is saved (for KServe)
    """
    Trains a simple Scikit-learn Linear Regression model and saves it to MinIO.
    """
    import joblib
    from sklearn.linear_model import LinearRegression
    from minio import Minio
    from minio.error import S3Error
    import os

    # Load data
    data = joblib.load(data_path)
    X = data['X']
    y = data['y']

    # Train model
    model = LinearRegression()
    model.fit(X, y)
    print("Model training complete.")

    # Save model locally first (necessary before uploading)
    local_model_filename = "model.joblib"
    local_model_path = os.path.join("/tmp", local_model_filename)
    joblib.dump(model, local_model_path)
    print(f"Model saved locally to {local_model_path}")

    # Upload model to MinIO
    try:
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False # Set to True if using HTTPS
        )

        # Define the object path in MinIO, including the model_name as a sub-directory
        object_name = f"{model_name}/{local_model_filename}"
        
        # Ensure the bucket exists
        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)
            print(f"Bucket '{minio_bucket}' created.")
        
        # Upload the file
        client.fput_object(minio_bucket, object_name, local_model_path)
        print(f"Model '{object_name}' uploaded successfully to MinIO bucket '{minio_bucket}'.")
        
        # KServe expects s3://bucket/path/to/model/directory format
        # So we return the directory where the model file resides
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
    base_image="python:3.9-slim",
    packages_to_install=["kubernetes"] # kubernetes client to interact with K8s API
)
def deploy_model(
    model_storage_uri: str,
    model_name: str,
    namespace: str,
    inferenceservice_url_output: dsl.OutputPath[str] # Output path for the URL
):
    """
    Deploys the trained model using KServe (InferenceService).
    Waits for the deployment to be ready and prints the URL.
    """
    from kubernetes import client, config
    import time
    
    # Load Kubernetes configuration (assumes running inside a K8s cluster)
    config.load_incluster_config()
    custom_api = client.CustomObjectsApi()

    # Define the KServe InferenceService spec for Scikit-learn
    inferenceservice_manifest = {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "name": model_name,
            "namespace": namespace,
        },
        "spec": {
            "predictor": {
                "sklearn": {
                    "storageUri": model_storage_uri
                }
            }
        }
    }

    print(f"Attempting to deploy KServe InferenceService '{model_name}' in namespace '{namespace}'...")
    try:
        # Check if InferenceService already exists and replace it if it does
        # This allows re-running the pipeline without manual cleanup of old services
        try:
            custom_api.get_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                name=model_name,
                namespace=namespace
            )
            print(f"InferenceService '{model_name}' already exists. Replacing...")
            custom_api.replace_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                name=model_name,
                namespace=namespace,
                body=inferenceservice_manifest
            )
        except client.ApiException as e:
            if e.status == 404:
                print(f"InferenceService '{model_name}' not found. Creating new one...")
                custom_api.create_namespaced_custom_object(
                    group="serving.kserve.io",
                    version="v1beta1",
                    namespace=namespace,
                    body=inferenceservice_manifest
                )
            else:
                raise # Re-raise other Kubernetes API exceptions

        print(f"InferenceService '{model_name}' deployment initiated. Waiting for it to become ready...")

        # Poll the InferenceService status until it's ready
        max_retries = 30 # Check for up to 300 seconds (5 minutes)
        retry_delay_seconds = 10
        inferenceservice_url = ""

        for i in range(max_retries):
            time.sleep(retry_delay_seconds)
            try:
                isvc_status = custom_api.get_namespaced_custom_object(
                    group="serving.kserve.io",
                    version="v1beta1",
                    name=model_name,
                    namespace=namespace
                )
                
                conditions = isvc_status.get("status", {}).get("conditions", [])
                ready_condition = next((c for c in conditions if c.get("type") == "Ready"), None)
                
                if ready_condition and ready_condition.get("status") == "True":
                    inferenceservice_url = isvc_status.get("status", {}).get("address", {}).get("url")
                    if inferenceservice_url:
                        print(f"InferenceService '{model_name}' is Ready!")
                        print(f"Model Serving URL: {inferenceservice_url}")
                        
                        # Write the URL to the designated output path
                        with open(inferenceservice_url_output, "w") as f:
                            f.write(inferenceservice_url)
                        return
                    else:
                        print(f"InferenceService '{model_name}' is Ready, but URL is not yet available. Retrying...")
                else:
                    message = ready_condition.get("message", "No specific message.") if ready_condition else "Ready condition not found or not True."
                    print(f"InferenceService '{model_name}' not ready yet (Attempt {i+1}/{max_retries}). Status: {ready_condition.get('status', 'Unknown')}, Message: {message}")

            except client.ApiException as e:
                print(f"Kubernetes API error getting InferenceService status: {e.status} - {e.reason}. Retrying...")
            except Exception as e:
                print(f"Unexpected error during KServe status check: {e}. Retrying...")

        print(f"Error: InferenceService '{model_name}' did not become ready within the expected time.")
        raise RuntimeError("KServe InferenceService deployment failed.")

    except client.ApiException as e:
        print(f"Kubernetes API Error: {e.status} - {e.reason}")
        print(f"Error details: {e.body}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during KServe deployment: {e}")
        raise

@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["requests", "numpy"]
)
def test_prediction(
    model_serving_url: str,
    prediction_result_path: dsl.OutputPath[str] # Output path for the prediction result
):
    """
    Tests the deployed model by sending a sample prediction request.
    """
    import requests
    import numpy as np
    import json
    
    print(f"Testing model at URL: {model_serving_url}")
    
    # Example input for a simple linear regression model (single feature, needs to be list of lists)
    sample_input = [[5.0]] 

    headers = {'Content-Type': 'application/json'}
    payload = {
        "instances": sample_input
    }

    try:
        response = requests.post(model_serving_url, headers=headers, json=payload)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        predictions = response.json().get('predictions')
        
        print(f"Prediction successful!")
        print(f"Input: {sample_input}")
        print(f"Predictions: {predictions}")

        # Store the prediction result to an output artifact
        with open(prediction_result_path, "w") as f:
            f.write(json.dumps(predictions))
        
    except requests.exceptions.RequestException as e:
        print(f"Error during prediction request to {model_serving_url}: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during prediction testing: {e}")
        raise

# --- Kubeflow Pipeline Definition ---

@dsl.pipeline(
    name="ML Model Lifecycle Pipeline",
    description="A pipeline to prepare data, train, save, deploy, and test a simple ML model.",
    pipeline_root=PIPELINE_ROOT
)
def ml_model_lifecycle_pipeline(
    model_name: str = "my-sklearn-regression-model", # Default model name
    namespace: str = KUBEFLOW_NAMESPACE # Default namespace
):
    """
    Defines the end-to-end ML model lifecycle pipeline.
    """
    # 1. Prepare Data
    prepare_data_task = prepare_data()

    # 2. Train Model and Save to MinIO
    train_and_save_model_task = train_and_save_model(
        data_path=prepare_data_task.outputs['data_path'],
        model_name=model_name,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_bucket=MINIO_BUCKET,
        model_save_path_prefix=MODEL_SAVE_PATH_PREFIX
    )
    # The output of train_and_save_model_task is the KServe storage URI

    # 3. Deploy Model using KServe
    deploy_model_task = deploy_model(
        model_storage_uri=train_and_save_model_task.output,
        model_name=model_name,
        namespace=namespace
    )

    # 4. Test Prediction (requires the model to be deployed)
    # Use .after() to ensure the deployment task completes before testing
    test_prediction_task = test_prediction(
        model_serving_url=deploy_model_task.outputs['inferenceservice_url_output']
    )
    test_prediction_task.after(deploy_model_task) 

# --- Main execution block ---
if __name__ == "__main__":
    # Validate the cookie placeholder
    if OAUTH2_PROXY_COOKIE == "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE":
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value.   ")
        print("!!!          Refer to the comments in the script for instructions.    \n")
        exit(1)

    # Compile the pipeline to a YAML file
    pipeline_filename = "ml_model_lifecycle_pipeline.yaml"
    Compiler().compile(ml_model_lifecycle_pipeline, package_path=pipeline_filename)
    print(f"Kubeflow Pipeline compiled to '{pipeline_filename}'")

    # Connect to Kubeflow Pipelines client
    print(f"Connecting to Kubeflow Pipelines at: {KUBEFLOW_PIPELINES_HOST}")
    try:
        client = Client(host=KUBEFLOW_PIPELINES_HOST, cookies=f"oauth2_proxy_kubeflow={OAUTH2_PROXY_COOKIE}")
    except Exception as e:
        print(f"Error connecting to KFP client. Check your host URL and cookie: {e}")
        exit(1)

    # Create an experiment (or use an existing one)
    experiment_name = "Model Lifecycle Demo"
    try:
        experiment = client.get_experiment(experiment_name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Using existing experiment: '{experiment_name}' (ID: {experiment.experiment_id})")
    except:
        experiment = client.create_experiment(name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Created new experiment: '{experiment_name}' (ID: {experiment.experiment_id})")

    # Run the pipeline
    run = client.create_run_from_pipeline_package(
        pipeline_file=pipeline_filename,
        arguments={
            'model_name': f'my-first-kserve-model-{int(time.time())}', # Unique name for each run
            'namespace': KUBEFLOW_NAMESPACE
        },
        experiment_id=experiment.experiment_id,
        namespace=KUBEFLOW_NAMESPACE
    )
    print(f"Pipeline run submitted: {run.run_id}")
    print(f"View run details in Kubeflow Dashboard: {KUBEFLOW_PIPELINES_HOST}/#/runs/details/{run.run_id}")

    # You can optionally wait for the run to complete and print final status
    # print("Waiting for pipeline run to complete (this may take a few minutes)...")
    # try:
    #     client.wait_for_run_completion(run.run_id, timeout=900) # Wait up to 15 minutes
    #     print("Pipeline run finished successfully.")
    # except Exception as e:
    #     print(f"Pipeline run failed or timed out: {e}")