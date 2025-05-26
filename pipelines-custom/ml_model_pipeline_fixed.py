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

    # Save the data to the provided output path (which is now a Dataset artifact)
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

    # Load data from the provided InputPath (which is a Dataset artifact)
    dataset_content = joblib.load(data)
    X = dataset_content['X']
    y = dataset_content['y']

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
    packages_to_install=["kubernetes", "kserve"],
    base_image="python:3.9",
)
def deploy_model(
    model_name: str,
    model_storage_uri: str,
    namespace: str,
    kserve_group: str = "serving.kserve.io", # Add default values for kserve_group and kserve_version
    kserve_version: str = "v1beta1",         # if they are always the same.
) -> str: # Add return type hint for the URL
    # Import necessary libraries inside the component function
    from kubernetes import client as k8s
    import json
    import time
    from kserve import KServeClient

    # Initialize KServeClient
    kserve_client = KServeClient()

    # Define the InferenceService manifest
    inferenceservice_manifest = {
        "apiVersion": f"{kserve_group}/{kserve_version}",
        "kind": "InferenceService",
        "metadata": {
            "name": model_name,
            "namespace": namespace,
            "annotations": {
                "serving.kserve.io/s3-secret": "minio-credentials"
            }
        },
        "spec": {
            "predictor": {
                "sklearn": {
                    "storageUri": model_storage_uri,
                },
            }
        }
    }

    print(f"Creating InferenceService: {model_name} in namespace: {namespace}")
    try:
        kserve_client.create(inferenceservice_manifest, namespace=namespace)
        print(f"InferenceService {model_name} created/updated successfully.")
        print(f"Waiting for InferenceService {model_name} to be ready...")
        kserve_client.wait_is_ready(model_name, namespace=namespace)
        print(f"InferenceService {model_name} is ready.")

        # Fetch the status to get the URL
        status = kserve_client.get(model_name, namespace=namespace)
        service_url = status.status.address.url
        print(f"Model serving URL: {service_url}")
        return service_url # This will be the output of the component

    except k8s.ApiException as e:
        print(f"Error creating/updating InferenceService: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
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

    print(f"Testing model at URL: {model_serving_url}")

    sample_input = [[5.0]]

    headers = {'Content-Type': 'application/json'}
    payload = {
        "instances": sample_input
    }

    try:
        response = requests.post(model_serving_url, headers=headers, json=payload)
        response.raise_for_status()

        predictions = response.json().get('predictions')

        print(f"Prediction successful!")
        print(f"Input: {sample_input}")
        print(f"Predictions: {predictions}")

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
    model_name: str = "my-sklearn-regression-model",
    namespace: str = KUBEFLOW_NAMESPACE
):
    """
    Defines the end-to-end ML model lifecycle pipeline.
    """
    prepare_data_task = prepare_data()

    train_and_save_model_task = train_and_save_model(
        data=prepare_data_task.outputs['data'],
        model_name=model_name,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_bucket=MINIO_BUCKET,
        model_save_path_prefix=MODEL_SAVE_PATH_PREFIX
    )

    deploy_model_task = deploy_model(
        model_storage_uri=train_and_save_model_task.output,
        model_name=model_name,
        namespace=namespace
    )

    # Access the single output of deploy_model_task using .output
    test_prediction_task = test_prediction(
        model_serving_url=deploy_model_task.output
    )
    test_prediction_task.after(deploy_model_task)

# --- Main execution block ---
if __name__ == "__main__":
    if OAUTH2_PROXY_COOKIE == "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE":
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value.   ")
        print("!!!          Refer to the comments in the script for instructions.     \n")
        exit(1)

    pipeline_filename = "ml_model_lifecycle_pipeline.yaml"
    Compiler().compile(ml_model_lifecycle_pipeline, package_path=pipeline_filename)
    print(f"Kubeflow Pipeline compiled to '{pipeline_filename}'")

    print(f"Connecting to Kubeflow Pipelines at: {KUBEFLOW_PIPELINES_HOST}")
    try:
        client = Client(host=KUBEFLOW_PIPELINES_HOST, cookies=f"oauth2_proxy_kubeflow={OAUTH2_PROXY_COOKIE}")
    except Exception as e:
        print(f"Error connecting to KFP client. Check your host URL and cookie: {e}")
        exit(1)

    experiment_name = "Model Lifecycle Demo"
    try:
        experiment = client.get_experiment(experiment_name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Using existing experiment: '{experiment_name}' (ID: {experiment.experiment_id})")
    except:
        experiment = client.create_experiment(name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Created new experiment: '{experiment_name}' (ID: {experiment.experiment_id})")

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