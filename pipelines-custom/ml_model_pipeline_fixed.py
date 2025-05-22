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

if hasattr(kfp, 'dsl'):
    if hasattr(kfp.dsl, 'OutputPath'):
        output_path_obj = kfp.dsl.OutputPath
        print(f"kfp.dsl.OutputPath object: {output_path_obj}")
        try:
            dummy_type = output_path_obj(str)
            print(f"kfp.dsl.OutputPath is callable with (str) for generic paths (as expected for v2).")
        except TypeError as e:
            print(f"kfp.dsl.OutputPath is NOT callable with (str) (TypeError: {e}). This means it's likely KFP v1.x behavior or an incompatible SDK version.")
        except Exception as e:
            print(f"Unexpected error when checking callability with (str): {e}")
    else:
        print(f"kfp.dsl.OutputPath not found within kfp.dsl.")
else:
    print(f"kfp.dsl module not found within kfp.")
print(f"--- End KFP Runtime Check ---")
# --- END DIAGNOSTIC CODE ---

# --- Configuration ---
MINIO_ENDPOINT = "minio-service.kubeflow:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "mlpipeline"
PIPELINE_ROOT = f"minio://{MINIO_BUCKET}/artifacts"
MODEL_SAVE_PATH_PREFIX = f"s3://{MINIO_BUCKET}/models"
KUBEFLOW_PIPELINES_HOST = "http://10.254.50.50:8080/pipeline"
KUBEFLOW_NAMESPACE = "kubeflow-user"

OAUTH2_PROXY_COOKIE = "erhzAdbWmXFu9ARvdgbML-QYz6Pgzv0r7QkmsRX90hCrjyoS-5oWNvPFSgRkni_mEoVO25NTv6Yj3dccao-lWL18AnLjp65StqdVJQV9GzAKZd6TMaaKZT6d0QlGx7XK8yLpOwyFXFbQGLD5ZZltCE5jEy4LJjAqryTn4z1h4gdH95rpn53wqS0VxKXKqaL7oNtF6JwAMqCb8Bzcd8Ueb6pOEkWdKA9mFzSIYJAwLJVWtcSE7i_AnOmZcvs4W-vlrMtlbql-ehYZ0QSP4u2TsA4rtr07s0RnbF1qns8Ww3KawdSOJlDwOsJdcWc1qonwtIuafkeRUmqiuX8_PebTAwia_qE2EgKM1ipq9grdAiYzyByCsrpmWqVmmHWyC7xZdmwJn0giFxUzfEKo2Ih8suTmfYI_cU69Wf7nK80oGH_CCP4uvX3sCi5Z-hb0_ifMuSkArQ0zMzBmHkPDQOsoTiUWcrXR4CzVqUvjV_iL-6H5blieiw00yCSSfLhgWKODN9oPecVqGZVffUIAIOw1NPp3Fs560Xmoxl81fKTxGGHZXY4uuRIO5FSuApJAQmcPj_77sg6mk7Kdb_Cf0pQF_TlgkWN0BHUU_zdcNyN0UqXZ6ZBRLLlsWA63m9McrEZDs-6WslKKmsz99AsxIB2r6oEy1UphXh1cSYwlHwtGs2zpgpK17vgwiuAcCpFkU9JgAh_u0GeZrEXHJdFQEDF3RfvhLPdjxfXyzHzOvywuSCT6zTMI7lY7bDX5XN84avOEulBACUEBGnmnVE1hwsMDWIewjjdOPkSEc26x5h4UhYuSyAfPLJNSj8flh71OdSoUucfnmzfoNvRXmTxQL-WpIbICIhVbWshopdxFqxl19IPQ82ssD4wMgDeMPN7hND5vx4ZWwDzdCBGmB18-cAxazBtZpgWQ3ydqX8h1L_9rySeBOQB4sFOfPOGD6RvewyOeDCRF8iJpqFFpiCLnn_7VDtYGHCBT-zmxHvDZf7SPThyUdIFZrMz-bcwY_HF1WlVhfcKxARK-B7THCZhIEuTVszQHzetb_DsjTrWyTeFh-WwCz2dQE1nuZ4jOuNokef8lezPDG_WxTwLKD13I3MJUzuRNDdeSe5EteTWHPm-j0_9u3xXX4qVz1z1xV83V1bx-tc__ZFD7mRyMnxc5wxMKbBVZd9ntc2v0wpM0McQWQi4Z9hELSxE-IoWqGY7WzfRxRVVhNxHLD6FnGTf5K2XbLgOC-QLJn4GC0YAs5kkCYO1oNXKM__MCsHMfoDnGdzVph5K6900iZXxCOTH80M1CYlePvzGFUTexHXR48T5JV05S9jgvL7qqEIozAun0xfIjCvZxBncGkXsyzo53tTtO-OCr2H7aCb5m1D12MWGCfrxhgbaLEOwbH4cWHFoycbT45x6PDpTMhLxWRUIuY0fG8xaEP81t55LAchsvYemAoXOb9_mgJdvbw9CC9CY5vxLc6hA8FoN1dc8eNcKLk5IlQRz7RlMp8lpHQSWpPQf8wMUqD6U9FBT4Wx6hZVhtDXhthq2xq2YEmccaPpnrLWyaHlEckOnDCrlV37U_AUAsX9euVX9q4cXisFfEeP1aMI6wCdVBQGt1zezt5kUypNVokgrFv_BCepIImX8uJcbjyJfLpCU1MBYMWGmof3qI-TiPFUXbMKICbZOYnB7mnsBaQzWgCFnH9JDVJM-lT7VnshOQZsfkdJE0rjOSySnSFR1qAu5GkqOj3FOjN32_MyUS0Ye9wD4KKrRKQSdf5jLed0QV4UlS7wFjPYmU5WPL_43l1d_h0JMBcTNNiDLwWnQ2jLsGLaLvNY_qWGnd4IfDXe3iZo9fGtIK-iQvPmxVOy-02AgIcNM6qnAA7dihySIxcQEG13illWNzlliE9zC1uB3n8HwA69xJg3DG9CK3S8NBi7H9JML2gOYA4cvm1-wVl0MgeUweMZfA8O5H_vDZzjmjaZlKTvQiwmmDMtrYhi8IhOns-Hboh6w74ee3AhnD5WErwygf9x4hLmTGZOzyjtMHRMgzMwBq0r1EECiD8wxGEgCVHSjacLz2l6N0dhyG65ULQQySd8pHvIEZeIl6r5GcZ-ukKlAWHs8AYBVG8gfpg3RGc4rcGd2F51PHGbmCtuzUbjZrpnxGtooWGfjea3djzNvicIBJCqugIQbI1Q1q90QXpmGIxoHA8MRKzhUZ0NWqrB1mwvUMxcnqKwpnIi9z-b8QBZhFCHTMf5CqIJ8uFeRcruXWLQ==|1747879832|e-hd1kjuKFxGTJDQCKb-E0irXUMuztuk6opctEgePmU="

# --- Kubeflow Pipeline Components ---

@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib"]
)
def prepare_data(data: dsl.OutputPath(dsl.Dataset)): # Changed to OutputPath(Dataset)
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
    joblib.dump(dataset_content, data) # 'data' is the path to the Dataset artifact
    print(f"Synthetic data prepared and saved to {data}")


@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib", "minio"]
)
def train_and_save_model(
    data: dsl.InputPath(dsl.Dataset), # Changed to InputPath(Dataset)
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
    dataset_content = joblib.load(data) # 'data' is the path to the Dataset artifact
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
    base_image="python:3.9-slim",
    packages_to_install=["kubernetes==30.1.0"] # Ensure kubernetes version < 31 is installed in this image
)
def deploy_model(
    model_storage_uri: str,
    model_name: str,
    namespace: str,
    inferenceservice_url_output: dsl.OutputPath(str)
):
    """
    Deploys the trained model using KServe (InferenceService).
    Waits for the deployment to be ready and prints the URL.
    """
    from kubernetes import client, config
    import time

    config.load_incluster_config()
    custom_api = client.CustomObjectsApi()

    # Define common KServe API group and version for clarity
    kserve_group = "serving.kserve.io"
    kserve_version = "v1beta1"
    kserve_plural = "inferenceservices"

    inferenceservice_manifest = {
        "apiVersion": f"{kserve_group}/{kserve_version}",
        "kind": "InferenceService",
        "metadata": {
            "name": model_name,
            "namespace": namespace,
        },
        "spec": {
            "predictor": {
                "sklearn": {
                    "storageUri": model_storage_uri,
                },
                "env": [ # <--- This is the correct location for KServe's automated injection
                    {
                        "name": "AWS_ACCESS_KEY_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "minio-credentials", # Name of your secret
                                "key": "AWS_ACCESS_KEY_ID" # Key within the secret
                            }
                        }
                    },
                    {
                        "name": "AWS_SECRET_ACCESS_KEY",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "minio-credentials",
                                "key": "AWS_SECRET_ACCESS_KEY"
                            }
                        }
                    },
                    {"name": "S3_ENDPOINT", "value": "http://minio-service.kubeflow.svc.cluster.local:9000"},
                    {"name": "S3_USE_HTTPS", "value": "false"}
                ]
            }
        }
    }

    print(f"Attempting to deploy KServe InferenceService '{model_name}' in namespace '{namespace}'...")
    try:
        try:
            # Check if InferenceService exists
            custom_api.get_namespaced_custom_object(
                group=kserve_group,
                version=kserve_version,
                name=model_name,
                namespace=namespace,
                plural=kserve_plural
            )
            print(f"InferenceService '{model_name}' already exists. Replacing...")
            custom_api.replace_namespaced_custom_object(
                group=kserve_group,
                version=kserve_version,
                name=model_name,
                namespace=namespace,
                plural=kserve_plural,
                body=inferenceservice_manifest
            )
        except client.ApiException as e:
            if e.status == 404:
                print(f"InferenceService '{model_name}' not found. Creating new one...")
                custom_api.create_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    namespace=namespace,
                    plural=kserve_plural,
                    body=inferenceservice_manifest
                )
            else:
                raise

        print(f"InferenceService '{model_name}' deployment initiated. Waiting for it to become ready...")

        max_retries = 30
        retry_delay_seconds = 10
        inferenceservice_url = ""

        for i in range(max_retries):
            time.sleep(retry_delay_seconds)
            try:
                isvc_status = custom_api.get_namespaced_custom_object(
                    group=kserve_group,
                    version=kserve_version,
                    name=model_name,
                    namespace=namespace,
                    plural=kserve_plural
                )

                conditions = isvc_status.get("status", {}).get("conditions", [])
                ready_condition = next((c for c in conditions if c.get("type") == "Ready"), None)

                if ready_condition and ready_condition.get("status") == "True":
                    inferenceservice_url = isvc_status.get("status", {}).get("address", {}).get("url")
                    if inferenceservice_url:
                        print(f"InferenceService '{model_name}' is Ready!")
                        print(f"Model Serving URL: {inferenceservice_url}")

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
        data=prepare_data_task.outputs['data'], # Changed from data_path to data
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

    test_prediction_task = test_prediction(
        model_serving_url=deploy_model_task.outputs['inferenceservice_url_output']
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