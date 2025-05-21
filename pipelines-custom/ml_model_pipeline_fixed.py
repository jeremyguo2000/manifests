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

OAUTH2_PROXY_COOKIE = "YebtA1tGUBTmxcR0J4ErEcQ5WRveSy2aVbZGWI6dekTla-aeJmQR237-vsm-5z-qP2wvwpcdIY8SuTXfZowQ0vFiixnvlQYLJXybP8CwM4QJ1hX-O42JEq1iJE0PVNkX7nniYhGcGDZnRUZL9FbpdoAatI7muK2gJiNjcKigMP8YGQm84GHh845WBR-2xhPfdaZ54U0ZPY_iuKxTLuVo5YkmVOidKhhcxGwLWfCHo3qLfymaWYliwSIyZEdKddqMUcgEeADmD_Jv9alll71QyMdeiJCXNvOAZ2sN1Ypc1UJ4frrQAjzpt9QPDr9bYcKgdLdlIDaljN4Wo9bb-zRuP1FVDWwNTIbx-DtG-_mkuCxgC_ttK2KJjWOSb1MQmktnOgZ51I0IPtFMvfFB1x4NvOIxdsBTmkeBjEVc8qlycC3w73uTI9do02gnKEpMX_zYnVHp06NMLwK05d3pevku_RpAGUe0U1GTzW0DvPcJDWrCTgz0LjkmYwZoBqZf1RC9TTIFDWit1SpUxf8XqhjNn8bayhiW9Rqurd0moQJVXMCSSIX72V9O_LJGuJx5QWVg0kChYnI74P7tT4tU8jplebp_ATREo8hLPtm1RyVObRBHrVC_XRsoXi-7wZyP4DyJreZT238QuhciYzt-mPQLS8g2oUeqQ02lw1lf1UYnz6WGkj06QEl54c8cEAG1c1Z0aff_GeGfI0PHeyV-GwTpfFTXQgpmwH7X6Lk9hnGyCyb3dxOR1BmFkVNKJsk7R-BTqn6YxBW_stwu-SvxA76Ci9noMdu9yqLTT58FL7HkPgUSmtr8kNhPwTkZViQcDryDBD5_6TK3W1Ur7UpUQn2RIEyvc-j4Oz-4kQbpikMkg0oOeZ7GpQWkpBwnWuTUY114DTOpWFm8clgEFNmwNQcLLI4B3ckwj8CLRdQ3ANnZ5yevY0nc17yidzvjfA2D588D6Jn4xHqysDVyJTGKaewBZ4V_n2RX6bG5sBVJaQd2mJJGKg_o1ntke4GEVtkmbt0Zc7PTASqbpibEsiYCTmXfPKyMebQkbDTffYDr4P8PWK5JCvNGSEXLxkElk9x4DAyoS8cquCadwsgvrtYRpd7CHcehFkOtyIme6fj4PiSpa0nObjZ8wQN3lb3FtOSDZcUVtwVPdiaaMqQFewFIJT1b0pCnsjNjogJMCpZQ5ighRLZrmJGHWfEpNIMUWz2caWnUCJNW76yDMDqfJxEubMybNhknK0PhwOv02OaGwn6lrVhOQrJxvE2wLCLlz2qP996Oz7S5fKCgVPYqgMd37c9Q-oVec0LYHDkKvZo3FN7YFx_INZfgAsoqiaaoai8JBUXD19JrzQ9M0K9ZMJVv9zxfB_BecLW_9yf2HOo1Bqj_CA-a6s6TLgdNEe86-YHWD4fenckQgGD6dOb9eA2BAt8a2UouKhsdagTP7CkFkBz5fm7O_ozG11mN_utGa5e0lShRPkgkg5Gnb-GbUPqndlkwamrn90LeGYcWuo4a7rhUe2ze2vZq1gKqBVgwCK8ZSb6cOHkrfBi_OVDzUr5w-1tbdMbmhHXBJ8Sn1LHsoFveXkP-XP6t2NGdUDeXSwBGnWHoyEOr42lIXZsNhbGB10s9UsTUJ3cy-Q-KRSmvo_1eTVXgzUYXjk3jQ-pDR8aXgBu5AEvYht8-cJlDNylw1S_urDzmcDHXQK7S4dMjN-_YhZMT-tfr8DAQIZYEHQzaDA90ev81d2XsvYRQA8yS7MN9M4Kgrwo80WEw7LvLzs9QNY0ghG85a9QrY4SEtHlPSuTlE9m3twTSOMpPaVf0uZuKeWSI5eqyDrkrqe4yaeT8YeXKZAw9a5ldk-dE4Fg4daPdN2VpdGRnd2Rj0rESqibhMjHrI4NzeN1KEGtOHoOTOEf5VmRbJk1cWLIwfoyleeHN_QEwMBOX5q5GCOHL2epapXGUMGTd92v5mx3ZO0QNHJMw3vT_RuRCzxLO2Pj_1WLEAmWWADdRa22XLsFTi5ZyYXW8HVQbPN1k1MHniRDbgoy2jWNgiIZFjxpVnoflZwLJTznUJo6cS5z-9rLaCB2GebPRqP7LjdeiVRkw6PGnOLGPTCogDpBQYm6P26duVej69aRYQlgu99z7VE-9T0aqTAG8wL9gT3gqX2-2-vIFcYrVNkm0_Vt8uq9-cj2BwUGfNysoiE_GFd8HDX5jpXWoeNmgFZVS0L5vMewMoOX5Xxt_aFh_8fYiCtI27vwx6zKlKQ4S9IMvXUwASyX8TtpopEirVmT2RwWq|1747793267|ZamwNuYkOJeLu_vrzrj0_-jWc8vl8yyvzzVg9wpriVo="

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
    packages_to_install=["kubernetes"]
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
                raise

        print(f"InferenceService '{model_name}' deployment initiated. Waiting for it to become ready...")

        max_retries = 30
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