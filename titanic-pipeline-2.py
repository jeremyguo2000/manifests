#!/usr/bin/env python3
import os
import time
import botocore
import kfp
from kfp.dsl import (
pipeline,
component,
InputPath,
OutputPath
)
from kubernetes import client as k8s_client
import yaml

#---------------------------
# Component 1: Data Preparation
#Output type is set to "Directory" so that the component can write multiple CSV files.
@component(packages_to_install=["pandas", "scikit-learn"])
def preprocess_data(output_data: OutputPath("Directory")):
    import pandas as pd
    from sklearn.model_selection import train_test_split

    # Load the Titanic dataset
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    df = pd.read_csv(url)

    # Simple preprocessing: select a few columns and drop rows with missing values
    df = df[['Pclass', 'Age', 'SibSp', 'Parch', 'Survived']].dropna()

    # Split into train and test sets
    train, test = train_test_split(df, test_size=0.2, random_state=42)

    # Create the output directory (output_data is a directory provided by KFP)
    os.makedirs(output_data, exist_ok=True)
    train.to_csv(os.path.join(output_data, "train.csv"), index=False)
    test.to_csv(os.path.join(output_data, "test.csv"), index=False)
    print(f"Data processing complete. Files saved to {output_data}")
#---------------------------
#Component 2: Model Training
#Here both the input (the directory with train.csv) and the model_output are directories.
@component(packages_to_install=["pandas", "scikit-learn", "joblib"])
def train_model(input_data: InputPath("Directory"), model_output: OutputPath("Directory")):
    import joblib
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier


    # Load training data from the provided directory
    train_df = pd.read_csv(os.path.join(input_data, "train.csv"))
    X_train = train_df.drop(columns=["Survived"])
    y_train = train_df["Survived"]

    # Train a Random Forest classifier
    print("Training RandomForest model...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    print("Model training complete")

    # Create the output directory and save the model
    os.makedirs(model_output, exist_ok=True)
    model_path = os.path.join(model_output, "model.joblib")
    joblib.dump(model, model_path)
    print(f"Model saved to {model_path}")

#---------------------------
#Component 3: Model Evaluation
@component(packages_to_install=["pandas", "scikit-learn", "joblib"])
def evaluate_model(input_data: InputPath("Directory"), model_path: InputPath("Directory")):
    import joblib
    import pandas as pd
    from sklearn.metrics import accuracy_score, classification_report


    # Load test data from the preprocessed directory
    test_df = pd.read_csv(os.path.join(input_data, "test.csv"))
    X_test = test_df.drop(columns=["Survived"])
    y_test = test_df["Survived"]

    # Load the model from the given path (model.joblib is inside the directory)
    model = joblib.load(os.path.join(model_path, "model.joblib"))

    # Evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    print(f"Model Accuracy: {accuracy:.4f}")
    print("Classification Report:")
    print(report)
#---------------------------
#Component 4: Upload to MinIO
@component(packages_to_install=["boto3", "botocore"])
def upload_to_minio(model_path: InputPath("Directory"),
    minio_url: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str):
    import boto3
    from botocore.client import Config
    import os


    try:
        # Configure the S3 (MinIO) client
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_url,  # adjust based on your environment
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(signature_version="s3v4"),
        )
        
        # Build the local file path to the model file
        model_file = os.path.join(model_path, "model.joblib")
        s3_dest = "titanic-model/model.joblib"
        print(f"Uploading {model_file} to MinIO bucket {minio_bucket} at {s3_dest}")
        s3.upload_file(model_file, minio_bucket, s3_dest)
        print("Upload successful.")
    except Exception as e:
        print(f"Error during upload to MinIO: {e}")
        raise

    
#---------------------------
# Component 5: Deploy to KServe
@component(packages_to_install=["kubernetes", "pyyaml"])
def deploy_model(namespace: str):
    from kubernetes import client, config
    import yaml


    #`print(f"Deploying model to KServe in namespace {namespace}")

    # Try to load in-cluster configuration first. If that fails, use the local kubeconfig.
    try:
        config.load_incluster_config()
        print("Using in-cluster Kubernetes configuration.")
    except Exception:
        config.load_kube_config()
        print("Using local kubeconfig.")

    # KServe InferenceService definition (adjust if needed)
    kserve_yaml = f"""
    apiVersion: "serving.kserve.io/v1beta1"
    kind: "InferenceService"
    metadata:
    name: "titanic-model"
    namespace: "{namespace}"
    spec:
    predictor:
    sklearn:
    storageUri: "s3://models/titanic-model/"
    s3Configuration:
    endpoint: "minio-service.minio.svc.cluster.local:9000"
    region: "us-east-1"
    accessKeyIDSecret:
    name: "minio-credentials"
    key: "AWS_ACCESS_KEY_ID"
    secretAccessKeySecret:
    name: "minio-credentials"
    key: "AWS_SECRET_ACCESS_KEY"
    useHttps: false
    """


# Load the YAML and create a Kubernetes custom resource
    kserve_dict = yaml.safe_load(kserve_yaml)
    k8s_custom_api = k8s_client.CustomObjectsApi()

    try:
        # If the InferenceService exists, update it
        try:
            k8s_custom_api.get_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                name="titanic-model"
            )
            print("InferenceService already exists, updating it...")
            k8s_custom_api.patch_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                name="titanic-model",
                body=kserve_dict
            )
        except Exception:
            print("Creating new InferenceService...")
            k8s_custom_api.create_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                body=kserve_dict
            )
        print("Model deployment submitted successfully!")
        print("Waiting for deployment to be ready...")
        time.sleep(10)  # wait for the service to initialize
        
        print("\nTo access your model once it's ready:")
        print("1. Run: kubectl get inferenceservice titanic-model -n", namespace)
        print("2. For local access, try: kubectl port-forward svc/titanic-model-predictor-default -n", namespace, "8081:8080")
        print("3. And then: curl -v http://localhost:8081/v1/models/titanic-model:predict -d '{\"instances\": [[3, 25, 0, 0]]}'")
    except Exception as e:
        print(f"Error deploying model: {e}")
        raise
# ---------------------------
#Define the Pipeline
@pipeline(name="titanic-pipeline", description="End-to-end pipeline for Titanic Model")
def titanic_pipeline(
    namespace: str = "kubeflow-user",
    minio_url: str = "http://minio-service.minio.svc.cluster.local:9000",
    minio_bucket: str = "models",
    minio_access_key: str = "minio",
    minio_secret_key: str = "minio123"
    ):
    # Step 1: Data Preprocessing
    data_op = preprocess_data()


    # Step 2: Train the Model; pass the preprocessed data (a directory) to training.
    model_op = train_model(input_data=data_op.outputs["output_data"])

    # Step 3: Evaluate the model using test.csv from the data folder and the saved model.
    evaluate_model(
        input_data=data_op.outputs["output_data"],
        model_path=model_op.outputs["model_output"]
    )

    # Step 4: Upload the trained model to MinIO.
    upload_op = upload_to_minio(
        model_path=model_op.outputs["model_output"],
        minio_url=minio_url,
        minio_bucket=minio_bucket,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key
    )

    # Step 5: Deploy the model to KServe (this runs after the upload completes)
    deploy_model(namespace=namespace).after(upload_op)
#---------------------------
#Compile and (optionally) run the pipeline
if __name__ == "main":
    # Compile the pipeline to a YAML file
    pipeline_filename = "titanic_pipeline.yaml"
    kfp.compiler.Compiler().compile(titanic_pipeline, pipeline_filename)
    print(f"Pipeline compiled: {pipeline_filename}")


    # If a Kubeflow Pipelines client is available, try submitting the pipeline.
    try:
        client = kfp.Client()
        run = client.create_run_from_pipeline_func(
            titanic_pipeline,
            arguments={
                'namespace': 'kubeflow-user',
                'minio_url': 'http://minio-service.minio.svc.cluster.local:9000',
                'minio_bucket': 'models',
                'minio_access_key': 'minio',
                'minio_secret_key': 'minio123'
            }
        )
        print(f"Pipeline submitted. Run ID: {run.run_id}")
    except Exception as e:
        print(f"Note: Pipeline compiled but not submitted: {e}")
        print("To run the pipeline, use the Kubeflow Pipelines UI or the KFP CLI:")
        print(f"kfp run submit -f {pipeline_filename} -e kubeflow-user")