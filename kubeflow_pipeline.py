import botocore
import kfp
from kfp.dsl import pipeline, component, Output, Model, InputPath, OutputPath
import os

# --- Component 1: Data Preparation ---
@component(packages_to_install=["pandas", "scikit-learn"])
def preprocess_data(output_data: OutputPath()):
    import pandas as pd
    from sklearn.model_selection import train_test_split
    import os
    
    # Load Titanic dataset
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    df = pd.read_csv(url)
    
    # Simple preprocessing
    df = df[['Pclass', 'Age', 'SibSp', 'Parch', 'Survived']].dropna()
    
    # Split into train and test
    train, test = train_test_split(df, test_size=0.2, random_state=42)
    
    # Save processed data
    os.makedirs(output_data, exist_ok=True)
    train.to_csv(os.path.join(output_data, "train.csv"), index=False)
    test.to_csv(os.path.join(output_data, "test.csv"), index=False)
    print(f"Data processing complete. Files saved to {output_data}")

# --- Component 2: Model Training ---
@component(packages_to_install=["pandas", "scikit-learn", "joblib"])
def train_model(input_data: InputPath(), model_output: OutputPath()):
    import joblib
    import pandas as pd
    import os
    from sklearn.ensemble import RandomForestClassifier

    # Load data
    train_df = pd.read_csv(os.path.join(input_data, "train.csv"))
    X_train = train_df.drop(columns=["Survived"])
    y_train = train_df["Survived"]

    # Train model
    print("Training RandomForest model...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    print("Model training complete")

    # Save model
    os.makedirs(model_output, exist_ok=True)
    model_path = os.path.join(model_output, "model.joblib")
    joblib.dump(model, model_path)
    print(f"Model saved to {model_path}")

# --- Component 3: Model Evaluation ---
@component(packages_to_install=["pandas", "scikit-learn", "joblib"])
def evaluate_model(input_data: InputPath(), model_path: InputPath()):
    import joblib
    import pandas as pd
    import os
    from sklearn.metrics import accuracy_score, classification_report

    # Load test data
    test_df = pd.read_csv(os.path.join(input_data, "test.csv"))
    X_test = test_df.drop(columns=["Survived"])
    y_test = test_df["Survived"]

    # Load model
    model = joblib.load(os.path.join(model_path, "model.joblib"))
    
    # Evaluate model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    
    print(f"Model Accuracy: {accuracy:.4f}")
    print("Classification Report:")
    print(report)

@component(packages_to_install=["boto3", "botocore"])
def upload_to_minio(model_path: Model, minio_url: str, minio_bucket: str, minio_access_key: str, minio_secret_key: str):
    import boto3
    import botocore
    from botocore.client import Config
    import os

    try:
        # Configure MinIO client
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_url,  # Make sure the endpoint is correct
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(signature_version="s3v4"),
        )

        # Check if the bucket exists; if not, create it
        # try:
        #    s3.head_bucket(Bucket=minio_bucket)  # Check if the bucket exists
        # except botocore.exceptions.ClientError:
        #    s3.create_bucket(Bucket=minio_bucket)

        # Upload model to MinIO
        model_file = os.path.join(model_path.path, "model.joblib")
        s3.upload_file(model_file, minio_bucket, "titanic-model/model.joblib")

    except Exception as e:
        raise

# --- Component 5: Deploy to KServe ---
@component(packages_to_install=["kubernetes", "pyyaml"])
def deploy_model(namespace: str):
    from kubernetes import client, config
    import yaml
    import time
    
    print(f"Deploying model to KServe in namespace {namespace}")
    
    # Load in-cluster config
    try:
        config.load_incluster_config()
        print("Using in-cluster configuration")
    except:
        config.load_kube_config()
        print("Using local kubeconfig")
    
    # Create the KServe YAML for deployment
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

    # Parse YAML and create object using Kubernetes Python client
    kserve_dict = yaml.safe_load(kserve_yaml)
    k8s_client = client.CustomObjectsApi()
    
    # Apply the KServe resource
    try:
        # Check if the InferenceService already exists
        try:
            k8s_client.get_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                name="titanic-model"
            )
            print("InferenceService already exists, updating it...")
            
            k8s_client.patch_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                name="titanic-model",
                body=kserve_dict
            )
        except:
            print("Creating new InferenceService...")
            k8s_client.create_namespaced_custom_object(
                group="serving.kserve.io",
                version="v1beta1",
                namespace=namespace,
                plural="inferenceservices",
                body=kserve_dict
            )
        
        print("Model deployment submitted successfully!")
        print("Waiting for deployment to be ready...")
        # Give some time for the service to initialize
        time.sleep(10)
        
        # Print access information
        print("\nTo access your model once it's ready:")
        print("1. Run: kubectl get inferenceservice titanic-model -n", namespace)
        print("2. For local access: kubectl port-forward svc/titanic-model-predictor-default -n", namespace, "8081:8080")
        print("3. Then try: curl -v http://localhost:8081/v1/models/titanic-model:predict -d '{\"instances\": [[3, 25, 0, 0]]}'")
        
    except Exception as e:
        print(f"Error deploying model: {e}")
        raise

# --- Define the Pipeline ---
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
    
    # Step 2: Train Model
    model_op = train_model(input_data=data_op.outputs["output_data"])
    
    # Step 3: Evaluate Model
    evaluate_model(
        input_data=data_op.outputs["output_data"],
        model_path=model_op.outputs["model_output"]
    )
    
    # Step 4: Upload Model to MinIO
    upload_op = upload_to_minio(
        model_path=model_op.outputs["model_output"],
        minio_url=minio_url,
        minio_bucket=minio_bucket,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key
    )
    
    # Step 5: Deploy the Model to KServe
    deploy_model(namespace=namespace).after(upload_op)

# Compile and run the pipeline
if __name__ == "__main__":
    # Compile the pipeline
    pipeline_filename = "titanic_pipeline.yaml"
    kfp.compiler.Compiler().compile(titanic_pipeline, pipeline_filename)
    print(f"Pipeline compiled: {pipeline_filename}")
    
    # Run the pipeline (if KFP client available)
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
        print("To run the pipeline, use the Kubeflow Pipelines UI or the KFP CLI.")
        print("Example CLI command:")
        print(f"kfp run submit -f {pipeline_filename} -e kubeflow-user")