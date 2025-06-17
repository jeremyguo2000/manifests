from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client
import time
import os

# --- Configuration ---
KUBEFLOW_PIPELINES_HOST = "http://10.254.50.50:8080/pipeline"
KUBEFLOW_NAMESPACE = "kubeflow-user"

# --- IMPORTANT: REPLACE THIS PLACEHOLDER WITH YOUR ACTUAL COOKIE VALUE ---
# You can find this in your browser's developer tools under Application > Cookies
# when logged into Kubeflow. Look for 'oauth2_proxy_kubeflow'.
OAUTH2_PROXY_COOKIE = "q5vRYGQQ1paoxMi-t0wEjivQRVqdh1YP287bKGGsRoydj-5a0m2GI31cWZ8D1tL1j8i4M7ZPhwDWo-hvkvkjvsKwzPzI0o_U-a05LqxiwC6wj-UTwHa0P-a3bzfxTUQJUWq88qsgQ9vWt_logTrgPgM-da5RwYuFWW3BhqKDHeAhsaxF4AFNswbj_7APNAoFWhdqbVk6Xzfn-LhHb_5XveYgkVlSFUwhKDI4wrFrSvKsVeutjhS9Utj4QCCUDSpkhIGMa4-YFn7t8I6pJy2Aeqp45IbAcwc1pnwfi2j3a7r59dTXTbdcX3H0F_4iH-sI2_MkWqAachaPLOSeSrZ-vSLA4URjaGG63inqF9a1rNtT8ik_s_wllyM_xREOx4tsVKbtv7Z1xht_0epwj0EZObjJorhpfCQULe-yN5y67hyZ7-kys7tk68-4hIZbwD3nEXJk9wA-b-hukE2rFljSXHFBCX26JR169FGBqWCB6rvO_sNpLeTEDxGZ_cFbq3XJUTdCopj9nlc8jd2K14rHelZN6FahVNVf6Cy9VfpUhcJttc7LkfpLlHpXGdApNKlHXRqbVK7I85eUUBE7dBKfYQ83WxYI2M0cpiDRibTn0ytMlAhlaatP1mC2HfNyysOnIuG0gcrWEfXNmoHVZzyF2tV1tfoYnvC00LDR0MbBmO7dguXS9NE2aekU_IWzfKXhFdB70G8dlFswt7QWlfVvUMi8tCL1sWw_V0HOTxzZ7fCg8sYMLXoEpqcT4s129KrOC9-1M4kffE-E1Iad4E4363PxqmJ3viZ0AV4JdG1lMJy6XZJh7k1aEMJjUR2FMphGCQDvHHuo4ZZTDh6ftbH8OvtzGINeU22LGK8is2ggpz4H2W9akdahhVqSPxybu8kBzme4EUGM6DF9q0she8QU3cuJfqefsmoP3Y23fon-JSGUvEGoGzhDrmFZYjNWbSjGDl71-bIRbiCqZ0lqeMDebNhQlyj3aFdi-FY1DEDNNCKi8M2zvJiz11YsDjAmqJNNE5xM6qOD64y0i6AHQKnQhc8KijNveSQ-sqPmpHkD99d5xp1Ca9OtLRiJtXzQVn8sQn0ptqCRQTC4_UllJnUfDuCo2TdFQq2VdzNbgcqH8IwLaZqOOWdOSg0IYYuG0JtvXHK8ATDWaNckJ85tlG0x_6vyOZ5ZdsAZHCHzDj3Dokl035edh7kvUOzd3mywO54ig0qHkv10nNQFUJh_fwPM7PqIBsJD2EztHOjYhKnJ2YLWyuErbopMXRrYv41RdL3CARZtriHFe-n6uCQQiNXY2DDBDDfLBur85LSCYPK82fF4dG2TvcQDGVlOIJHR-H6TQWjxO7b0K91dJJhXLee1IlYn9I0AHoYBE3zMypJmvbuaibFwAirdO48FtoYh0GbJKt3ar5fATFUkcBxc0dCKL_jpj3U9xpCz_Cwfwu_L24suFyi0F6Y2JqNS4upZc12ADu-CCOtN0j32RKcNHESunI__4rx2bTDW88ENNTLkJ42-JjUJ39vSEqEY91hBcHYrIgUnxYoBKX42XWGQl3RfG6jOyz8u60BdW5fs1Q7S8Gql_R66Vy3Ae2AXwDQdWLSU-Ur5-og54NdKoOSnEl4M30e4rc4GS6tH4FvgUUhxhRxKyrCdZf_oRDT14YtWltnleEsdLY1_pn4eumPrBzTxhHBp7n-nwR7IfUO5R5OTdKVdN6B0QMO4zys6oeMMmkh1qAdCqTBI-0P34X93-iDGMAJyL2HHjUADiL6U9QRe0xJaaA8cF4X3EoLwjvpMjQtJaMnfSe3Bxke6XZth3elFu8dWqwSlfL8UywSqiKYitOWJtkrVBnskAGGP7zFV6j-rTQQB35dfc-EFfhGSi151f5UfCiurYwcrQX1VgedpaVhKgWRyuJVqyqawTETpTahz_HTo9UkoXiX5JrmAJ6ZHBkYgPbpf73qtf2wpzwpCsrEd7yMfbbqchWgg0EBSQaDDgPzoefBDiTj6DYUqn5DcPe3U-URc4bE4caMqebpldh3HGPMUwVerhC98_yKmDDn6hpYkgW8XueP1GljsnDnaqxJiIksqfZkiSUIMmlakFcsFFzm3cMYccDsBwXKb14IZrJK-31rYNSI0phuq-hDYh-WV50bBS60ZHlbUjk3iXeeJK41g4EJMpo9REHFn_cC8J38hQX3C3MmK--9stroEVIARluwS7-LjgkLVkpjX7r1AkuzZ8wdeEeAdPfzpQkglsH1QvBws0Mk=|1750131241|PXfmDgUa8oKrN3gc-IioayQPXIWnwUwrgMpJBqWkItg=" # Now a clear placeholder

# Using a generic pipeline root as it's required by dsl.pipeline
# For this simplified example, artifacts will use the default KFP MinIO storage
# which is usually configured automatically.
PIPELINE_ROOT = "minio://mlpipeline/artifacts"

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
    packages_to_install=["scikit-learn", "numpy", "pandas", "joblib"]
)
def train_model(
    data: dsl.InputPath(dsl.Dataset),
    model: dsl.OutputPath(dsl.Model) # Output the model directly
):
    """
    Trains a simple Scikit-learn Linear Regression model and saves it.
    """
    import joblib
    from sklearn.linear_model import LinearRegression
    import os

    dataset_content = joblib.load(data)
    X = dataset_content['X']
    y = dataset_content['y']

    model_obj = LinearRegression()
    model_obj.fit(X, y)
    print("Model training complete.")

    joblib.dump(model_obj, model)
    print(f"Model saved as artifact to {model}")


@dsl.component(
    base_image="python:3.9-slim",
    packages_to_install=["scikit-learn", "numpy", "joblib"]
)
def test_model(
    model: dsl.InputPath(dsl.Model),
    prediction_result_path: dsl.OutputPath(str)
):
    """
    Tests the trained model by making a sample prediction using the model artifact.
    """
    import joblib
    import numpy as np
    import json

    print(f"Loading model from: {model}")
    model_obj = joblib.load(model)

    # Sample input data for a single-feature regression model
    sample_input = np.array([[5.0]])
    predictions = model_obj.predict(sample_input).tolist() # Convert to list for JSON serialization

    print(f"Prediction successful!")
    print(f"Input: {sample_input.tolist()}")
    print(f"Predictions: {predictions}")

    with open(prediction_result_path, "w") as f:
        f.write(json.dumps({
            "input": sample_input.tolist(),
            "predictions": predictions,
            "status": "success"
        }))
    print(f"Prediction results saved to {prediction_result_path}")


# --- Kubeflow Pipeline Definition ---

@dsl.pipeline(
    name="Simplified ML Pipeline",
    description="A simple pipeline to prepare data, train, and test an ML model.",
    pipeline_root=PIPELINE_ROOT
)
def simplified_ml_pipeline():
    """
    Defines the simplified end-to-end ML model lifecycle pipeline.
    """
    # Step 1: Prepare data
    prepare_data_task = prepare_data()

    # Step 2: Train model
    train_model_task = train_model(
        data=prepare_data_task.outputs['data']
    )

    # Step 3: Test prediction
    test_model_task = test_model(
        model=train_model_task.outputs['model']
    )
    # No explicit .after() needed here as test_model implicitly depends on train_model_task.outputs['model']

# --- Main execution block ---
if __name__ == "__main__":
    # Define the placeholder string to check against
    PLACEHOLDER_COOKIE = "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE"

    # Display partial cookie value (or full if short) for user confirmation
    display_cookie = OAUTH2_PROXY_COOKIE
    if len(OAUTH2_PROXY_COOKIE) > 40 and OAUTH2_PROXY_COOKIE != PLACEHOLDER_COOKIE:
        display_cookie = OAUTH2_PROXY_COOKIE[:20] + "..." + OAUTH2_PROXY_COOKIE[-20:]
    print(f"OAUTH2_PROXY_COOKIE set to: {display_cookie}")

    # Simplified check for placeholder cookie
    if OAUTH2_PROXY_COOKIE == PLACEHOLDER_COOKIE:
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value. ")
        print("!!!          You can find this in your browser's developer tools   ")
        print("!!!          under Application > Cookies when logged into Kubeflow.\n")
        exit(1)

    # Compile pipeline
    pipeline_filename = "simplified_ml_pipeline.yaml"
    Compiler().compile(simplified_ml_pipeline, package_path=pipeline_filename)
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
    experiment_name = "Simplified ML Demo"
    try:
        experiment = client.get_experiment(experiment_name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Using existing experiment: '{experiment_name}' (ID: {experiment.experiment_id})")
    except:
        experiment = client.create_experiment(name=experiment_name, namespace=KUBEFLOW_NAMESPACE)
        print(f"Created new experiment: '{experiment_name}' (ID: {experiment.experiment_id})")

    # Submit pipeline run
    run = client.create_run_from_pipeline_package(
        pipeline_file=pipeline_filename,
        arguments={}, # No arguments needed for this simplified pipeline
        experiment_id=experiment.experiment_id,
        namespace=KUBEFLOW_NAMESPACE
    )
    print(f"Pipeline run submitted: {run.run_id}")
    print(f"View run details in Kubeflow Dashboard: {KUBEFLOW_PIPELINES_HOST}/#/runs/details/{run.run_id}")