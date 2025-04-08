from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client

# TODO: what is this
PIPELINE_ROOT = "minio://mlpipeline/artifacts"  # Adjust based on your Minikube setup

@dsl.component
def flip_three_sided_coin() -> str:
    import random
    return random.choice(['heads', 'tails', 'draw'])

@dsl.component
def print_comp(text: str):
    print(text)

@dsl.pipeline(
    name="Three-Sided Coin Pipeline",
    pipeline_root=PIPELINE_ROOT
)
def my_pipeline():
    coin_flip_task = flip_three_sided_coin()
    
    with dsl.If(coin_flip_task.output == 'heads'):
        print_task = print_comp(text='Got heads!')
    with dsl.Elif(coin_flip_task.output == 'tails'):
        print_task = print_comp(text='Got tails!')
    with dsl.Else():
        print_task = print_comp(text='Draw!')

# Compile the pipeline
if __name__ == "__main__":
    Compiler().compile(my_pipeline, package_path="three_sided_coin_pipeline.yaml")

    namespace = "kubeflow-user-example-com"

    # Connect to Kubeflow Pipelines and run
    # Make sure to do port forwarding  
    #  kubectl port-forward -n kubeflow svc/ml-pipeline 8888:8888
    # get the token with 
    # kubectl create token default -n kubeflow-user-example-com --audience=pipelines.kubeflow.org --duration=120h
    # kubectl create token default -n kubeflow-user-example-com --audience="pipelines.kubeflow.org"
    # https://claude.ai/chat/2a5dde58-def5-4eb0-b9c0-cf3dc3cfa932
    client = Client(host="http://localhost:8888", existing_token="eyJhbGciOiJSUzI1NiIsImtpZCI6Im1jVWZ3blNoSy1IWUdaVDJPdG1rZ3QwR0NhcXBWZE83NHRWQnZzYm5CUkUifQ.eyJhdWQiOlsicGlwZWxpbmVzLmt1YmVmbG93Lm9yZyJdLCJleHAiOjE3NDM0ODc4NzEsImlhdCI6MTc0MzA1NTg3MSwiaXNzIjoiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiLCJqdGkiOiJmNzBjYzVmMy0xY2RiLTRlZTAtOGQ3OS1lM2FhZDVhODM3MTkiLCJrdWJlcm5ldGVzLmlvIjp7Im5hbWVzcGFjZSI6Imt1YmVmbG93LXVzZXItZXhhbXBsZS1jb20iLCJzZXJ2aWNlYWNjb3VudCI6eyJuYW1lIjoiZGVmYXVsdCIsInVpZCI6ImZmOTY3OTQ4LTM3ODMtNDY4Zi05NDQwLTY0YWNjYjU5MDNiZiJ9fSwibmJmIjoxNzQzMDU1ODcxLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZWZsb3ctdXNlci1leGFtcGxlLWNvbTpkZWZhdWx0In0.F5Nkp4kSoAuNt2l8lehETJAOUT43yu7XzEkMpxXjPlvgaI6w9m8B1Gsl89nRP_xofewro1hfTdPjKvlFLHd0KJlVGIH4nR1vAYNPKvbGLqtM3H3YYZkYvcvxPcqGWotDVMe6DPAelc-7aEbz13DETaikJaL89Z4BVIzmS8cyly3tK6vRSf3RZRwKp2ivBZZodAbaXwD3zRbO9p243-ji7T3aTs3MOeOJcyqu4iX2vNzh-29K3LLtYa6KI4HLyV7NLTdW3orDYJ6QZFQz79oKdjBrTaLudnucLCnDGXr7AwOJ4iO5_fCCCYXPhFfENU6Zy673XHPfLnzPkw0rYAuw9w")
    run = client.create_run_from_pipeline_package(
        pipeline_file="three_sided_coin_pipeline.yaml",
        arguments={},
        experiment_name="Three-Sided Coin Experiment",
        namespace=namespace
    )
    print(f"Pipeline submitted: {run.run_id}")
