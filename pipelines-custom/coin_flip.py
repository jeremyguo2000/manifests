from kfp import dsl
from kfp.compiler import Compiler
from kfp.client import Client
import subprocess

# TODO: what is this
PIPELINE_ROOT = "minio://mlpipeline/artifacts"  # Adjust based on your Minikube setup

@dsl.component
def flip_three_sided_coin() -> str:
    """
    A simple component that simulates flipping a three-sided coin,
    returning 'heads', 'tails', or 'draw'.
    """
    import random
    return random.choice(['heads', 'tails', 'draw'])

@dsl.component
def print_comp(text: str):
    """
    A simple component that prints the given text.
    """
    print(text)

@dsl.pipeline(
    name="Three-Sided Coin Pipeline",
    pipeline_root=PIPELINE_ROOT 
)
def my_pipeline():
    """
    Defines a Kubeflow Pipeline that flips a three-sided coin
    and prints a message based on the outcome.
    """
    coin_flip_task = flip_three_sided_coin()
    
    with dsl.If(coin_flip_task.output == 'heads'):
        print_task = print_comp(text='Got heads!')
    with dsl.Elif(coin_flip_task.output == 'tails'):
        print_task = print_comp(text='Got tails!')
    with dsl.Else():
        print_task = print_comp(text='Draw!')

if __name__ == "__main__":
    Compiler().compile(my_pipeline, package_path="three_sided_coin_pipeline.yaml")

    #namespace = "kubeflow-user-example-com"
    namespace = "kubeflow-user"

    # (Keep the token generation if you wish, but remember it's not used by the client below)
    try:
        command = [
            "kubectl", "create", "token", "default", 
            "-n", namespace, 
            "--audience=pipelines.kubeflow.org"
        ]
        process = subprocess.run(command, capture_output=True, text=True, check=True)
        kubeflow_token = process.stdout.strip()
        print(f"Successfully retrieved Kubernetes token (for direct K8s API access).")
        print(f"Note: This token is typically NOT used when connecting via an OIDC-enabled Ingress Gateway.")
    except Exception as e:
        print(f"Error getting Kubernetes token: {e}")
        exit(1)

    # --- IMPORTANT CHANGE HERE: Added '/pipeline' to the host URL ---
    # The Kubeflow Pipelines API is typically exposed under the '/pipeline' path
    # when accessed via the Istio Ingress Gateway.
    # The KFP client will then append its API paths (e.g., '/apis/v2beta1/experiments')
    # to this base URL, resulting in the correct full path like
    # 'http://10.254.50.50:8080/pipeline/apis/v2beta1/experiments'.
    
    oauth2_proxy_cookie = "YebtA1tGUBTmxcR0J4ErEcQ5WRveSy2aVbZGWI6dekTla-aeJmQR237-vsm-5z-qP2wvwpcdIY8SuTXfZowQ0vFiixnvlQYLJXybP8CwM4QJ1hX-O42JEq1iJE0PVNkX7nniYhGcGDZnRUZL9FbpdoAatI7muK2gJiNjcKigMP8YGQm84GHh845WBR-2xhPfdaZ54U0ZPY_iuKxTLuVo5YkmVOidKhhcxGwLWfCHo3qLfymaWYliwSIyZEdKddqMUcgEeADmD_Jv9alll71QyMdeiJCXNvOAZ2sN1Ypc1UJ4frrQAjzpt9QPDr9bYcKgdLdlIDaljN4Wo9bb-zRuP1FVDWwNTIbx-DtG-_mkuCxgC_ttK2KJjWOSb1MQmktnOgZ51I0IPtFMvfFB1x4NvOIxdsBTmkeBjEVc8qlycC3w73uTI9do02gnKEpMX_zYnVHp06NMLwK05d3pevku_RpAGUe0U1GTzW0DvPcJDWrCTgz0LjkmYwZoBqZf1RC9TTIFDWit1SpUxf8XqhjNn8bayhiW9Rqurd0moQJVXMCSSIX72V9O_LJGuJx5QWVg0kChYnI74P7tT4tU8jplebp_ATREo8hLPtm1RyVObRBHrVC_XRsoXi-7wZyP4DyJreZT238QuhciYzt-mPQLS8g2oUeqQ02lw1lf1UYnz6WGkj06QEl54c8cEAG1c1Z0aff_GeGfI0PHeyV-GwTpfFTXQgpmwH7X6Lk9hnGyCyb3dxOR1BmFkVNKJsk7R-BTqn6YxBW_stwu-SvxA76Ci9noMdu9yqLTT58FL7HkPgUSmtr8kNhPwTkZViQcDryDBD5_6TK3W1Ur7UpUQn2RIEyvc-j4Oz-4kQbpikMkg0oOeZ7GpQWkpBwnWuTUY114DTOpWFm8clgEFNmwNQcLLI4B3ckwj8CLRdQ3ANnZ5yevY0nc17yidzvjfA2D588D6Jn4xHqysDVyJTGKaewBZ4V_n2RX6bG5sBVJaQd2mJJGKg_o1ntke4GEVtkmbt0Zc7PTASqbpibEsiYCTmXfPKyMebQkbDTffYDr4P8PWK5JCvNGSEXLxkElk9x4DAyoS8cquCadwsgvrtYRpd7CHcehFkOtyIme6fj4PiSpa0nObjZ8wQN3lb3FtOSDZcUVtwVPdiaaMqQFewFIJT1b0pCnsjNjogJMCpZQ5ighRLZrmJGHWfEpNIMUWz2caWnUCJNW76yDMDqfJxEubMybNhknK0PhwOv02OaGwn6lrVhOQrJxvE2wLCLlz2qP996Oz7S5fKCgVPYqgMd37c9Q-oVec0LYHDkKvZo3FN7YFx_INZfgAsoqiaaoai8JBUXD19JrzQ9M0K9ZMJVv9zxfB_BecLW_9yf2HOo1Bqj_CA-a6s6TLgdNEe86-YHWD4fenckQgGD6dOb9eA2BAt8a2UouKhsdagTP7CkFkBz5fm7O_ozG11mN_utGa5e0lShRPkgkg5Gnb-GbUPqndlkwamrn90LeGYcWuo4a7rhUe2ze2vZq1gKqBVgwCK8ZSb6cOHkrfBi_OVDzUr5w-1tbdMbmhHXBJ8Sn1LHsoFveXkP-XP6t2NGdUDeXSwBGnWHoyEOr42lIXZsNhbGB10s9UsTUJ3cy-Q-KRSmvo_1eTVXgzUYXjk3jQ-pDR8aXgBu5AEvYht8-cJlDNylw1S_urDzmcDHXQK7S4dMjN-_YhZMT-tfr8DAQIZYEHQzaDA90ev81d2XsvYRQA8yS7MN9M4Kgrwo80WEw7LvLzs9QNY0ghG85a9QrY4SEtHlPSuTlE9m3twTSOMpPaVf0uZuKeWSI5eqyDrkrqe4yaeT8YeXKZAw9a5ldk-dE4Fg4daPdN2VpdGRnd2Rj0rESqibhMjHrI4NzeN1KEGtOHoOTOEf5VmRbJk1cWLIwfoyleeHN_QEwMBOX5q5GCOHL2epapXGUMGTd92v5mx3ZO0QNHJMw3vT_RuRCzxLO2Pj_1WLEAmWWADdRa22XLsFTi5ZyYXW8HVQbPN1k1MHniRDbgoy2jWNgiIZFjxpVnoflZwLJTznUJo6cS5z-9rLaCB2GebPRqP7LjdeiVRkw6PGnOLGPTCogDpBQYm6P26duVej69aRYQlgu99z7VE-9T0aqTAG8wL9gT3gqX2-2-vIFcYrVNkm0_Vt8uq9-cj2BwUGfNysoiE_GFd8HDX5jpXWoeNmgFZVS0L5vMewMoOX5Xxt_aFh_8fYiCtI27vwx6zKlKQ4S9IMvXUwASyX8TtpopEirVmT2RwWq|1747793267|ZamwNuYkOJeLu_vrzrj0_-jWc8vl8yyvzzVg9wpriVo=" # <--- REPLACE THIS PLACEHOLDER!

    if oauth2_proxy_cookie == "YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE":
        print("\n!!! IMPORTANT: Please replace 'YOUR_OAUTH2_PROXY_KUBEFLOW_COOKIE_VALUE_HERE' ")
        print("!!!          with your actual 'oauth2_proxy_kubeflow' cookie value.   ")
        print("!!!          Refer to the comments in the script for instructions.  \n")
        exit(1)

    client = Client(host="http://10.254.50.50:8080/pipeline", cookies=f"oauth2_proxy_kubeflow={oauth2_proxy_cookie}")
    # --- END IMPORTANT CHANGE ---

    run = client.create_run_from_pipeline_package(
        pipeline_file="three_sided_coin_pipeline.yaml",
        arguments={},
        experiment_name="Three-Sided Coin Experiment",
        namespace=namespace
    )
    print(f"Pipeline submitted: {run.run_id}")
