import boto3
import argparse
import json

# Cliente de Lambda
lambda_client = boto3.client("lambda", region_name="eu-west-1")

# Función para invocar una Lambda
def invoke_lambda(function_name, payload={}):
  
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    
    print("----- Respuesta de la Lambda -----")
    print(json.dumps(json.loads(response['Payload'].read().decode('utf-8')), ensure_ascii=False, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invocar Lambdas del proyecto")
    parser.add_argument("--fn", choices=["inicial", "diaria"], required=True, help="Lambda a ejecutar")
    args = parser.parse_args()

    if args.fn == "inicial":
        invoke_lambda("tmbd-lambda-inicial")  
    elif args.fn == "diaria":
        invoke_lambda("tmbd-lambda-diaria")  