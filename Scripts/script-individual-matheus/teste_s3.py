import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

PASTA_SCRIPT = Path(__file__).resolve().parent
load_dotenv(PASTA_SCRIPT / ".env", override=True)

chave = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID")
segredo = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
token = os.getenv("AWS_SESSION_TOKEN") or None
regiao = os.getenv("AWS_REGION") or "us-east-1"

bucket = (os.getenv("S3_BUCKET_BRONZE") or "").strip()
prefixo = (os.getenv("S3_PREFIX_BRONZE") or "bronze/hardware/").strip("/")
caminho = f"{prefixo}/teste_upload.txt" if prefixo else "teste_upload.txt"

if not bucket:
    raise SystemExit("Preencha S3_BUCKET_BRONZE no .env.")

sessao = boto3.Session(
    aws_access_key_id=chave,
    aws_secret_access_key=segredo,
    aws_session_token=token,
    region_name=regiao
)

print("Região:", regiao)
print("Bucket:", bucket)
print("Arquivo:", caminho)

print("\n1. Verificando a identidade AWS...")

try:
    identidade = sessao.client("sts").get_caller_identity()
    print("Conta:", identidade["Account"])
    print("ARN:", identidade["Arn"])
except ClientError as erro:
    print("Erro na identidade:", erro)
except Exception as erro:
    print("Erro na identidade:", type(erro).__name__, str(erro))

print("\n2. Tentando enviar ao S3...")

try:
    s3 = sessao.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=caminho,
        Body=b"Teste de upload InfraWatch",
        ContentType="text/plain"
    )
    print("SUCESSO: arquivo enviado ao S3.")
except ClientError as erro:
    print("Erro no upload:", erro)
except Exception as erro:
    print("Erro no upload:", type(erro).__name__, str(erro))