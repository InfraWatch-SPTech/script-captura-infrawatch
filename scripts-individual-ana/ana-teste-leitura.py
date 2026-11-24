import boto3
import csv
import io
import json
import urllib.parse
from dotenv import load_dotenv
import os
import pandas as pd
from collections import defaultdict
from datetime import datetime
import pymysql as mysql
from pathlib import Path
import math

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

# Variáveis AWS
AWS_ACESS_KEY_ID = os.getenv("AWS_ACESS_KEY_ID") or None # Em caso de variável vazia, retorna None
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)


DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = int(os.getenv("PORT"))


bd = mysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=PORT,
    cursorclass=mysql.cursors.DictCursor
)

# ------------------------------------------------------------
# -- EXTRACT : BUSCANDO ARQUIVOS .CSV ------------------------
# ------------------------------------------------------------

def buscar_arquivo_mais_recente(bucket, prefixo='bronze/hardware/'):
    resposta = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo)
    if 'Contents' not in resposta:
        raise FileNotFoundError(
            f'Nenhum arquivo encontrado no bucket {bucket} com o prefixo {prefixo}'
        )

    # Filtra apenas arquivos que terminam com .csv
    arquivos_csv = [
        obj for obj in resposta['Contents']
        if obj['Key'].endswith('.csv')
    ]
    if not arquivos_csv:
        raise FileNotFoundError(
            f'Nenhum arquivo .csv encontrado em s3://{bucket}/{prefixo}'
        )

    # Ordena pelo campo LastModified para pegar o mais recente
    arquivo_mais_recente = sorted(
        arquivos_csv, key=lambda x: x['LastModified'], reverse=True
    )[0]
    return arquivo_mais_recente['Key']


def buscar_arquivos_s3(bucket, key_arquivo, parametro, prefixo='bronze/hardware/'):
    arquivos = []

    paginator = s3.get_paginator("list_objects_v2")

    key_empresa = key_arquivo.split("/")[2]

    data_arquivo = key_arquivo.split("_")[1]

    ano = data_arquivo.split("-")[0]
    mes = data_arquivo.split("-")[1]

    prefixo_empresa = f"{prefixo}{key_empresa}/"

    if parametro == "MENSAL":
        inicio_nome = f"captura_{ano}-{mes}"
    elif parametro == "ANUAL":
        inicio_nome = f"captura_{ano}"
    else:
        raise ValueError(f"Parâmetro inválido: {parametro}. Insira 'MENSAL' ou 'ANUAL'!")

    for pagina in paginator.paginate(Bucket = bucket, Prefix = prefixo_empresa):
        for item in pagina.get("Contents", []):
            key = item["Key"]
            nome = key.split("/")[-1]

            if not key.endswith(".csv") or not nome.startswith(inicio_nome):
                continue

            obj = s3.get_object(Bucket = bucket, Key = key)

            linhas = obj["Body"].read().decode("utf-8").splitlines(keepends=True)

            if "LATENCIA_MAX_MS" not in linhas[0] and "LATENCIA_P99_MS;" in linhas[0]:

                linhas[0] = linhas[0].replace(
                    "LATENCIA_P99_MS;", "LATENCIA_P99_MS;LATENCIA_MAX_MS;"
                )

            df_arquivo = pd.read_csv(io.StringIO("".join(linhas)), sep=";")
            df_arquivo["arquivo_origem"] = key
            arquivos.append(df_arquivo)

    if not arquivos:
        return pd.DataFrame()

    dados = pd.concat(arquivos, ignore_index=True)

    return dados, key_empresa


# Busca a path do arquivo mais recente
key_arquivo = buscar_arquivo_mais_recente("data-lake-infrawatch")

# Busca todos os arquivos da empresa do arquivo mais recente
# Filtra pelo mês do arquivo mais recente ("MENSAL")
dados, key_empresa = buscar_arquivos_s3("data-lake-infrawatch", key_arquivo, "MENSAL")

# Converte o csv em JSON
dados = dados.to_json(orient="records", indent=4, force_ascii=False)

# Junta todas as capturas como uma lista
capturas = json.loads(dados)

# Cria a variável que vai guardar todas as capturas agrupadas por servidor
capturas_por_servidor = defaultdict(list)

# Verifica ids iguais e agrupa em um servidor
for c in capturas:
    capturas_por_servidor[c["id"]].append(c)

# Organiza as capturas dos servidores por data
for lista in capturas_por_servidor.values():
    lista.sort(key=lambda c: c["data_hora"])

# Organiza os índices dos servidores
ids = sorted(capturas_por_servidor)
servers = [capturas_por_servidor[i] for i in ids]


# ------------------------------------------------------------
# -- SILVER : ENVIANDO ARQUIVOS PARA O BUCKET SILVER DO S3 ---
# ------------------------------------------------------------

# Envia JSON pro silver

timestamp = datetime.now()
timestamp = timestamp.strftime("%Y-%m-%d_%H-%M-%S")

try:
    print("Enviando para bucket silver do S3...")
    for s in servers:
        id_atual = s[0]["id"]
        s3.put_object(
            Bucket="data-lake-infrawatch",
            Key=f"silver/hardwares/{key_empresa}/{id_atual}/dash-gestao-problemas_{timestamp}.json",
            Body=json.dumps(s, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json"
        )
except:
    print("Um erro ocorreu ao tentar enviar arquivos para o bucket silver!")
else:
    print("Arquivos enviados com sucesso!")


# ------------------------------------------------------------
# -- TRANSFORM : FORMATANDO ARQUIVOS PARA O GOLD -------------
# ------------------------------------------------------------


# Os servidores já vêm filtrados pela empresa do arquivo bronze mais recente,
# por isso não é necessário passar empresa_id aqui.
cursor = bd.cursor()

cursor.execute(f"SELECT equipa.idEquipamento AS id, equipa.nome AS nome, equipa.valorDowntime AS valorDowntime, equipe.nome AS equipe FROM empresa AS e JOIN equipamento AS equipa ON equipa.fkEmpresa = e.idEmpresa JOIN usuario_equipamento AS ue ON ue.fkEquipamento = equipa.idEquipamento JOIN equipe ON equipe.idEquipe = ue.fkEquipe WHERE e.nome = '{key_empresa}' GROUP BY equipa.idEquipamento, equipe.nome ORDER BY equipa.idEquipamento;")

equipes_servidor = cursor.fetchall()

def juntarEquipes(equipes_servidor):
    servidores = {}
    # Adiciona os servidores individualmente na lista
    for s in equipes_servidor:
        id_eq = s['id']

        if id_eq not in servidores:
            servidores[id_eq] = {
                'id': id_eq,
                'nome': s['nome'],
                'valorDowntime': s['valorDowntime'],
                'equipes': []
            }

        if s['equipe'] not in servidores[id_eq]['equipes']:
            servidores[id_eq]['equipes'].append(s['equipe'])

    return list(servidores.values())

servidores = juntarEquipes(equipes_servidor)

# ------------------------------------------------------------
# -- GOLD : ENVIANDO ARQUIVOS PARA O BUCKET GOLD DO S3 -------
# ------------------------------------------------------------

# Envia JSON pro gold

timestamp = datetime.now()
timestamp = timestamp.strftime("%Y-%m-%d_%H-%M-%S")

try:
    print("Enviando para bucket gold do S3...")
    for s in servers:
        id_atual = s[0]["id"]
        s3.put_object(
            Bucket="data-lake-infrawatch",
            Key=f"gold/hardwares/{key_empresa}/{id_atual}/dash-gestao-problemas_{timestamp}.json",
            Body=json.dumps(s, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json"
        )
except:
    print("Um erro ocorreu ao tentar enviar arquivos para o bucket gold!")
else:
    print("Arquivos enviados com sucesso!")
    print("Fechando script...")