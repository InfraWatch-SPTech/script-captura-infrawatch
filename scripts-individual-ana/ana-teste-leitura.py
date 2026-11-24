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

# Envia JSON pro silver

timestamp = datetime.now()
timestamp = timestamp.strftime("%Y-%m-%d_%H-%M-%S")


# ------------------------------------------------------------
# -- SILVER : ENVIANDO ARQUIVOS PARA O BUCKET SILVER DO S3 ---
# ------------------------------------------------------------

# try:
#     print("Enviando para o S3...")
#     for s in servers:
#         id_atual = s[0]["id"]
#         s3.put_object(
#             Bucket="data-lake-infrawatch",
#             Key=f"silver/hardwares/{key_empresa}/{id_atual}/dash-gestao-problemas_{timestamp}.json",
#             Body=json.dumps(s, ensure_ascii=False).encode("utf-8"),
#             ContentType="application/json"
#         )
# except:
#     print("Um erro ocorreu ao tentar enviar arquivos!")
# else:
#     print("Arquivos enviados com sucesso!")


# ------------------------------------------------------------
# -- TRANSFORM : CONTAGEM DE ALERTAS POR SERVIDOR ------------
# ------------------------------------------------------------

# Nomes de coluna aceitos no CSV para cada métrica do parametro_alerta.
# Inclui as duas grafias usadas no projeto: USO_* (SQL) e USE_* (captura).
ALIAS_METRICAS = {
    "USO_CPU": {"CPU", "USO_CPU", "USE_CPU", "PROCESSADOR"},
    "USE_CPU": {"CPU", "USO_CPU", "USE_CPU", "PROCESSADOR"},
    "USO_RAM": {"RAM", "USO_RAM", "USE_RAM", "MEMORIA", "MEMORIA_RAM"},
    "USE_RAM": {"RAM", "USO_RAM", "USE_RAM", "MEMORIA", "MEMORIA_RAM"},
    "USO_DISCO": {"DISCO", "USO_DISCO", "USE_DISCO", "ARMAZENAMENTO"},
    "USE_DISCO": {"DISCO", "USO_DISCO", "USE_DISCO", "ARMAZENAMENTO"},
    "USO_REDE": {"REDE", "USO_REDE", "USE_REDE", "REDE_PERCENTUAL"},
    "USE_REDE": {"REDE", "USO_REDE", "USE_REDE", "REDE_PERCENTUAL"},
    "TEMPERATURA_CPU": {"TEMPERATURA", "TEMPERATURA_CPU", "CPU_TEMPERATURA"},
    "PROCESSOS_ATIVOS": {"PROCESSOS_ATIVOS", "PROCESSOS", "QTD_PROCESSOS"},
}


def valor_metrica(captura, nome_metrica):
    """Retorna o valor numérico da métrica em uma captura, ou None se ausente."""
    colunas = {str(chave).strip().lower(): chave for chave in captura}
    for alias in ALIAS_METRICAS.get(nome_metrica, {nome_metrica}):
        chave = colunas.get(alias.strip().lower())
        if chave is None:
            continue
        valor = captura.get(chave)
        if valor is None or valor == "":
            continue
        try:
            return float(valor)
        except (TypeError, ValueError):
            continue
    return None


def nivel_alerta(valor, limite_atencao, limite_critico):
    """Nível do alerta: CRITICO, ATENCAO ou None (normal/sem leitura)."""
    if valor is None:
        return None
    if valor >= float(limite_critico):
        return "CRITICO"
    if valor >= float(limite_atencao):
        return "ATENCAO"
    return None


def _parse_data_hora(valor):
    """Converte o campo data_hora do CSV em datetime (None se inválido)."""
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor))
    except (TypeError, ValueError):
        return None


def _montar_episodio(regra, nivel, inicio, fim, ativo):
    """Episódio de alerta com início, fim e duração em horas."""
    inicio_dt = _parse_data_hora(inicio)
    fim_dt = _parse_data_hora(fim)
    horas = None
    if inicio_dt is not None and fim_dt is not None:
        horas = round((fim_dt - inicio_dt).total_seconds() / 3600, 2)

    return {
        "componente": regra.get("componente"),
        "metrica": regra["nomeMetrica"],
        "nivel": nivel,
        "inicio": inicio,
        "fim": fim,
        "horas_ativo": horas,
        "ativo": ativo,
    }


def calcular_episodios_regra(historico, regra):
    """Episódios de uma regra: da 1ª leitura em alerta até a 1ª leitura normal.

    Se o alerta ainda estiver aberto na última leitura, 'fim' é essa última
    leitura e 'ativo' é True (a duração segue acumulando até o presente).
    """
    episodios = []
    inicio = None
    nivel_inicio = None

    for amostra in historico:
        nivel = nivel_alerta(
            valor_metrica(amostra, regra["nomeMetrica"]),
            regra["limite_atencao"],
            regra["limite_critico"],
        )
        if nivel is not None and inicio is None:
            inicio = amostra.get("data_hora")
            nivel_inicio = nivel
        elif nivel is None and inicio is not None:
            episodios.append(
                _montar_episodio(
                    regra, nivel_inicio, inicio, amostra.get("data_hora"), False
                )
            )
            inicio = None
            nivel_inicio = None

    if inicio is not None:
        episodios.append(
            _montar_episodio(
                regra, nivel_inicio, inicio, historico[-1].get("data_hora"), True
            )
        )

    return episodios


def calcular_alertas_por_servidor(servidores, cursor, empresa_id=None):
    """Conta os alertas ativos e os episódios de alerta por servidor."""

    # 1) Regras vigentes: um registro por (servidor, componente) monitorado.
    sql = """
        SELECT pa.fkEquipamento AS id_servidor,
               pa.fkComponente  AS id_componente,
               pa.nomeMetrica,
               pa.limite_atencao,
               pa.limite_critico,
               c.nome AS componente,
               eq.nome AS servidor
        FROM parametro_alerta pa
        JOIN componente c   ON c.idComponente = pa.fkComponente
        JOIN equipamento eq ON eq.idEquipamento = pa.fkEquipamento
        WHERE pa.ativo = 1
    """
    parametros = ()
    if empresa_id is not None:
        sql += " AND eq.fkEmpresa = %s"
        parametros = (empresa_id,)
    sql += " ORDER BY pa.fkEquipamento, pa.fkComponente"

    cursor.execute(sql, parametros)
    regras_por_servidor = defaultdict(list)
    for regra in cursor.fetchall():
        regras_por_servidor[int(regra["id_servidor"])].append(regra)

    # 2) Para cada servidor, avaliar cada regra ao longo do histórico.
    resultado = {}
    for amostras in servidores:
        if not amostras:
            continue

        id_servidor = int(amostras[0]["id"])
        regras = regras_por_servidor.get(id_servidor, [])
        historico = sorted(amostras, key=lambda amostra: amostra["data_hora"])
        ultima = historico[-1]

        alertas_ativos = []
        episodios_alerta = []
        sem_leitura = []

        for regra in regras:
            valor_atual = valor_metrica(ultima, regra["nomeMetrica"])
            nivel_atual = nivel_alerta(
                valor_atual, regra["limite_atencao"], regra["limite_critico"]
            )

            # Episódios com início, fim e duração (inclusive os já encerrados).
            episodios_regra = calcular_episodios_regra(historico, regra)
            episodios_alerta.extend(episodios_regra)

            if nivel_atual is not None:
                alerta = {
                    "componente": regra["componente"],
                    "metrica": regra["nomeMetrica"],
                    "nivel": nivel_atual,
                    "valor": valor_atual,
                    "limite_atencao": float(regra["limite_atencao"]),
                    "limite_critico": float(regra["limite_critico"]),
                }
                # O alerta ativo recebe a duração do episódio ainda aberto.
                episodio_atual = next(
                    (e for e in episodios_regra if e["ativo"]), None
                )
                if episodio_atual is not None:
                    alerta["horas_ativo"] = episodio_atual["horas_ativo"]
                    alerta["inicio"] = episodio_atual["inicio"]
                    alerta["fim"] = episodio_atual["fim"]
                alertas_ativos.append(alerta)
            elif valor_atual is None:
                sem_leitura.append(regra["nomeMetrica"])

        resultado[len(resultado)] = {
            "id": id_servidor,
            "nome": regras[0]["servidor"] if regras else id_servidor,
            "qtd_alertas_ativos": len(alertas_ativos),
            "qtd_alertas_totais": len(episodios_alerta),
            "alertas": alertas_ativos,
            "episodios_alerta": episodios_alerta,
            "sem_leitura": sem_leitura,
        }

    return resultado


# Os servidores já vêm filtrados pela empresa do arquivo bronze mais recente,
# por isso não é necessário passar empresa_id aqui.
cursor = bd.cursor()
alertas = calcular_alertas_por_servidor(servers, cursor)
alertas = alertas.values()

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

alertas_por_id = {}
for a in alertas:
    alertas_por_id[a['id']] = a

for s in servidores:
    a = alertas_por_id.get(int(s["id"]))
    if a is None:
        continue
    if s['id'] == a['id']:
        s['qtd_alertas_ativos'] = a['qtd_alertas_ativos']
        s['qtd_alertas_totais'] = a['qtd_alertas_totais']
        s['alertas'] = a['alertas']
        s['episodios_alerta'] = a['episodios_alerta']
        s['sem_leitura'] = a['sem_leitura']

for s in servidores:
    risco_downtime = 0
    criticos = 0
    atencao = 0
    for a in s['alertas']:
        if a['nivel'] == 'CRITICO':
            criticos += 1
        elif a['nivel' == 'ATENCAO']:
            atencao += 1

    # risco_downtime = s['qtd_alertas_ativos'] * 