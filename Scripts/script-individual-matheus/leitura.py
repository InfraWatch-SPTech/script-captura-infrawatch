# ============================================================
# LEITURA / ETL INDIVIDUAL - MATHEUS (roda na AWS Lambda)
# ------------------------------------------------------------
# Bronze (JSON da captura)
#   -> limpa e valida as amostras         -> Silver (normalizado.json)
#   -> separa só o que a dashboard usa    -> Gold   (dashboard.json)
#
# Este script NÃO acessa o banco de dados.
# Limites, empresa e nomes dos servidores são buscados pelo
# BACKEND da dashboard no MySQL (como pede a arquitetura).
#
# Uso local (teste):  python3 leitura.py --local ./dados
# Uso no S3:          python3 leitura.py --s3
# Uso na Lambda:      handler = leitura.lambda_handler
# ============================================================
import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote_plus

from leitura_processos import (
    MAX_PROCESSOS_GOLD,
    formatar_campos_processos_gold,
    normalizar_campos_processos,
)

PASTA_SCRIPT = Path(__file__).resolve().parent

COMPONENTES = ["cpu", "ram", "disco", "rede"]

# Duração máxima aceita para um intervalo de captura (segundos)
INTERVALO_MAXIMO = 300


# ============================================================
# 0. CONFIGURAÇÃO
# ============================================================

def carregar_ambiente():
    arquivo_env = PASTA_SCRIPT / ".env"
    if arquivo_env.is_file():
        from dotenv import load_dotenv
        load_dotenv(arquivo_env, override=False)


def prefixo(nome_variavel, padrao):
    valor = (os.getenv(nome_variavel) or padrao).strip("/")
    if not valor or ".." in valor.split("/"):
        raise ValueError(f"{nome_variavel} inválido.")
    return valor + "/"


def buscar_configuracao():
    return {
        "bronze": prefixo("S3_PREFIX_BRONZE", "bronze/hardware/"),
        "silver": prefixo("S3_PREFIX_SILVER", "silver/"),
        "gold": prefixo("S3_PREFIX_GOLD", "gold/"),
        "bucket_bronze": os.getenv("S3_BUCKET_BRONZE"),
        "bucket_silver": os.getenv("S3_BUCKET_SILVER") or os.getenv("S3_BUCKET_BRONZE"),
        "bucket_gold": os.getenv("S3_BUCKET_GOLD") or os.getenv("S3_BUCKET_BRONZE"),
    }


def conectar_s3():
    import boto3
    parametros = {"region_name": os.getenv("AWS_REGION") or "us-east-1"}
    chave = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID")
    segredo = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    # Na Lambda, as credenciais vêm da role IAM (não precisa de chave)
    if chave and segredo:
        parametros["aws_access_key_id"] = chave
        parametros["aws_secret_access_key"] = segredo
        if os.getenv("AWS_SESSION_TOKEN"):
            parametros["aws_session_token"] = os.getenv("AWS_SESSION_TOKEN")
    return boto3.client("s3", **parametros)


# ============================================================
# 1. LEITURA DOS ARQUIVOS DA BRONZE (local ou S3)
# ============================================================

def listar_bronze(config, pasta_local=None, s3=None):
    arquivos = []
    if pasta_local is not None:
        raiz = Path(pasta_local)
        pasta_bronze = raiz / config["bronze"]
        if pasta_bronze.is_dir():
            for caminho in pasta_bronze.rglob("*.json"):
                arquivos.append(caminho.relative_to(raiz).as_posix())
    else:
        paginador = s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=config["bucket_bronze"], Prefix=config["bronze"]):
            for objeto in pagina.get("Contents", []):
                if objeto["Key"].endswith(".json"):
                    arquivos.append(objeto["Key"])
    return sorted(arquivos)


def ler_arquivo(chave, config, pasta_local=None, s3=None):
    if pasta_local is not None:
        conteudo = (Path(pasta_local) / chave).read_text(encoding="utf-8-sig")
    else:
        resposta = s3.get_object(Bucket=config["bucket_bronze"], Key=chave)
        conteudo = resposta["Body"].read().decode("utf-8-sig")
    dados = json.loads(conteudo)
    if isinstance(dados, dict):
        dados = [dados]
    return dados


def ids_do_caminho(chave, config):
    # Caminho esperado: bronze/hardware/ID_EMPRESA/ID_SERVIDOR/arquivo.json
    partes = chave[len(config["bronze"]):].split("/")
    if len(partes) != 3 or not partes[0].isdigit() or not partes[1].isdigit():
        return None, None
    return str(int(partes[0])), str(int(partes[1]))


# ============================================================
# 2. LIMPEZA DAS AMOSTRAS (Bronze -> Silver)
# ============================================================

def converter_data(valor):
    data = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    if data.tzinfo is None:
        raise ValueError("data sem fuso horário")
    return data.astimezone(timezone.utc)


def percentual(valor):
    # Retorna None quando não há valor; erro quando o valor é inválido
    if valor is None:
        return None
    if isinstance(valor, bool):
        raise ValueError("percentual inválido")
    numero = float(valor)
    if not math.isfinite(numero) or numero < 0 or numero > 100:
        raise ValueError("percentual fora de 0 a 100")
    return round(numero, 2)


def limpar_amostra(linha, empresa_id, servidor_id):
    if str(linha.get("empresa_id")) != empresa_id or str(linha.get("servidor_id")) != servidor_id:
        raise ValueError("IDs do JSON diferentes dos IDs do caminho")

    inicio = converter_data(linha["inicio"])
    fim = converter_data(linha["fim"])
    duracao = (fim - inicio).total_seconds()
    if duracao <= 0 or duracao > INTERVALO_MAXIMO:
        raise ValueError("intervalo de captura inválido")

    amostra = {
        "empresa_id": empresa_id,
        "servidor_id": servidor_id,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
    }
    for componente in COMPONENTES:
        # Capturas antigas (versão 1) não têm "rede": fica null
        amostra[componente] = percentual(linha.get(componente))

    # Detalhes da rede ficam só na Silver (a dashboard usa o %)
    for campo in ("rede_mbps", "rede_capacidade_mbps", "rede_capacidade_reserva"):
        if campo in linha:
            amostra[campo] = linha[campo]

    amostra.update(normalizar_campos_processos(linha))
    return amostra


def remover_sobrepostas(amostras):
    # Ordena por servidor e horário; descarta repetidas ou sobrepostas
    amostras.sort(key=lambda a: (a["servidor_id"], a["inicio"]))
    resultado = []
    descartadas = 0
    ultimo_fim = {}
    for amostra in amostras:
        servidor = amostra["servidor_id"]
        inicio = converter_data(amostra["inicio"])
        if servidor in ultimo_fim and inicio < ultimo_fim[servidor]:
            descartadas += 1
            continue
        ultimo_fim[servidor] = converter_data(amostra["fim"])
        resultado.append(amostra)
    if descartadas:
        print(f"Aviso: {descartadas} amostra(s) repetida(s)/sobreposta(s) descartada(s).")
    return resultado


# ============================================================
# 3. FORMATAÇÃO DA GOLD (somente o que a dashboard usa)
# ============================================================

def montar_gold(empresa_id, amostras_silver, gerado_em):
    amostras = []
    servidores = []
    for amostra in amostras_silver:
        if amostra["servidor_id"] not in servidores:
            servidores.append(amostra["servidor_id"])
        amostras.append({
            "servidor_id": amostra["servidor_id"],
            "inicio": amostra["inicio"],
            "fim": amostra["fim"],
            "cpu": amostra["cpu"],
            "ram": amostra["ram"],
            "disco": amostra["disco"],
            "rede": amostra["rede"],
            **formatar_campos_processos_gold(amostra),
        })
    return {
        "versao": 2,
        "empresa_id": empresa_id,
        "gerado_em": gerado_em,
        "processos_por_amostra": MAX_PROCESSOS_GOLD,
        "servidores": servidores,
        "amostras": amostras,
    }


def salvar_json(chave, dados, bucket, pasta_local=None, s3=None):
    conteudo = json.dumps(dados, ensure_ascii=False, allow_nan=False)
    if pasta_local is not None:
        destino = Path(pasta_local) / chave
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporario = destino.with_suffix(".tmp")
        temporario.write_text(conteudo, encoding="utf-8")
        temporario.replace(destino)
        print("Salvo localmente: " + str(destino))
    else:
        s3.put_object(Bucket=bucket, Key=chave, Body=conteudo.encode("utf-8"),
                      ContentType="application/json")
        print(f"Salvo no S3: s3://{bucket}/{chave}")


# ============================================================
# 4. EXECUÇÃO DO ETL
# ============================================================

def executar_etl(pasta_local=None):
    config = buscar_configuracao()
    s3 = None
    if pasta_local is None:
        if not config["bucket_bronze"]:
            raise ValueError("Configure S3_BUCKET_BRONZE.")
        s3 = conectar_s3()

    arquivos = listar_bronze(config, pasta_local, s3)
    if not arquivos:
        raise FileNotFoundError("Nenhum JSON encontrado na Bronze.")

    # Agrupa as amostras limpas por empresa
    por_empresa = {}
    ignorados = 0
    for chave in arquivos:
        empresa_id, servidor_id = ids_do_caminho(chave, config)
        if empresa_id is None:
            print("Aviso: caminho fora do padrão, arquivo ignorado: " + chave)
            ignorados += 1
            continue
        try:
            linhas = ler_arquivo(chave, config, pasta_local, s3)
            limpas = []
            for linha in linhas:
                limpas.append(limpar_amostra(linha, empresa_id, servidor_id))
        except (ValueError, KeyError, TypeError) as erro:
            print(f"Aviso: arquivo ignorado ({erro}): {chave}")
            ignorados += 1
            continue
        if empresa_id not in por_empresa:
            por_empresa[empresa_id] = []
        por_empresa[empresa_id].extend(limpas)
        print(f"JSON tratado: {chave} | amostras: {len(limpas)}")

    gerado_em = datetime.now(timezone.utc).isoformat()
    total = 0
    for empresa_id in sorted(por_empresa):
        amostras = remover_sobrepostas(por_empresa[empresa_id])
        total += len(amostras)

        silver = {"versao": 2, "empresa_id": empresa_id, "gerado_em": gerado_em, "amostras": amostras}
        salvar_json(config["silver"] + "empresas/" + empresa_id + "/normalizado.json",
                    silver, config["bucket_silver"], pasta_local, s3)

        gold = montar_gold(empresa_id, amostras, gerado_em)
        salvar_json(config["gold"] + "empresas/" + empresa_id + "/dashboard.json",
                    gold, config["bucket_gold"], pasta_local, s3)

    return {"arquivos": len(arquivos), "ignorados": ignorados,
            "empresas": len(por_empresa), "amostras": total}


# ============================================================
# 5. ENTRADA DA AWS LAMBDA (gatilho: novo arquivo na Bronze)
# ============================================================

def lambda_handler(event, context):
    carregar_ambiente()
    config = buscar_configuracao()
    novo_arquivo = False
    for registro in event.get("Records", []):
        chave = unquote_plus(registro.get("s3", {}).get("object", {}).get("key", ""))
        if chave.startswith(config["bronze"]) and chave.endswith(".json"):
            novo_arquivo = True
    if not novo_arquivo:
        return {"ignorado": True}
    return executar_etl()


# ============================================================
# 6. EXECUÇÃO PELO TERMINAL (local ou S3)
# ============================================================

def main():
    carregar_ambiente()
    argumentos = argparse.ArgumentParser(description="Leitura Bronze -> Silver -> Gold")
    argumentos.add_argument("--local", metavar="PASTA", help="Lê e grava em uma pasta local (ex.: ./dados)")
    argumentos.add_argument("--s3", action="store_true", help="Lê e grava nos buckets do .env")
    opcoes = argumentos.parse_args()
    if bool(opcoes.local) == opcoes.s3:
        argumentos.error("Escolha --local ./dados OU --s3")
    resultado = executar_etl(opcoes.local) if opcoes.local else executar_etl()
    print("ETL concluído: " + json.dumps(resultado, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, FileNotFoundError) as erro:
        print("ETL interrompido: " + str(erro), file=sys.stderr)
        sys.exit(1)
