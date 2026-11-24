# ============================================================
# LEITURA / ETL (roda na AWS Lambda)
# ------------------------------------------------------------
# Bronze (CSV ou JSON da captura)
#   -> limpa e valida as amostras         -> Silver (normalizado.json)
#   -> detecta incidentes                 -> Silver (incidentes.json)
#   -> separa só o que a dashboard usa    -> Gold   (dashboard.json)
#   -> agrega para a dashboard de padrões -> Gold   (padroes.json)
#
# Entende o CSV do captura_padroes.py (Scripts/captura.py) e os
# arquivos JSON/CSV do captura individual.
#
# CAPTURA CONTÍNUA: defina JANELA_BRONZE_DIAS (ex.: 3). O ETL passa a ler só os arquivos
# recentes da Bronze e guarda os incidentes antigos na Silver (incidentes.json), em vez de
# reprocessar todo o histórico a cada arquivo novo. 0 (padrão) = reprocessa tudo.
#
# Uso local (teste):  python3 leitura.py --local ./dados
# Uso no S3:          python3 leitura.py --s3
# Uso na Lambda:      handler = leitura.lambda_handler
# ============================================================
import argparse
import csv
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote_plus

from leitura_padroes import analisar_padroes, normalizar_campos_padroes, numero
from leitura_processos import (
    MAX_PROCESSOS_GOLD,
    formatar_campos_processos_gold,
    normalizar_campos_processos,
)

PASTA_SCRIPT = Path(__file__).resolve().parent

COMPONENTES = ["cpu", "ram", "disco", "rede"]

# Duração máxima aceita para um intervalo de captura (segundos)
INTERVALO_MAXIMO = 300

# Nomes de coluna do CSV do captura -> nomes usados pelo ETL.
# (O captura grava CPU/RAM em maiúsculas quando vêm da tabela componente.)
ALIAS_COLUNAS = {
    "INICIO_AMOSTRA": "inicio",
    "FIM_AMOSTRA": "fim",
    "CPU": "cpu",
    "RAM": "ram",
    "ARMAZENAMENTO": "disco",
    "DISCO": "disco",
    "REDE": "rede",
    "CPU_PCT": "cpu",
    "RAM_PCT": "ram",
    "DISCO_PCT": "disco",
    "REDE_PCT": "rede",
}


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
        "processos": prefixo("S3_PREFIX_PROCESSO", "bronze/processo/"),
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

def corte_da_janela():
    # None = sem janela (reprocessa toda a Bronze)
    dias = float(os.getenv("JANELA_BRONZE_DIAS") or 0)
    if dias < 0:
        raise ValueError("JANELA_BRONZE_DIAS inválido.")
    return datetime.now(timezone.utc) - timedelta(days=dias) if dias else None


def _recente_local(caminho, corte):
    return corte is None or datetime.fromtimestamp(caminho.stat().st_mtime, timezone.utc) >= corte


def listar_bronze(config, pasta_local=None, s3=None, corte=None):
    arquivos = []
    if pasta_local is not None:
        raiz = Path(pasta_local)
        pasta_bronze = raiz / config["bronze"]
        if pasta_bronze.is_dir():
            for extensao in ("*.json", "*.csv"):
                for caminho in pasta_bronze.rglob(extensao):
                    if _recente_local(caminho, corte):
                        arquivos.append(caminho.relative_to(raiz).as_posix())
    else:
        paginador = s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=config["bucket_bronze"], Prefix=config["bronze"]):
            for objeto in pagina.get("Contents", []):
                if not objeto["Key"].endswith((".json", ".csv")):
                    continue
                if corte is None or objeto["LastModified"] >= corte:
                    arquivos.append(objeto["Key"])
    return sorted(arquivos)


def normalizar_linha_csv(linha):
    # Mapeia colunas do captura para os nomes do ETL (sem sobrescrever valores existentes)
    for origem, destino in ALIAS_COLUNAS.items():
        if not linha.get(destino) and linha.get(origem):
            linha[destino] = linha[origem]
    for campo in ("processos", "PROCESSOS_CPU_RAM"):
        if linha.get(campo):
            linha["processos"] = json.loads(linha[campo])
            break
    if linha.get("processos_completos"):
        linha["processos_completos"] = linha["processos_completos"].strip().lower() in ("1", "true", "sim")
    # Arquivos legados mantêm os IDs e horário nos campos originais.
    linha.setdefault("servidor_id", linha.get("id"))
    linha.setdefault("inicio", linha.get("data_hora"))
    if not linha.get("fim") and linha.get("inicio"):
        linha["fim"] = linha["inicio"]
    return linha


def ler_arquivo(chave, config, pasta_local=None, s3=None):
    if pasta_local is not None:
        conteudo = (Path(pasta_local) / chave).read_text(encoding="utf-8-sig")
    else:
        resposta = s3.get_object(Bucket=config["bucket_bronze"], Key=chave)
        conteudo = resposta["Body"].read().decode("utf-8-sig")
    if chave.lower().endswith(".csv"):
        linhas = list(csv.DictReader(conteudo.splitlines(), delimiter=";"))
        return [normalizar_linha_csv(linha) for linha in linhas]

    dados = json.loads(conteudo)
    if isinstance(dados, dict):
        dados = [dados]
    return dados


def ids_do_caminho(chave, config):
    # Caminhos aceitos: bronze/hardware/EMPRESA/ID_SERVIDOR/arquivo
    # EMPRESA pode ser o ID ou o nome da empresa (o captura grava o nome);
    # o ID da empresa é confirmado pela coluna empresa_id do arquivo.
    partes = chave[len(config["bronze"]):].split("/")
    if len(partes) != 3 or not partes[0] or not partes[1].isdigit():
        return None, None
    return partes[0], str(int(partes[1]))


def ler_processos_separados(config, pasta_local=None, s3=None, corte=None):
    """Lê CSVs de processos separados e agrupa-os pelo servidor e intervalo amostrado."""
    arquivos = []
    if pasta_local is not None:
        raiz = Path(pasta_local)
        pasta_processos = raiz / config["processos"]
        if pasta_processos.is_dir():
            arquivos = [caminho for caminho in pasta_processos.rglob("*.csv") if _recente_local(caminho, corte)]
    else:
        paginador = s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=config["bucket_bronze"], Prefix=config["processos"]):
            arquivos.extend(objeto["Key"] for objeto in pagina.get("Contents", [])
                            if objeto.get("Key", "").lower().endswith(".csv")
                            and (corte is None or objeto["LastModified"] >= corte))

    agrupados = {}
    for arquivo in arquivos:
        if pasta_local is not None:
            conteudo = arquivo.read_text(encoding="utf-8-sig")
        else:
            conteudo = s3.get_object(Bucket=config["bucket_bronze"], Key=arquivo)["Body"].read().decode("utf-8-sig")
        for linha in csv.DictReader(conteudo.splitlines(), delimiter=";"):
            # Aceita o formato do captura (ID_EQUIPAMENTO, PROCESSO, CPU_AGREGADA...)
            # e o formato individual (servidor_id, processo, cpu...)
            servidor = str(linha.get("servidor_id") or linha.get("ID_EQUIPAMENTO") or "").strip()
            inicio = str(linha.get("inicio") or linha.get("INICIO_AMOSTRA") or "").strip()
            if not servidor or not inicio:
                continue
            nome = (linha.get("processo") or linha.get("PROCESSO") or "").strip()
            cpu = linha.get("cpu") or linha.get("CPU_AGREGADA")
            ram = linha.get("ram") or linha.get("RAM_AGREGADA")
            amostra = agrupados.setdefault((servidor, inicio), {"processos": [], "processos_completos": False})
            if linha.get("processos_completos"):
                amostra["processos_completos"] = str(linha["processos_completos"]).strip().lower() in ("1", "true", "sim")
            # Linhas só de inventário (sem CPU/RAM medidos) não entram na amostra
            if not nome or (cpu in (None, "") and ram in (None, "")):
                continue
            amostra["processos"].append({
                "nome": nome,
                "cpu": numero(cpu) or 0.0,
                "ram": numero(ram) or 0.0,
                "disco_mb_s": numero(linha.get("DISCO_MB_S") or linha.get("disco_mb_s")) or 0.0,
                "conexoes": numero(linha.get("CONEXOES") or linha.get("conexoes")) or 0.0,
            })
    return agrupados


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
    if valor is None or valor == "":
        return None
    if isinstance(valor, bool):
        raise ValueError("percentual inválido")
    numero_ = float(valor)
    if not math.isfinite(numero_) or numero_ < 0 or numero_ > 100:
        raise ValueError("percentual fora de 0 a 100")
    return round(numero_, 2)


def resolver_empresa(linha, empresa_pasta):
    empresa_id = str(linha.get("empresa_id") or "").strip()
    if empresa_pasta.isdigit():
        if empresa_id and empresa_id.isdigit() and int(empresa_id) != int(empresa_pasta):
            raise ValueError("empresa do arquivo diferente da empresa do caminho")
        if not empresa_id:
            empresa_id = empresa_pasta
    if not empresa_id.isdigit():
        raise ValueError("empresa_id ausente ou inválido")
    return str(int(empresa_id))


def limpar_amostra(linha, servidor_id, empresa_pasta):
    if str(linha.get("servidor_id")).strip() != servidor_id:
        raise ValueError("servidor_id do arquivo diferente do ID do caminho")
    empresa_id = resolver_empresa(linha, empresa_pasta)

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

    # Campos da dashboard de padrões (limites, status, latência, processo líder...)
    amostra.update(normalizar_campos_padroes(linha))
    amostra.update(normalizar_campos_processos(linha))
    return amostra


def remover_sobrepostas(amostras):
    # Ordena por servidor e horário; descarta repetidas ou sobrepostas
    amostras.sort(key=lambda a: (int(a["servidor_id"]), a["inicio"]))
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


def carregar_json(chave, bucket, pasta_local=None, s3=None):
    # Devolve None só quando o arquivo não existe; qualquer outro erro interrompe o ETL,
    # para nunca sobrescrever o histórico de incidentes com dados parciais.
    if pasta_local is not None:
        caminho = Path(pasta_local) / chave
        if not caminho.is_file():
            return None
        return json.loads(caminho.read_text(encoding="utf-8"))
    try:
        resposta = s3.get_object(Bucket=bucket, Key=chave)
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(resposta["Body"].read().decode("utf-8"))


# ============================================================
# 4. EXECUÇÃO DO ETL
# ============================================================

def executar_etl(pasta_local=None):
    config = buscar_configuracao()
    corte = corte_da_janela()
    s3 = None
    if pasta_local is None:
        if not config["bucket_bronze"]:
            raise ValueError("Configure S3_BUCKET_BRONZE.")
        s3 = conectar_s3()

    arquivos = listar_bronze(config, pasta_local, s3, corte)
    if not arquivos:
        if corte is None:
            raise FileNotFoundError("Nenhum arquivo CSV ou JSON encontrado na Bronze.")
        print("Nenhum arquivo na janela de leitura; nada a processar.")
        return {"arquivos": 0, "ignorados": 0, "linhas_invalidas": 0,
                "empresas": 0, "amostras": 0, "incidentes": 0}
    processos_por_amostra = ler_processos_separados(config, pasta_local, s3, corte)

    # Agrupa as amostras limpas por empresa
    por_empresa = {}
    ignorados = 0
    linhas_invalidas = 0
    for chave in arquivos:
        empresa_pasta, servidor_id = ids_do_caminho(chave, config)
        if servidor_id is None:
            print("Aviso: caminho fora do padrão, arquivo ignorado: " + chave)
            ignorados += 1
            continue
        try:
            linhas = ler_arquivo(chave, config, pasta_local, s3)
        except (ValueError, KeyError, TypeError) as erro:
            print(f"Aviso: arquivo ignorado ({erro}): {chave}")
            ignorados += 1
            continue
        # Uma linha inválida descarta só ela, não o lote inteiro (a captura contínua grava lotes)
        limpas = []
        invalidas = 0
        for linha in linhas:
            try:
                chave_amostra = (servidor_id, str(linha.get("inicio", "")))
                if not linha.get("processos") and chave_amostra in processos_por_amostra:
                    linha.update(processos_por_amostra[chave_amostra])
                limpas.append(limpar_amostra(linha, servidor_id, empresa_pasta))
            except (ValueError, KeyError, TypeError):
                invalidas += 1
        linhas_invalidas += invalidas
        if not limpas:
            print(f"Aviso: arquivo sem linhas válidas, ignorado: {chave}")
            ignorados += 1
            continue
        if invalidas:
            print(f"Aviso: {invalidas} linha(s) inválida(s) descartada(s) em {chave}")
        for amostra in limpas:
            por_empresa.setdefault(amostra["empresa_id"], []).append(amostra)
        print(f"Arquivo tratado: {chave} | amostras: {len(limpas)}")

    gerado_em = datetime.now(timezone.utc).isoformat()
    total = 0
    total_incidentes = 0
    for empresa_id in sorted(por_empresa, key=int):
        amostras = remover_sobrepostas(por_empresa[empresa_id])
        total += len(amostras)
        base_silver = config["silver"] + "empresas/" + empresa_id + "/"
        base_gold = config["gold"] + "empresas/" + empresa_id + "/"

        silver = {"versao": 3, "empresa_id": empresa_id, "gerado_em": gerado_em, "amostras": amostras}
        salvar_json(base_silver + "normalizado.json", silver, config["bucket_silver"], pasta_local, s3)

        # Incidentes (Silver) e agregados da dashboard de padrões (Gold).
        # Com janela de leitura, os incidentes antigos vêm do incidentes.json anterior.
        anterior = None
        if corte is not None:
            anterior = carregar_json(base_silver + "incidentes.json", config["bucket_silver"], pasta_local, s3)
        silver_incidentes, gold_padroes = analisar_padroes(empresa_id, amostras, gerado_em, anterior)
        total_incidentes += len(silver_incidentes["incidentes"])
        salvar_json(base_silver + "incidentes.json", silver_incidentes, config["bucket_silver"], pasta_local, s3)
        salvar_json(base_gold + "padroes.json", gold_padroes, config["bucket_gold"], pasta_local, s3)

        gold = montar_gold(empresa_id, amostras, gerado_em)
        salvar_json(base_gold + "dashboard.json", gold, config["bucket_gold"], pasta_local, s3)

    return {"arquivos": len(arquivos), "ignorados": ignorados, "linhas_invalidas": linhas_invalidas,
            "empresas": len(por_empresa), "amostras": total, "incidentes": total_incidentes}


# ============================================================
# 5. ENTRADA DA AWS LAMBDA (gatilho: novo arquivo na Bronze)
# ============================================================

def lambda_handler(event, context):
    carregar_ambiente()
    config = buscar_configuracao()
    novo_arquivo = False
    for registro in event.get("Records", []):
        chave = unquote_plus(registro.get("s3", {}).get("object", {}).get("key", ""))
        # O captura envia hardware e depois processos: reprocessa nos dois casos
        if chave.startswith((config["bronze"], config["processos"])) and chave.endswith((".json", ".csv")):
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