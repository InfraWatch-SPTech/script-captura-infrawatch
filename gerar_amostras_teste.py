"""Dados SINTÉTICOS para a InfraWatch. Não importa nem executa a captura real.

Na raiz do projeto:
  python3 gerar_amostras_teste.py --local
  python3 gerar_amostras_teste.py --enviar
"""
import argparse
import csv
import getpass
import hashlib
import json
import os
from pathlib import Path
import random
import sys
from datetime import date, datetime, timedelta, timezone


PASTA = Path(__file__).resolve().parent
BUCKET = "aws-s3-infrawatch-testes"
LOTE = "padroes_v1_2026-04-08"
FUSO = timezone(timedelta(hours=-3))
PRIMEIRO_DIA = date(2026, 3, 26)
ULTIMO_DIA = date(2026, 4, 8)
FERIADOS = {date(2026, 4, 3)}  # Sexta-feira Santa, igual ao calendário da tela.
PERFIS = [
    ("cpu", "SIMULADO - CPU no almoco"),
    ("ram", "SIMULADO - RAM no quinto dia util"),
    ("rede", "SIMULADO - Rede no feriado"),
    ("saudavel", "SIMULADO - Servidor saudavel"),
]
PROCESSOS = ["SIMULADO_api-pedidos", "SIMULADO_fechamento", "SIMULADO_backup", "SIMULADO_monitor"]
COLUNAS_HARDWARE = ['id', 'data_hora', 'CPU', 'RAM', 'ARMAZENAMENTO', 'REDE', 'CONCORRENCIA_TESTE', 'REQUISICOES', 'REQUISICOES_SUCESSO', 'REQUISICOES_ERRO', 'REQUISICOES_POR_SEGUNDO', 'LATENCIA_MEDIA_MS', 'LATENCIA_P95_MS', 'LATENCIA_P99_MS', 'INICIO_AMOSTRA', 'FIM_AMOSTRA', 'rede_mbps', 'rede_capacidade_mbps', 'rede_capacidade_reserva', 'empresa_id', 'servidor_id', 'id_mac', 'LATENCIA_MAX_MS', 'versao', 'processos_completos', 'inicio_local', 'dia_semana', 'hora', 'dentro_expediente', 'boot_time', 'uptime_s', 'status_geral', 'componentes_em_alerta', 'cpu_limite_atencao', 'cpu_limite_critico', 'cpu_status', 'ram_limite_atencao', 'ram_limite_critico', 'ram_status', 'disco_limite_atencao', 'disco_limite_critico', 'disco_status', 'rede_limite_atencao', 'rede_limite_critico', 'rede_status', 'latencia_rede_ms', 'latencia_limite_atencao', 'latencia_limite_critico', 'latencia_status', 'processo_lider_cpu', 'processo_lider_ram', 'processo_lider_disco', 'processo_lider_rede']
COLUNAS_PROCESSOS = ['ID_EQUIPAMENTO', 'DATA_HORA', 'PID', 'PROCESSO', 'CPU_AGREGADA', 'RAM_AGREGADA', 'RAM_PROCESSO', 'TAMANHO_MB', 'CAMINHO', 'DISCO_MB_S', 'CONEXOES', 'INICIO_AMOSTRA']


def quinto_dia_util(dia):
    atual = dia.replace(day=1)
    uteis = 0
    while atual <= dia:
        if atual.weekday() < 5 and atual not in FERIADOS:
            uteis += 1
        if atual == dia:
            return uteis == 5 and atual.weekday() < 5 and atual not in FERIADOS
        atual += timedelta(days=1)


def status(valor):
    if valor >= 90:
        return "CRITICO"
    if valor >= 60:
        return "ATENCAO"
    return "NORMAL"


def percentual(valor, rng):
    return round(max(1, min(99.5, valor + rng.uniform(-3, 3))), 2)


def valores_do_intervalo(perfil, inicio, indice, rng):
    almoco = 12 <= inicio.hour < 14
    quinto = quinto_dia_util(inicio.date())
    feriado = inicio.date() in FERIADOS
    cpu, ram, disco, rede = 30, 38, 45, 20
    if perfil == "cpu":
        cpu = 95 if almoco else (74 if 18 <= inicio.hour < 20 else 40)
        ram = 73 if almoco else 44
        disco = 69
    elif perfil == "ram":
        ram = 95 if quinto else (75 if almoco else 47)
        cpu = 76 if quinto else 35
        disco = 82
    elif perfil == "rede":
        rede = 96 if feriado else (76 if 1 <= inicio.hour < 3 else 32)
        ram = 72 if feriado else 40
        disco = 60
    else:
        cpu, ram, disco, rede = 22, 29, 37, 13
        if indice % 288 == 150:  # Um pico de cinco minutos por dia.
            cpu = 65
    valores = dict(zip(("cpu", "ram", "disco", "rede"),
                       (percentual(v, rng) for v in (cpu, ram, disco, rede))))
    pesos = [0.25, 0.25, 0.25, 0.15]
    if almoco:
        pesos = [0.60, 0.12, 0.10, 0.08]
    elif quinto:
        pesos = [0.12, 0.60, 0.10, 0.08]
    elif feriado:
        pesos = [0.12, 0.10, 0.60, 0.08]
    processos = []
    for nome, peso in zip(PROCESSOS, pesos):
        processos.append({"nome": nome, "cpu": round(valores["cpu"] * peso, 2),
                          "ram": round(valores["ram"] * peso, 2)})
    return valores, processos


def mac_simulado(empresa_id, perfil):
    resumo = hashlib.sha256(f"SIMULADO:InfraWatch:{empresa_id}:{LOTE}:{perfil}".encode()).digest()
    # Endereço local/unicast, estável entre execuções; não usa o MAC do computador.
    return str(int.from_bytes(bytes([2]) + resumo[:5], "big"))


def gerar_csvs(destino, empresa, servidores):
    arquivos = []
    resumo = []
    for servidor in servidores:
        rng = random.Random("InfraWatch-" + servidor["perfil"] + LOTE)
        hardware = destino / "bronze" / "hardware" / empresa["nome"] / str(servidor["id"])
        processos_pasta = destino / "bronze" / "processo" / empresa["nome"] / str(servidor["id"])
        hardware.mkdir(parents=True, exist_ok=True)
        processos_pasta.mkdir(parents=True, exist_ok=True)
        caminho_h = hardware / f"captura_SIMULADO_{LOTE}.csv"
        caminho_p = processos_pasta / f"captura_processos_SIMULADO_{LOTE}.csv"
        total = 0
        with caminho_h.open("w", encoding="utf-8", newline="") as fh, caminho_p.open("w", encoding="utf-8", newline="") as fp:
            wh = csv.DictWriter(fh, fieldnames=COLUNAS_HARDWARE, delimiter=";")
            wp = csv.DictWriter(fp, fieldnames=COLUNAS_PROCESSOS, delimiter=";")
            wh.writeheader()
            wp.writeheader()
            inicio = datetime.combine(PRIMEIRO_DIA, datetime.min.time(), FUSO)
            termino = datetime.combine(ULTIMO_DIA + timedelta(days=1), datetime.min.time(), FUSO)
            boot = inicio - timedelta(days=5)
            while inicio < termino:
                fim = inicio + timedelta(minutes=5)
                valores, processos = valores_do_intervalo(servidor["perfil"], inicio, total, rng)
                iso = inicio.astimezone(timezone.utc).isoformat()
                linha = {campo: "" for campo in COLUNAS_HARDWARE}
                linha.update({
                    "id": servidor["id"], "data_hora": inicio.strftime("%Y-%m-%d %H:%M:%S"),
                    "CPU": valores["cpu"], "RAM": valores["ram"],
                    "ARMAZENAMENTO": valores["disco"], "REDE": valores["rede"],
                    "INICIO_AMOSTRA": iso, "FIM_AMOSTRA": fim.astimezone(timezone.utc).isoformat(),
                    "rede_mbps": round(valores["rede"] * 10, 2),
                    "rede_capacidade_mbps": 1000, "rede_capacidade_reserva": False,
                    "empresa_id": empresa["id"], "servidor_id": servidor["id"], "id_mac": servidor["mac"],
                    "versao": "SIMULADO-padroes-v1", "processos_completos": True,
                    "inicio_local": inicio.isoformat(), "dia_semana": inicio.weekday(), "hora": inicio.hour,
                    "dentro_expediente": 8 <= inicio.hour < 18,
                    "boot_time": boot.timestamp(), "uptime_s": int((inicio - boot).total_seconds()),
                    "processo_lider_cpu": max(processos, key=lambda p: p["cpu"])["nome"],
                    "processo_lider_ram": max(processos, key=lambda p: p["ram"])["nome"],
                    "processo_lider_disco": PROCESSOS[2], "processo_lider_rede": PROCESSOS[2],
                })
                em_alerta = []
                for campo, valor in valores.items():
                    linha[campo + "_limite_atencao"] = 60
                    linha[campo + "_limite_critico"] = 90
                    linha[campo + "_status"] = status(valor)
                    if valor >= 60:
                        em_alerta.append(campo)
                linha["status_geral"] = "CRITICO" if max(valores.values()) >= 90 else ("ATENCAO" if em_alerta else "NORMAL")
                linha["componentes_em_alerta"] = ",".join(em_alerta)
                # Sonda e latência também fictícias: não são feitas requisições HTTP.
                latencia = round(35 + max(valores.values()) * 2, 2)
                erros = 4 if max(valores.values()) >= 90 else 0
                linha.update({"CONCORRENCIA_TESTE": 8, "REQUISICOES": 100,
                              "REQUISICOES_SUCESSO": 100 - erros, "REQUISICOES_ERRO": erros,
                              "REQUISICOES_POR_SEGUNDO": 20, "LATENCIA_MEDIA_MS": latencia,
                              "LATENCIA_P95_MS": round(latencia * 1.2, 2),
                              "LATENCIA_P99_MS": round(latencia * 1.4, 2),
                              "LATENCIA_MAX_MS": round(latencia * 1.5, 2),
                              "latencia_rede_ms": latencia, "latencia_limite_atencao": 200,
                              "latencia_limite_critico": 300,
                              "latencia_status": "ATENCAO" if latencia >= 200 else "NORMAL"})
                wh.writerow(linha)
                for indice, processo in enumerate(processos):
                    wp.writerow(dict(zip(COLUNAS_PROCESSOS, [
                        servidor["id"], linha["data_hora"], 8000 + indice, processo["nome"],
                        processo["cpu"], processo["ram"], round(processo["ram"] * 163.84, 2),
                        100 + indice * 25, "/SIMULADO/" + processo["nome"],
                        round(0.5 + indice * 0.3, 2), indice + 1, iso,
                    ])))
                total += 1
                inicio = fim
        arquivos.extend([caminho_p, caminho_h])  # Processos antes do hardware.
        resumo.append({**servidor, "amostras": total, "minutos_ficticios": total * 5})
    manifesto = {"origem": "SIMULADO", "bucket": BUCKET, "empresa": empresa,
                 "primeiro_dia": str(PRIMEIRO_DIA), "ultimo_dia": str(ULTIMO_DIA),
                 "servidores": resumo, "arquivos": [str(p) for p in arquivos]}
    (destino / "manifesto.json").write_text(json.dumps(manifesto, ensure_ascii=False, indent=2), encoding="utf-8")
    return arquivos


def cadastrar_servidores(conexao, empresa_id):
    servidores = []
    with conexao.cursor() as cursor:
        cursor.execute("SELECT idComponente, tipo FROM componente")
        catalogo = cursor.fetchall()
        componentes = {}
        for campo, aliases in {"cpu": ("CPU",), "ram": ("RAM",),
                               "disco": ("ARMAZENAMENTO", "DISCO"), "rede": ("REDE",)}.items():
            encontrados = [c for c in catalogo if str(c["tipo"]).upper() in aliases]
            if len(encontrados) != 1:
                raise ValueError(f"Esperado um componente de {campo}; encontrados: {len(encontrados)}. Nenhum componente será inventado.")
            componentes[campo] = encontrados[0]["idComponente"]
        for perfil, nome in PERFIS:
            mac = mac_simulado(empresa_id, perfil)
            cursor.execute("SELECT idEquipamento, nome, fkEmpresa, tipo FROM equipamento WHERE idMac = %s FOR UPDATE", (mac,))
            existentes = cursor.fetchall()
            if len(existentes) > 1:
                raise ValueError("Há mais de um equipamento com o MAC de teste. Operação interrompida.")
            if existentes:
                existente = existentes[0]
                if str(existente["fkEmpresa"]) != str(empresa_id) or existente["nome"] != nome or existente["tipo"] != "Servidor":
                    raise ValueError("MAC já usado por outro cadastro. Nada será sobrescrito.")
                equipamento_id = existente["idEquipamento"]
            else:
                cursor.execute("""INSERT INTO equipamento (nome, tipo, status, idMac, fkEmpresa)
                                  VALUES (%s, 'Servidor', 'Online', %s, %s)""", (nome, mac, empresa_id))
                equipamento_id = cursor.lastrowid
            for campo, componente_id in componentes.items():
                cursor.execute("""SELECT limite_atencao, limite_critico, ativo, unidade FROM parametro_alerta
                                  WHERE fkEquipamento = %s AND fkComponente = %s""", (equipamento_id, componente_id))
                parametro = cursor.fetchone()
                if parametro:
                    if (float(parametro["limite_atencao"]) != 60 or float(parametro["limite_critico"]) != 90
                            or parametro["ativo"] != 1 or parametro["unidade"] != "%"):
                        raise ValueError("Parâmetro de um servidor simulado foi alterado. Não será sobrescrito.")
                else:
                    cursor.execute("""INSERT INTO parametro_alerta
                        (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente)
                        VALUES (%s, 60, 90, %s, 1, %s, %s)""",
                                   ("USE_" + campo.upper(), "%", equipamento_id, componente_id))
            servidores.append({"id": equipamento_id, "nome": nome, "perfil": perfil, "mac": mac})
    return servidores


def enviar_csvs(s3, destino, arquivos):
    # Primeiro verifica TODOS os destinos. Nunca sobrescreve objetos existentes.
    from botocore.exceptions import ClientError
    pendentes = []
    for caminho in arquivos:
        chave = caminho.relative_to(destino).as_posix()
        checksum = hashlib.sha256(caminho.read_bytes()).hexdigest()
        try:
            objeto = s3.head_object(Bucket=BUCKET, Key=chave)
        except ClientError as erro:
            if erro.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
            pendentes.append((caminho, chave, checksum))
        else:
            if objeto.get("Metadata", {}).get("sha256") != checksum:
                raise ValueError(f"Já existe um arquivo diferente em {chave}. Envio interrompido sem sobrescrever.")
            print("Já enviado, mantido:", chave)
    # Ordem global: todos os processos primeiro. Cada hardware contém os 14 dias.
    pendentes.sort(key=lambda item: 0 if "/processo/" in item[1] else 1)
    for caminho, chave, checksum in pendentes:
        print("Enviando:", chave)
        with caminho.open("rb") as arquivo:
            s3.put_object(Bucket=BUCKET, Key=chave, Body=arquivo, IfNoneMatch="*",
                          ContentType="text/csv; charset=utf-8",
                          Metadata={"origem": "simulado", "sha256": checksum})
    print("Envios concluídos. Isso NÃO confirma a execução da Lambda; confira os logs e a Gold.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modo = parser.add_mutually_exclusive_group(required=True)
    modo.add_argument("--local", action="store_true", help="Só gera arquivos de demonstração; não acessa MySQL/AWS")
    modo.add_argument("--enviar", action="store_true", help="Autentica, cadastra quatro servidores e envia para a Bronze")
    args = parser.parse_args()
    destino = PASTA / "dados_simulados" / LOTE
    if args.local:
        empresa = {"id": 999999, "nome": "SIMULADO - SOMENTE LOCAL"}
        servidores = [{"id": 900001 + i, "nome": nome, "perfil": perfil,
                       "mac": mac_simulado(empresa["id"], perfil)} for i, (perfil, nome) in enumerate(PERFIS)]
        print("MODO LOCAL: IDs fictícios, sem conexão ao banco e sem envio.")
    else:
        import boto3
        import pymysql
        from dotenv import load_dotenv
        load_dotenv(PASTA / ".env")
        load_dotenv(PASTA / "Scripts" / ".env")
        nome_banco = os.getenv("DB_NAME") or os.getenv("DB_DATABASE")
        porta = os.getenv("DB_PORT") or os.getenv("PORT") or "3306"
        if not nome_banco:
            raise ValueError("Defina DB_NAME no .env local, como na captura. Não envie credenciais pelo chat.")
        # Usa a cadeia padrão da AWS, incluindo AWS_SESSION_TOKEN do Learner Lab.
        sessao = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            region_name=os.getenv("AWS_REGION") or "us-east-1")
        s3 = sessao.client("s3")
        s3.head_bucket(Bucket=BUCKET)
        conexao = pymysql.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"),
                                   password=os.getenv("DB_PASSWORD"), database=nome_banco,
                                   port=int(porta), cursorclass=pymysql.cursors.DictCursor,
                                   autocommit=False, connect_timeout=10)
        try:
            usuario = input("Usuário/e-mail da InfraWatch: ").strip()
            senha = getpass.getpass("Senha da InfraWatch: ")
            with conexao.cursor() as cursor:
                # Mesmo contrato de autenticação da captura atual; senha nunca é salva no manifesto.
                cursor.execute("""SELECT u.fkEmpresa AS id, e.nome FROM usuario u
                    JOIN empresa e ON e.idEmpresa = u.fkEmpresa
                    WHERE (u.email = %s OR u.nome = %s) AND u.senha = %s""", (usuario, usuario, senha))
                contas = cursor.fetchall()
            del senha
            if len(contas) != 1:
                raise ValueError("Login inválido ou ambíguo. Nenhum cadastro realizado.")
            empresa = contas[0]
            if any(c in str(empresa["nome"]) for c in ("/", "\\")) or str(empresa["nome"]) in (".", "..", ""):
                raise ValueError("Nome da empresa incompatível com o caminho de arquivos.")
            print(f"\nEmpresa: {empresa['nome']} (ID {empresa['id']})")
            print(f"Bucket de TESTES: {BUCKET}")
            print("4 servidores SIMULADOS; CPU/RAM/disco/rede com limites 60% e 90%.")
            print("14 dias FICTÍCIOS: 26/03 a 08/04/2026; 4.032 amostras por servidor.")
            print("Os dados aparecerão também nas outras telas e gerarão custos normais de AWS.")
            print("Não apaga dados reais. Cadastros confirmados permanecem mesmo se o envio falhar.")
            if input("Digite SIMULAR para cadastrar e gerar: ").strip() != "SIMULAR":
                print("Cancelado, sem cadastros.")
                return
            servidores = cadastrar_servidores(conexao, empresa["id"])
            # Gera todos os arquivos antes de confirmar os cadastros no banco.
            arquivos = gerar_csvs(destino, empresa, servidores)
            conexao.commit()
        except Exception:
            conexao.rollback()
            raise
        finally:
            conexao.close()
    if args.local:
        arquivos = gerar_csvs(destino, empresa, servidores)
    print("\nArquivos em:", destino)
    for servidor in servidores:
        print(f"  ID {servidor['id']}: {servidor['nome']}")
    if args.enviar:
        if input("Digite ENVIAR para subir os 8 CSVs à Bronze e acionar a Lambda: ").strip() != "ENVIAR":
            print("Envio cancelado. Os cadastros e arquivos locais foram mantidos.")
            return
        enviar_csvs(s3, destino, arquivos)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido. Nenhum dado real foi apagado.")
        sys.exit(1)
    except Exception as erro:
        print(f"Falha ({type(erro).__name__}): confira banco, permissões e configuração local.")
        if isinstance(erro, ValueError):
            print(str(erro))  # Apenas mensagens de validação escritas neste script.
        elif hasattr(erro, "response"):
            print("Código AWS:", erro.response.get("Error", {}).get("Code", "indisponível"))
        elif erro.args and isinstance(erro.args[0], int):
            print("Código do banco:", erro.args[0])
        print("Não apague arquivos nem cadastros para tentar novamente. Peça ajuda com a etapa que falhou.")
        sys.exit(1)
