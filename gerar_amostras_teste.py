"""Simula UM dia por execução para o equipamento já cadastrado desta máquina.

Rode no terminal para rodar local: python3 gerar_amostras_teste.py --local --data 2026-10-01
Próximas:         python3 gerar_amostras_teste.py --local
Com envio para a AWS:       python3 gerar_amostras_teste.py --enviar --bucket SEU_BUCKET

--local também consulta o MySQL: não inventa IDs nem altera cadastros.
Hardware, processos, requisições e latências são sintéticos.
Nenhum teste HTTP real é executado. Não gera incidentes/Jira.
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
import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


PASTA = Path(__file__).resolve().parent
CAMPOS = {"CPU": "cpu", "RAM": "ram", "ARMAZENAMENTO": "disco",
          "DISCO": "disco", "REDE": "rede"}
COLUNAS_SONDA = ["CONCORRENCIA_TESTE", "REQUISICOES", "REQUISICOES_SUCESSO",
                 "REQUISICOES_ERRO", "REQUISICOES_POR_SEGUNDO", "LATENCIA_MEDIA_MS",
                 "LATENCIA_P95_MS", "LATENCIA_P99_MS"]
COLUNAS_PROCESSOS = ["ID_EQUIPAMENTO", "DATA_HORA", "PID", "PROCESSO", "CPU_AGREGADA",
                    "RAM_AGREGADA", "RAM_PROCESSO", "TAMANHO_MB", "CAMINHO",
                    "DISCO_MB_S", "CONEXOES", "INICIO_AMOSTRA"]
NOMES_PROCESSOS = ["SIMULADO_api-pedidos", "SIMULADO_fechamento",
                   "SIMULADO_backup", "SIMULADO_monitor"]


def carregar_configuracao():
    from dotenv import load_dotenv
    load_dotenv(PASTA / ".env")
    load_dotenv(PASTA / "Scripts" / ".env")
    fuso = ZoneInfo(os.getenv("FUSO_HORARIO") or "America/Sao_Paulo")
    return fuso


def identificar_equipamento():
    """Mesmo login/MAC da captura, mas SEM cadastro e sem escrita no banco."""
    import pymysql
    conexao = pymysql.connect(
        host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME") or os.getenv("DB_DATABASE"),
        port=int(os.getenv("DB_PORT") or os.getenv("PORT") or "3306"),
        cursorclass=pymysql.cursors.DictCursor, connect_timeout=10,
        autocommit=True)
    try:
        usuario = input("Usuário ou e-mail da InfraWatch: ").strip()
        senha = getpass.getpass("Senha da InfraWatch: ")
        with conexao.cursor() as cursor:
            cursor.execute("""SELECT u.fkEmpresa AS id, e.nome
                FROM usuario u JOIN empresa e ON e.idEmpresa = u.fkEmpresa
                WHERE (u.email = %s OR u.nome = %s) AND u.senha = %s""",
                           (usuario, usuario, senha))
            usuarios = cursor.fetchall()
            del senha
            if len(usuarios) != 1:
                raise ValueError("Login inválido ou ambíguo.")
            empresa = usuarios[0]
            nome = str(empresa["nome"])
            if nome in ("", ".", "..") or any(c in nome for c in ("/", "\\", "\x00")):
                raise ValueError("Nome da empresa incompatível com a pasta de destino.")
            mac = str(uuid.getnode())
            cursor.execute("""SELECT idEquipamento AS id, nome, idMac AS mac, fkEmpresa
                FROM equipamento WHERE idMac = %s AND fkEmpresa = %s""", (mac, empresa["id"]))
            equipamentos = cursor.fetchall()
            if len(equipamentos) != 1:
                raise ValueError("Esta máquina precisa ter exatamente um equipamento cadastrado "
                                 "pelo MAC na empresa do login. Nenhum equipamento será criado.")
            equipamento = equipamentos[0]
            # A captura percorre o catálogo nesta ordem para montar suas colunas.
            cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente")
            catalogo = cursor.fetchall()
            cursor.execute("""SELECT c.idComponente, c.tipo,
                pa.limite_atencao, pa.limite_critico
                FROM parametro_alerta pa JOIN componente c ON c.idComponente = pa.fkComponente
                WHERE pa.fkEquipamento = %s AND pa.ativo = 1""", (equipamento["id"],))
            parametros = cursor.fetchall()
        ativos = {p["idComponente"] for p in parametros}
        componentes = [c for c in catalogo if c["idComponente"] in ativos]
        if not componentes:
            raise ValueError("Equipamento sem componentes ativos. Não será usado catálogo de fallback.")
        limites = {}
        for parametro in parametros:
            campo = CAMPOS.get(str(parametro["tipo"]).upper())
            if campo:
                par = (parametro["limite_atencao"], parametro["limite_critico"])
                par = tuple(float(v) if v is not None else None for v in par)
                if campo in limites and limites[campo] != par:
                    raise ValueError("Existem limites conflitantes para " + campo)
                limites[campo] = par
        if not limites:
            raise ValueError("Não há CPU, RAM, disco ou rede habilitados neste equipamento.")
        return empresa, equipamento, componentes, limites
    finally:
        conexao.close()


def montar_colunas(componentes):
    """Replica os blocos e a ordem de colunas de captura_principal enviado."""
    originais, vistos = [], set()
    dashboard = []
    for componente in componentes:
        tipo = str(componente["tipo"])
        campo = CAMPOS.get(tipo.upper())
        if campo and campo not in dashboard:
            dashboard.append(campo)
        # Mesmo tratamento explícito do ID 6 presente na captura original.
        if componente["idComponente"] == 6:
            continue
        chave = campo or tipo.upper()
        if chave not in vistos:
            originais.append(tipo)
            vistos.add(chave)
    extras = [c for c in ("cpu", "ram", "disco", "rede") if c in dashboard and c not in vistos]
    rede = ["rede_mbps", "rede_capacidade_mbps", "rede_capacidade_reserva"] if "rede" in dashboard else []
    adicionais = ["LATENCIA_MAX_MS", "versao", "processos_completos", "inicio_local",
                  "dia_semana", "hora", "dentro_expediente", "boot_time", "uptime_s",
                  "status_geral", "componentes_em_alerta"]
    adicionais += [f"{c}_{s}" for c in ("cpu", "ram", "disco", "rede") if c in dashboard
                   for s in ("limite_atencao", "limite_critico", "status")]
    adicionais += ["latencia_rede_ms", "latencia_limite_atencao", "latencia_limite_critico",
                   "latencia_status", "processo_lider_cpu", "processo_lider_ram",
                   "processo_lider_disco", "processo_lider_rede", "origem_hardware"]
    colunas = (["id", "data_hora"] + originais + COLUNAS_SONDA +
               ["INICIO_AMOSTRA", "FIM_AMOSTRA"] + extras + rede +
               ["empresa_id", "servidor_id", "id_mac"] + adicionais)
    if len(colunas) != len(set(colunas)):
        raise ValueError("Configuração do catálogo resulta em colunas duplicadas.")
    return colunas, originais, dashboard


def feriados(ano):
    # Nacionais + Sexta-feira Santa, mesmo escopo do calendário da dashboard.
    a, b, c = ano % 19, ano // 100, ano % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    n = h + l - 7 * m + 114
    pascoa = date(ano, n // 31, n % 31 + 1)
    dias = {(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)}
    if ano >= 2024:
        dias.add((11, 20))
    return {date(ano, mes, dia) for mes, dia in dias} | {pascoa - timedelta(days=2)}


def quinto_dia_util(dia):
    atual, quantidade = dia.replace(day=1), 0
    calendario = feriados(dia.year)
    while atual <= dia:
        if atual.weekday() < 5 and atual not in calendario:
            quantidade += 1
            if atual == dia:
                return quantidade == 5
        atual += timedelta(days=1)
    return False


def classificar(valor, limites):
    atencao, critico = limites
    if critico is not None and valor >= critico:
        return "CRITICO"
    if atencao is not None and valor >= atencao:
        return "ATENCAO"
    return "NORMAL"


def valores_sinteticos(inicio, rng, limites):
    """Padrões fictícios de almoço/jantar/5º útil/feriado/fim de semana."""
    valores = {"cpu": 30, "ram": 40, "disco": 45, "rede": 20}
    picos = set()
    if 12 <= inicio.hour < 14:
        picos.add("cpu")
    if 18 <= inicio.hour < 20:
        picos.add("ram")
    if quinto_dia_util(inicio.date()):
        picos.add("ram")
    if inicio.date() in feriados(inicio.year):
        picos.add("rede")
    if inicio.weekday() >= 5:
        valores["cpu"] += 15
        valores["ram"] += 12
    # Variação determinística entre dias para comparação histórica.
    tendencia = (inicio.date().toordinal() % 28) / 3
    for campo in valores:
        base = valores[campo] + tendencia
        if campo in picos:
            critico = limites.get(campo, (None, None))[1]
            base = max(93, (critico + 2) if critico is not None else 93)
        valores[campo] = round(max(0, min(100, base + rng.uniform(-2, 2))), 2)
    return valores


def simular_requisicoes(inicio, rng):
    """Lote HTTP fictício: mesma unidade da sonda da captura, sem acessar URL."""
    pico = 12 <= inicio.hour < 14 or 18 <= inicio.hour < 20
    expediente = inicio.weekday() < 5 and 8 <= inicio.hour < 18
    concorrencia = (20 if pico else (8 if expediente else 2))
    total = concorrencia * 20
    taxa_erro = rng.uniform(0.06, 0.14) if pico else rng.uniform(0, 0.015)
    erros = round(total * taxa_erro)
    media = round(rng.uniform(350, 650) if pico else rng.uniform(35, 110), 2)
    p95 = round(media * rng.uniform(1.4, 1.7), 2)
    p99 = round(p95 * rng.uniform(1.1, 1.25), 2)
    maxima = round(p99 * rng.uniform(1.05, 1.2), 2)
    # O lote dura menos que o intervalo de 5 min; RPS é taxa do lote,
    # assim como na captura, e não total dividido pela janela de hardware.
    duracao_lote = (total / concorrencia) * (media / 1000) * 1.1
    return dict(zip(COLUNAS_SONDA + ["LATENCIA_MAX_MS"], [
        concorrencia, total, total - erros, erros,
        round(total / duracao_lote, 2), media, p95, p99, maxima]))


def simular_latencia(inicio, rng, habilitada, atencao, critico):
    if not habilitada:
        return {"latencia_rede_ms": "", "latencia_limite_atencao": "",
                "latencia_limite_critico": "", "latencia_status": ""}
    if 18 <= inicio.hour < 20:
        valor = critico + rng.uniform(10, 60)
    elif 12 <= inicio.hour < 14:
        valor = atencao + (critico - atencao) * rng.uniform(0.2, 0.8)
    else:
        valor = max(0.01, atencao * rng.uniform(0.1, 0.4))
    valor = round(valor, 2)
    return {"latencia_rede_ms": valor, "latencia_limite_atencao": atencao,
            "latencia_limite_critico": critico,
            "latencia_status": classificar(valor, (atencao, critico))}


def gerar_csvs(destino, dia, fuso, empresa, equipamento, componentes, limites):
    colunas, originais, dashboard = montar_colunas(componentes)
    desconhecidos = [tipo for tipo in originais if tipo.upper() not in CAMPOS
                    and tipo.upper() not in ("PROCESSOS", "PROCESSO")]
    if desconhecidos:
        raise ValueError("Métrica sem regra de simulação: " + ", ".join(desconhecidos) +
                         ". Interrompido para não inventar uma unidade/valor incompatível.")
    inicio_dia = datetime.combine(dia, time.min, fuso)
    termino = datetime.combine(dia + timedelta(days=1), time.min, fuso)
    timestamp = inicio_dia.strftime("%Y-%m-%d_%H-%M-%S")
    parte = Path(empresa["nome"]) / str(equipamento["id"])
    hardware = destino / "bronze" / "hardware" / parte / f"captura_{timestamp}.csv"
    processos = destino / "bronze" / "processo" / parte / f"captura_processos_{timestamp}.csv"
    if hardware.exists() or processos.exists():
        raise ValueError("Já existe CSV local para este dia/equipamento. Nada foi sobrescrito.")
    rng = random.Random(f"InfraWatch-diario-v3:{empresa['id']}:{equipamento['id']}:{dia}")
    boot = inicio_dia - timedelta(days=5)
    capacidade = float(os.getenv("CAPACIDADE_REDE_MBPS") or "100")
    if capacidade <= 0:
        raise ValueError("CAPACIDADE_REDE_MBPS precisa ser positiva.")
    latencia_ativa = ("rede" in dashboard and
        os.getenv("LATENCIA_ATIVA", "true").lower() not in ("0", "false", "nao", "não", "off"))
    latencia_atencao = float(os.getenv("LATENCIA_LIMITE_ATENCAO") or "200")
    latencia_critico = float(os.getenv("LATENCIA_LIMITE_CRITICO") or "300")
    if latencia_ativa and not (0 < latencia_atencao < latencia_critico):
        raise ValueError("Limites de latência devem satisfazer: 0 < atenção < crítico.")
    hardware.parent.mkdir(parents=True, exist_ok=True)
    processos.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with hardware.open("x", encoding="utf-8", newline="") as fh, processos.open("x", encoding="utf-8", newline="") as fp:
        wh = csv.DictWriter(fh, fieldnames=colunas, delimiter=";")
        wp = csv.DictWriter(fp, fieldnames=COLUNAS_PROCESSOS, delimiter=";")
        wh.writeheader()
        wp.writeheader()
        inicio = inicio_dia
        while inicio < termino:
            fim = min(inicio + timedelta(minutes=5), termino)
            iso = inicio.astimezone(timezone.utc).isoformat()
            horario = inicio.strftime("%Y-%m-%d %H:%M:%S")
            valores = valores_sinteticos(inicio, rng, limites)
            linha = dict.fromkeys(colunas, "")
            for tipo in originais:
                campo = CAMPOS.get(tipo.upper())
                linha[tipo] = valores[campo] if campo else len(NOMES_PROCESSOS)
            linha.update({c: valores[c] for c in dashboard if c in colunas})
            linha.update({"id": equipamento["id"], "data_hora": horario,
                "INICIO_AMOSTRA": iso, "FIM_AMOSTRA": fim.astimezone(timezone.utc).isoformat(),
                "empresa_id": empresa["id"], "servidor_id": equipamento["id"], "id_mac": equipamento["mac"],
                "versao": "SIMULADO-diario-v3", "processos_completos": True,
                "inicio_local": inicio.isoformat(), "dia_semana": inicio.weekday(), "hora": inicio.hour,
                "dentro_expediente": int(inicio.weekday() < 5 and int(os.getenv("EXPEDIENTE_INICIO") or "8") <= inicio.hour < int(os.getenv("EXPEDIENTE_FIM") or "18")),
                "boot_time": boot.astimezone(timezone.utc).isoformat(timespec="seconds"), "uptime_s": int((inicio - boot).total_seconds()),
                "origem_hardware": "SIMULADO"})
            if "rede" in dashboard:
                linha.update({"rede_mbps": round(valores["rede"] * capacidade / 100, 2),
                    "rede_capacidade_mbps": capacidade, "rede_capacidade_reserva": True})
            estados = []
            alertas = []
            for campo in dashboard:
                par = limites[campo]
                estado = classificar(valores[campo], par)
                linha.update({campo + "_limite_atencao": par[0], campo + "_limite_critico": par[1], campo + "_status": estado})
                estados.append(estado)
                if estado != "NORMAL":
                    alertas.append(f"{campo}:{estado}")
            linha.update(simular_requisicoes(inicio, rng))
            linha.update(simular_latencia(inicio, rng, latencia_ativa, latencia_atencao, latencia_critico))
            if linha["latencia_status"]:
                estados.append(linha["latencia_status"])
                if linha["latencia_status"] != "NORMAL":
                    alertas.append("latencia:" + linha["latencia_status"])
            linha["status_geral"] = "CRITICO" if "CRITICO" in estados else ("ATENCAO" if alertas else "NORMAL")
            linha["componentes_em_alerta"] = "|".join(alertas)
            pesos_cpu = [0.60, 0.15, 0.10, 0.05] if inicio.weekday() < 5 else [0.15, 0.10, 0.60, 0.05]
            pesos_ram = [0.15, 0.60, 0.10, 0.05] if inicio.weekday() < 5 else [0.10, 0.15, 0.60, 0.05]
            tem_processos = "cpu" in dashboard or "ram" in dashboard
            linha["processos_completos"] = tem_processos
            processos_amostra = []
            for indice, nome in enumerate(NOMES_PROCESSOS if tem_processos else []):
                cpu = round(valores["cpu"] * pesos_cpu[indice], 2) if "cpu" in dashboard else ""
                ram = round(valores["ram"] * pesos_ram[indice], 2) if "ram" in dashboard else ""
                # RAM_PROCESSO é % de memória no inventário; TAMANHO_MB é
                # tamanho do executável, não a RAM utilizada pelo processo.
                tamanho_mb = [85.5, 42.0, 120.0, 15.0][indice]
                pico_io = (1 <= inicio.hour < 3 and indice == 2)
                io_mb_s = round(rng.uniform(35, 70) if pico_io else rng.uniform(0.2, 3), 4)
                conexoes = (int(5 + valores["rede"] * [1.2, 0.3, 0.1, 0.05][indice])
                            if "rede" in dashboard else "")
                processos_amostra.append({"nome": nome, "cpu": cpu, "ram": ram,
                    "disco": io_mb_s, "rede": conexoes})
                wp.writerow(dict(zip(COLUNAS_PROCESSOS, [equipamento["id"], horario, 8000 + indice,
                    nome, cpu, ram, ram, tamanho_mb, "/SIMULADO/" + nome,
                    io_mb_s, conexoes, iso])))
            for campo in dashboard:
                candidatos = [p for p in processos_amostra if isinstance(p[campo], (int, float)) and p[campo] > 0]
                linha["processo_lider_" + campo] = (
                    max(candidatos, key=lambda p: p[campo])["nome"] if candidatos else "")
            wh.writerow(linha)
            total += 1
            inicio = fim
    return [processos, hardware], total


def enviar_csvs(bucket, destino, arquivos):
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_REGION") or "us-east-1").client("s3")
    pendentes = []
    for arquivo in arquivos:
        chave = arquivo.relative_to(destino).as_posix()
        checksum = hashlib.sha256(arquivo.read_bytes()).hexdigest()
        try:
            objeto = s3.head_object(Bucket=bucket, Key=chave)
        except ClientError as erro:
            if str(erro.response.get("Error", {}).get("Code")) not in ("404", "NoSuchKey", "NotFound"):
                raise
            pendentes.append((arquivo, chave, checksum))
        else:
            if objeto.get("Metadata", {}).get("sha256") != checksum:
                raise ValueError("Já existe objeto diferente: " + chave + ". Nada será sobrescrito.")
            print("Já enviado, mantido:", chave)
    for arquivo, chave, checksum in pendentes:
        with arquivo.open("rb") as conteudo:
            s3.put_object(Bucket=bucket, Key=chave, Body=conteudo, IfNoneMatch="*",
                          ContentType="text/csv; charset=utf-8",
                          Metadata={"origem": "simulado", "sha256": checksum})
        print("Enviado:", chave)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modo = parser.add_mutually_exclusive_group(required=True)
    modo.add_argument("--local", action="store_true", help="Consulta o banco e gera os CSVs, sem AWS")
    modo.add_argument("--enviar", action="store_true", help="Gera os CSVs e pede confirmação para enviar")
    parser.add_argument("--data", type=date.fromisoformat, help="Dia inicial/explícito no formato AAAA-MM-DD")
    parser.add_argument("--bucket", help="Bucket Bronze (ou S3_BUCKET_BRONZE do .env)")
    args = parser.parse_args()
    fuso = carregar_configuracao()
    bucket = (args.bucket or os.getenv("S3_BUCKET_BRONZE") or "").strip()
    if args.enviar and (not bucket or "/" in bucket):
        raise ValueError("Informe --bucket NOME ou S3_BUCKET_BRONZE, sem s3:// ou barras.")
    empresa, equipamento, componentes, limites = identificar_equipamento()
    destino = PASTA / "dados_simulados" / "diario_v3"
    destino.mkdir(parents=True, exist_ok=True)
    estado_path = destino / f"controle_{empresa['id']}_{equipamento['id']}.json"
    # O bloqueio impede duas execuções simultâneas para o mesmo equipamento.
    bloqueio = destino / f"controle_{empresa['id']}_{equipamento['id']}.lock"
    try:
        lock = bloqueio.open("x")
    except FileExistsError:
        raise ValueError("Já há uma execução para este equipamento (arquivo .lock presente).") from None
    try:
        with lock:
            estado = json.loads(estado_path.read_text(encoding="utf-8")) if estado_path.exists() else {"dias": {}}
            if args.data:
                dia = args.data
            elif estado["dias"]:
                dia = max(date.fromisoformat(d) for d in estado["dias"]) + timedelta(days=1)
            else:
                raise ValueError("Na primeira execução informe --data AAAA-MM-DD. As próximas avançam um dia.")
            if dia >= datetime.now(fuso).date():
                raise ValueError("Escolha um dia anterior a hoje para não simular horários futuros.")
            registro = estado["dias"].get(str(dia))
            print(f"\nEmpresa: {empresa['nome']} (ID {empresa['id']})")
            print(f"Equipamento existente: {equipamento['nome']} (ID {equipamento['id']})")
            print(f"Dia SINTÉTICO: {dia} | Componentes: {', '.join(limites)}")
            print("Nenhum cadastro, limite ou dado existente será alterado no banco.")
            print("Hardware, processos, requisições e latências SINTÉTICOS. Nenhum HTTP real.")
            print("Incidentes/Jira e cadastros de negócio não são simulados.")
            if args.enviar:
                print(f"Bucket: {bucket}. Os dados vão se misturar ao histórico DESTE equipamento.")
                print("O envio pode acionar Lambda e afetar outras dashboards do site.")
            if input("Digite GERAR para continuar: ").strip() != "GERAR":
                print("Cancelado.")
                return
            if registro:
                arquivos = [destino / p for p in registro["arquivos"]]
                if not all(p.is_file() for p in arquivos):
                    raise ValueError("CSV de dia já registrado não encontrado. Controle mantido, sem regeneração automática.")
                for arquivo, esperado in zip(arquivos, registro["sha256"]):
                    if hashlib.sha256(arquivo.read_bytes()).hexdigest() != esperado:
                        raise ValueError("CSV registrado foi alterado. Envio bloqueado.")
                print("Reutilizando os CSVs deste dia, sem gerar novas amostras.")
            else:
                arquivos, total = gerar_csvs(destino, dia, fuso, empresa, equipamento, componentes, limites)
                registro = {"origem": "SIMULADO", "amostras": total,
                    "arquivos": [p.relative_to(destino).as_posix() for p in arquivos],
                    "sha256": [hashlib.sha256(p.read_bytes()).hexdigest() for p in arquivos], "enviado": False}
                estado["dias"][str(dia)] = registro
            estado.update({"empresa": empresa, "equipamento": equipamento})
            temporario = estado_path.with_suffix(".tmp")
            temporario.write_text(json.dumps(estado, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            temporario.replace(estado_path)
            for arquivo in arquivos:
                print(arquivo)
            print(f"{registro['amostras']} amostras de 5 min, somente {dia}, um equipamento.")
            if args.enviar:
                if input("Digite ENVIAR para enviar os CSVs ao S3: ").strip() != "ENVIAR":
                    print(f"CSV mantido localmente. Para enviar depois, repita com --data {dia} --enviar.")
                    return
                enviar_csvs(bucket, destino, arquivos)
                registro["enviado"] = True
                registro["bucket"] = bucket
                temporario.write_text(json.dumps(estado, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
                temporario.replace(estado_path)
                print("Envio concluído. Confira Lambda/Gold: upload não confirma processamento.")
            print("Na próxima execução sem --data, o controle avança para o próximo dia gerado.")
    finally:
        bloqueio.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido. Confira os CSVs locais antes de repetir.")
        sys.exit(1)
    except Exception as erro:
        print(f"Falha ({type(erro).__name__}).")
        if isinstance(erro, ValueError):
            print(str(erro))
        elif hasattr(erro, "response"):
            print("Código AWS:", erro.response.get("Error", {}).get("Code", "indisponível"))
        elif erro.args and isinstance(erro.args[0], int):
            print("Código do banco:", erro.args[0])
        print("Não envie credenciais. Nenhum cadastro foi criado/removido pelo simulador.")
        sys.exit(1)
