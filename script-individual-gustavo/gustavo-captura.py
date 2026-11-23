# ============================================================
# CAPTURA INFRAWATCH + DADOS DA DASHBOARD "IDENTIFICAÇÃO DE PADRÕES"
# (versão corrigida)
# ------------------------------------------------------------
# Mantém o fluxo do captura.py e entrega, por amostra,
# o que a dashboard de padrões precisa:
#
#   Dashboard                          Dado capturado
#   ---------------------------------  ---------------------------------------
#   Heatmap hora x dia / meses         inicio_local, dia_semana, hora
#   Duração dentro/fora do expediente  dentro_expediente
#   Incidente (gravidade, limiar)      <comp>_limite_atencao/_critico/_status,
#                                      status_geral, componentes_em_alerta
#   Rede em "ms"                       latencia_rede_ms + limites + status
#   Tempo inativo / reinício           boot_time, uptime_s
#   Processo líder do incidente        processo_lider_cpu/ram/disco/rede
#
# processo_lider_ram  = processo que mais usa memória no instante.
# processo_lider_rede = processo com mais conexões abertas (aproximação).
# ============================================================
import psutil
import csv
from datetime import datetime, timezone, timedelta
import time
import os
import uuid
import socket
import boto3
import io
import pymysql as mysql
import sys
import json
from pathlib import Path
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Rich para interface no terminal
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich import box

# ============================================================
# CONFIGURAÇÃO VISUAL
# ============================================================
console = Console()
AZUL_ESCURO = "#0B1F3A"
AZUL_CLARO = "#38BDF8"
PRETO = "#000000"

BANNER = f"""
[bold {AZUL_CLARO}]██╗   ██╗██╗   ██╗[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]██║   ██║██║   ██║[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]██║   ██║██║   ██║[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]╚██╗ ██╔╝╚██╗ ██╔╝[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}] ╚████╔╝  ╚████╔╝[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]  ╚═══╝    ╚═══╝[/bold {AZUL_CLARO}]
"""

def mostrar_banner():
    console.print(
        Panel(
            Align.center(BANNER),
            border_style=AZUL_CLARO,
            style=f"on {PRETO}",
            padding=(1, 4),
            box=box.ROUNDED
        )
    )

def limpar_tela():
    console.clear()


# ============================================================
# 0. CARREGANDO VARIÁVEIS DE AMBIENTE
# ============================================================
load_dotenv()

PASTA_SCRIPT = Path(__file__).resolve().parent
AWS_ACESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID") or None
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None
AWS_REGION = os.getenv("AWS_REGION") or "us-east-1"

QUANTIDADE_AMOSTRAS = int(os.getenv("QUANTIDADE_AMOSTRAS") or "15")  # 0 = contínuo até Ctrl+C
AMOSTRAS_POR_ARQUIVO = int(os.getenv("AMOSTRAS_POR_ARQUIVO") or "15")
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS") or "5")
TOP_PROCESSOS = int(os.getenv("TOP_PROCESSOS") or "5")
CAMINHO_DISCO = os.getenv("CAMINHO_DISCO") or os.path.abspath(os.sep)
CAPACIDADE_REDE_MBPS = float(os.getenv("CAPACIDADE_REDE_MBPS") or "100")
BUCKET_BRONZE = (os.getenv("S3_BUCKET_BRONZE") or "").strip()
PASTA_LOCAL = Path(os.getenv("DIRETORIO_DADOS") or PASTA_SCRIPT / "dados")
CAMPOS_COMPONENTES = {"CPU": "cpu", "RAM": "ram", "ARMAZENAMENTO": "disco", "DISCO": "disco", "REDE": "rede"}
PREFIXOS_REDE_IGNORADOS = ("lo", "docker", "veth", "br-", "virbr", "vmnet", "vboxnet")
if QUANTIDADE_AMOSTRAS < 0 or AMOSTRAS_POR_ARQUIVO < 1 or INTERVALO_SEGUNDOS <= 0 or INTERVALO_SEGUNDOS > 290:
    raise ValueError("Use QUANTIDADE_AMOSTRAS >= 0, AMOSTRAS_POR_ARQUIVO >= 1 e INTERVALO_SEGUNDOS entre 0 e 290.")

# Execução sem prompts (agendada): informe usuário e senha no .env
USUARIO_AUTO = os.getenv("INFRAWATCH_USUARIO")
SENHA_AUTO = os.getenv("INFRAWATCH_SENHA")
MODO_AUTOMATICO = bool(USUARIO_AUTO and SENHA_AUTO)

# Variáveis BD
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = int(os.getenv("PORT") or os.getenv("DB_PORT") or 3306)

# Sonda HTTP habilitada por padrão. httpbin.org/post simula temporariamente o endpoint do cliente.
REQUEST_PROBE_URL = os.getenv(
    "INFRAWATCH_REQUEST_PROBE_URL", "https://httpbin.org/post"
).strip()
REQUEST_PROBE_METHOD = os.getenv("INFRAWATCH_REQUEST_PROBE_METHOD", "POST").upper()
REQUEST_PROBE_TIMEOUT = float(os.getenv("INFRAWATCH_REQUEST_PROBE_TIMEOUT", "3"))
REQUEST_PROBE_ENABLED = os.getenv("INFRAWATCH_REQUEST_PROBE_ENABLED", "true").lower() not in ("0", "false", "nao", "não", "off")
REQUESTS_POR_NIVEL = int(os.getenv("INFRAWATCH_REQUEST_PROBE_REQUESTS") or "20")
NIVEIS_CONCORRENCIA = [1, 2, 4, 8, 16] * 3

if REQUEST_PROBE_ENABLED and not REQUEST_PROBE_URL:
    raise ValueError("A sonda HTTP está habilitada. Informe INFRAWATCH_REQUEST_PROBE_URL.")
if REQUEST_PROBE_URL and not REQUEST_PROBE_URL.lower().startswith(("http://", "https://")):
    raise ValueError("INFRAWATCH_REQUEST_PROBE_URL precisa começar com http:// ou https://.")
if REQUEST_PROBE_METHOD not in ("GET", "POST"):
    raise ValueError("INFRAWATCH_REQUEST_PROBE_METHOD aceita somente GET ou POST.")

# Fuso e expediente (a dashboard considera seg-sex, 8h às 18h)
try:
    from zoneinfo import ZoneInfo
    FUSO = ZoneInfo(os.getenv("FUSO_HORARIO") or "America/Sao_Paulo")
except Exception:
    FUSO = timezone(timedelta(hours=-3))  # fallback: horário de Brasília
EXPEDIENTE_INICIO = int(os.getenv("EXPEDIENTE_INICIO") or "8")
EXPEDIENTE_FIM = int(os.getenv("EXPEDIENTE_FIM") or "18")

# Limite usado quando o componente está ativo mas sem linha em parametro_alerta
LIMITE_FALLBACK = (60.0, 90.0)

# Parâmetros criados para servidores novos: (tipo do componente, nomeMetrica, atenção, crítico, unidade)
PARAMETROS_PADRAO = [
    ("CPU", "USO_CPU", 60.0, 90.0, "%"),
    ("RAM", "USO_RAM", 60.0, 90.0, "%"),
    ("ARMAZENAMENTO", "USO_DISCO", 60.0, 90.0, "%"),
    ("PROCESSOS", "PROCESSOS_ATIVOS", 65.0, 80.0, "Dict"),
]

# Latência de rede (ms): tempo de conexão TCP até um host de referência
LATENCIA_ATIVA = os.getenv("LATENCIA_ATIVA", "true").lower() not in ("0", "false", "nao", "não", "off")
LATENCIA_HOST = os.getenv("LATENCIA_HOST") or "8.8.8.8"
LATENCIA_PORTA = int(os.getenv("LATENCIA_PORTA") or "53")
LATENCIA_TIMEOUT = float(os.getenv("LATENCIA_TIMEOUT") or "1")
LATENCIA_LIMITE_ATENCAO = float(os.getenv("LATENCIA_LIMITE_ATENCAO") or "200")
LATENCIA_LIMITE_CRITICO = float(os.getenv("LATENCIA_LIMITE_CRITICO") or "300")
LATENCIA_SEM_RESPOSTA_CRITICA = os.getenv("LATENCIA_SEM_RESPOSTA_CRITICA", "true").lower() not in ("0", "false", "nao", "não", "off")

ORDEM_STATUS = {"NORMAL": 0, "ATENCAO": 1, "CRITICO": 2}
PROCESSOS_IGNORADOS_LIDER = {"System Idle Process", "Idle", "System", "Registry", "Memory Compression"}


# ============================================================
# 1. CONEXÃO COM O S3
# ============================================================
BUCKET = "infrawatch-server-s3"
if not BUCKET_BRONZE:
    BUCKET_BRONZE = BUCKET

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

# Captura o idMac único da máquina atual
id_mac = uuid.getnode()


def conectar_banco():
    return mysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=PORT,
        cursorclass=mysql.cursors.DictCursor
    )


# ============================================================
# 2. CONEXÃO COM O BANCO DE DADOS (InfraWatch)
# ============================================================
console.print(
    Panel("[bold white]Conectando ao banco de dados InfraWatch...[/bold white]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)

bd = conectar_banco()
cursor = bd.cursor()

cursor.execute("SELECT idEquipamento, nome, idMac, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente;")
componentes = cursor.fetchall()


def id_componente(tipo):
    for c in componentes:
        if str(c["tipo"]).upper() == tipo:
            return c["idComponente"]
    return None


# ============================================================
# 3. VALIDAÇÃO DO USUARIO
# ============================================================
limpar_tela()
mostrar_banner()

if MODO_AUTOMATICO:
    ipt_usuario, ipt_senha = USUARIO_AUTO, SENHA_AUTO
else:
    console.print(
        Panel(
            Align.center(
                "[bold white]Bem-vindo à InfraWatch![/bold white]\n"
                "[dim white]Insira suas credenciais para continuar.[/dim white] "
            ),
            border_style=AZUL_CLARO,
            style=f"on {PRETO}",
            padding=(1, 3),
        )
    )
    time.sleep(1)
    ipt_usuario = Prompt.ask("[bold white]Usuário ou E-mail[/bold white]")
    ipt_senha = Prompt.ask("[bold white]Senha[/bold white]", password=True)

cursor.execute(
    """
    SELECT u.idUsuario, u.nome, u.email, u.senha, u.fkEmpresa, e.nome AS
    nomeEmpresa
    FROM usuario u
    JOIN empresa e ON u.fkEmpresa = e.idEmpresa
    WHERE (u.email = %s OR u.nome = %s) AND u.senha = %s;
    """,
    (ipt_usuario, ipt_usuario, ipt_senha),
)
usuario_logado = cursor.fetchone()

if not usuario_logado:
    console.print()
    console.print(
        Panel(
            "[bold white]Você não pode acessar nosso serviço (Acesso Negado).[/bold white]",
            title="[bold #38BDF8]ACESSO NEGADO[/bold #38BDF8]",
            border_style=AZUL_CLARO,
            style=f"on {PRETO}"
        )
    )
    cursor.close()
    bd.close()
    sys.exit(1)

fk_empresa = usuario_logado["fkEmpresa"]
nome_empresa = usuario_logado["nomeEmpresa"]
nome_usuario = usuario_logado["nome"]


# ============================================================
# 4. VALIDAÇÃO OU CADASTRO DO SERVIDOR
# ============================================================
cadastrar = True
equipamento_atual = None

for i in range(len(equipamentos)):
    if equipamentos[i]['idMac'] == str(id_mac):
        cadastrar = False
        equipamento_atual = {
            "idEquipamento": equipamentos[i]['idEquipamento'],
            "nome": equipamentos[i]['nome'],
            "idMac": id_mac,
            "fkEmpresa": equipamentos[i]['fkEmpresa']
        }

if cadastrar:
    console.print("\n[bold #38BDF8]--- Cadastro de Novo Servidor ---[/bold #38BDF8]")
    cursor.execute(
        "SELECT COUNT(*) AS total FROM equipamento WHERE fkEmpresa = %s;",
        (fk_empresa,),
    )
    qtd_equip = cursor.fetchone()["total"] + 1
    nome_novo_servidor = f"{nome_empresa}.server_id#{qtd_equip}"
    cursor.execute(
        """
        INSERT INTO equipamento (nome, tipo, status, idMac, fkEmpresa)
        VALUES (%s, 'Servidor', 'Online', %s, %s);
        """,
        (nome_novo_servidor, id_mac, fk_empresa),
    )
    bd.commit()

    id_novo_eq = cursor.lastrowid
    equipamento_atual = {
        "idEquipamento": id_novo_eq,
        "nome": nome_novo_servidor,
        "idMac": id_mac,
        "fkEmpresa": fk_empresa,
    }
    console.print(
        f"[bold green]Novo servidor '{nome_novo_servidor}' cadastrado com sucesso! (ID: {id_novo_eq})[/bold green]\n"
    )

    # Parâmetros padrão (CPU, RAM, DISCO, PROCESSOS), procurando o componente pelo TIPO
    for tipo, nome_metrica, atencao, critico, unidade in PARAMETROS_PADRAO:
        id_comp = id_componente(tipo)
        if id_comp is None:
            console.print(f"[bold yellow]Componente {tipo} não existe no catálogo. Parâmetro não cadastrado.[/bold yellow]")
            continue
        cursor.execute(
            """
            INSERT INTO parametro_alerta
            (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente)
            VALUES (%s, %s, %s, %s, 1, %s, %s);
            """,
            (nome_metrica, atencao, critico, unidade, equipamento_atual["idEquipamento"], id_comp),
        )
        bd.commit()
        console.print(f"[bold green]Métrica {nome_metrica} salva com sucesso![/bold green]")
    console.print()


# ============================================================
# 5. BUSCA MAPEAMENTO DE EQUIPAMENTO E COMPONENTES
# ============================================================
cursor.execute("SELECT DISTINCT fkComponente, fkEquipamento FROM parametro_alerta WHERE ativo = 1;")
equip_comp = cursor.fetchall()

cursor.execute(
    """
    SELECT DISTINCT c.idComponente, c.nome, c.tipo
    FROM componente c
    JOIN parametro_alerta pa ON c.idComponente = pa.fkComponente
    WHERE pa.fkEquipamento = %s AND pa.ativo = 1;
    """,
    (equipamento_atual["idEquipamento"],),
)
componentes_selecionados_objs = cursor.fetchall()

if not componentes_selecionados_objs:
    console.print("[bold yellow]Aviso: Nenhum componente ativo configurado no banco. Carregando lista geral...[/bold yellow]")
    cursor.execute("SELECT idComponente, nome, tipo FROM componente;")
    componentes_selecionados_objs = cursor.fetchall()

if not componentes_selecionados_objs:
    console.print("[bold yellow]Nenhum componente selecionado para monitorar. Encerrando.[/bold yellow]")
    cursor.close()
    bd.close()
    sys.exit(0)

# Limites de atenção/crítico do equipamento, para classificar cada amostra
cursor.execute(
    """
    SELECT c.tipo, pa.limite_atencao, pa.limite_critico
    FROM parametro_alerta pa
    JOIN componente c ON c.idComponente = pa.fkComponente
    WHERE pa.fkEquipamento = %s AND pa.ativo = 1;
    """,
    (equipamento_atual["idEquipamento"],),
)
limites_alerta = {}
for linha_limite in cursor.fetchall():
    campo_limite = CAMPOS_COMPONENTES.get(str(linha_limite["tipo"]).upper())
    if campo_limite:
        limites_alerta[campo_limite] = (
            float(linha_limite["limite_atencao"]),
            float(linha_limite["limite_critico"]),
        )

cursor.close()
bd.close()

if not MODO_AUTOMATICO:
    limpar_tela()
    mostrar_banner()
    console.print(
        Panel(
            f"[bold white]Olá, {usuario_logado['nome']}![/bold white]\n\n"
            "[dim white]As informações da sua máquina serão coletadas automaticamente.[/dim white]",
            title=f"[bold {AZUL_CLARO}]InfraWatch[/bold {AZUL_CLARO}]",
            border_style=AZUL_CLARO,
            style=f"on {AZUL_ESCURO}"
        )
    )
    console.print()
    if not Confirm.ask("[bold white]Deseja continuar?[/bold white]", default=True):
        console.print(
            Panel("[bold white]Fechando script...[/bold white]", border_style=AZUL_CLARO, style=f"on {PRETO}")
        )
        sys.exit(0)


# ============================================================
# 6. IDENTIFICA COMPONENTES DO EQUIPAMENTO ATUAL
# ============================================================
capturar_comp = []
for ec in equip_comp:
    if ec['fkEquipamento'] == equipamento_atual['idEquipamento']:
        capturar_comp.append(ec['fkComponente'])

id_processos = id_componente("PROCESSOS")
enviar_processos = id_processos is not None and id_processos in capturar_comp

componentes_dashboard = []
for c in componentes_selecionados_objs:
    campo = CAMPOS_COMPONENTES.get(c["tipo"].upper())
    if campo and campo not in componentes_dashboard:
        componentes_dashboard.append(campo)
if not componentes_dashboard:
    for c in componentes:
        campo = CAMPOS_COMPONENTES.get(c["tipo"].upper())
        if campo and campo not in componentes_dashboard:
            componentes_dashboard.append(campo)


# ============================================================
# 7. COLUNAS DOS CSVs
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel("[bold white]Preparando captura dos dados...[/bold white]",
          title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)
console.print()

# Colunas dos componentes cadastrados (sem repetir a mesma métrica com nomes equivalentes).
# Os valores vêm da própria amostra, os mesmos usados no status.
componentes_csv = []
campos_componentes_csv = set()
for comp_id in capturar_comp:
    if comp_id == id_processos:
        continue
    for componente in componentes:
        if comp_id != componente['idComponente']:
            continue
        tipo = componente['tipo']
        campo = CAMPOS_COMPONENTES.get(tipo.upper())
        if campo and campo not in campos_componentes_csv:
            componentes_csv.append(componente)
            campos_componentes_csv.add(campo)

titulo_executados = ['id', 'data_hora'] + [c['tipo'] for c in componentes_csv]
campos_metricas_dashboard = [
    campo for campo in ('cpu', 'ram', 'disco', 'rede')
    if campo in componentes_dashboard and campo not in campos_componentes_csv
]
campos_rede_detalhados = (
    ['rede_mbps', 'rede_capacidade_mbps', 'rede_capacidade_reserva']
    if 'rede' in componentes_dashboard else []
)
colunas_sonda_http = [
    'CONCORRENCIA_TESTE', 'REQUISICOES', 'REQUISICOES_SUCESSO',
    'REQUISICOES_ERRO', 'REQUISICOES_POR_SEGUNDO',
    'LATENCIA_MEDIA_MS', 'LATENCIA_P95_MS', 'LATENCIA_P99_MS',
    'LATENCIA_MAX_MS',
]

campos_status = [c for c in ('cpu', 'ram', 'disco', 'rede') if c in componentes_dashboard]
colunas_padroes = (
    ['inicio_local', 'dia_semana', 'hora', 'dentro_expediente',
     'boot_time', 'uptime_s', 'status_geral', 'componentes_em_alerta']
    + [f"{c}_{s}" for c in campos_status for s in ('limite_atencao', 'limite_critico', 'status')]
    + ['latencia_rede_ms', 'latencia_limite_atencao', 'latencia_limite_critico', 'latencia_status']
    + ['processo_lider_cpu', 'processo_lider_ram', 'processo_lider_disco', 'processo_lider_rede']
)

colunas_captura = (
    titulo_executados + colunas_sonda_http + ['INICIO_AMOSTRA', 'FIM_AMOSTRA']
    + campos_metricas_dashboard + campos_rede_detalhados
    + ['empresa_id', 'servidor_id', 'id_mac']
    + colunas_padroes
)

colunas_processos = [
    'ID_EQUIPAMENTO', 'DATA_HORA', 'PID', 'PROCESSO', 'CPU_AGREGADA',
    'RAM_AGREGADA', 'RAM_PROCESSO', 'TAMANHO_MB', 'CAMINHO',
    'DISCO_MB_S', 'CONEXOES', 'INICIO_AMOSTRA',
]


# ============================================================
# 8. SONDA HTTP
# ============================================================
def capturar_requisicoes_http(nivel_concorrencia):
    """Executa um lote HTTP controlado contra o endpoint configurado (9 valores)."""
    colunas = [nivel_concorrencia if REQUEST_PROBE_ENABLED else '', '', '', '', '', '', '', '', '']
    if not REQUEST_PROBE_ENABLED:
        return colunas

    def requisitar():
        inicio = time.perf_counter()
        status = 0
        try:
            requisicao = urllib.request.Request(
                REQUEST_PROBE_URL,
                data=(json.dumps({
                    "origem": "InfraWatch",
                    "tipo": "teste_capacidade",
                    "concorrencia": nivel_concorrencia,
                }).encode("utf-8") if REQUEST_PROBE_METHOD == "POST" else None),
                headers={
                    'User-Agent': 'InfraWatch-Capacity-Probe/1.0',
                    'Content-Type': 'application/json',
                },
                method=REQUEST_PROBE_METHOD
            )
            with urllib.request.urlopen(requisicao, timeout=REQUEST_PROBE_TIMEOUT) as resposta:
                status = resposta.getcode()
                resposta.read(1024)
        except urllib.error.HTTPError as erro:
            status = erro.code
        except Exception:
            status = 0
        duracao_ms = (time.perf_counter() - inicio) * 1000
        return status, duracao_ms

    total = max(REQUESTS_POR_NIVEL, 1)
    inicio_lote = time.perf_counter()
    latencias = []
    sucessos = 0
    with ThreadPoolExecutor(max_workers=nivel_concorrencia) as executor:
        tarefas = [executor.submit(requisitar) for _ in range(total)]
        for tarefa in as_completed(tarefas):
            status, latencia = tarefa.result()
            latencias.append(latencia)
            if 200 <= status < 400:
                sucessos += 1
    duracao_lote = max(time.perf_counter() - inicio_lote, 0.001)
    latencias.sort()

    def percentil(p):
        indice = min(int((len(latencias) - 1) * p), len(latencias) - 1)
        return round(latencias[indice], 2) if latencias else 0

    erros = total - sucessos
    media = round(sum(latencias) / len(latencias), 2) if latencias else 0
    return [
        nivel_concorrencia, total, sucessos, erros,
        round(total / duracao_lote, 2), media, percentil(.95),
        percentil(.99), round(max(latencias), 2) if latencias else 0
    ]


# ============================================================
# 9. FUNÇÕES DE CAPTURA
# ============================================================
def interfaces_de_rede():
    # Retorna as placas físicas ativas e a velocidade de cada uma (Mbps)
    estados = psutil.net_if_stats()
    placas = {}
    for nome in estados:
        if nome.startswith(PREFIXOS_REDE_IGNORADOS):
            continue
        if not estados[nome].isup:
            continue
        placas[nome] = estados[nome].speed
    return placas


def ler_contadores_rede(placas):
    contadores = psutil.net_io_counters(pernic=True)
    leitura = {}
    for nome in placas:
        if nome in contadores:
            leitura[nome] = (contadores[nome].bytes_recv, contadores[nome].bytes_sent)
    return leitura


def calcular_uso_rede(placas, antes, depois, segundos):
    # Uso da rede = maior direção (download ou upload) / capacidade da placa.
    # Se a velocidade vier 0, usa CAPACIDADE_REDE_MBPS como reserva.
    capacidade_total = 0.0
    mbps_recebido = 0.0
    mbps_enviado = 0.0
    usou_reserva = False
    for nome in placas:
        if nome not in antes or nome not in depois:
            continue
        velocidade = placas[nome]
        if velocidade <= 0:
            velocidade = CAPACIDADE_REDE_MBPS
            usou_reserva = True
        capacidade_total += velocidade
        mbps_recebido += (depois[nome][0] - antes[nome][0]) * 8 / segundos / 1_000_000
        mbps_enviado += (depois[nome][1] - antes[nome][1]) * 8 / segundos / 1_000_000

    if capacidade_total <= 0:
        return None, None, None, usou_reserva

    maior_direcao = max(mbps_recebido, mbps_enviado)
    uso = min(100.0, maior_direcao / capacidade_total * 100)
    return round(uso, 2), round(maior_direcao, 3), capacidade_total, usou_reserva


def ler_processos():
    # Tempo de CPU acumulado + % de RAM + bytes de I/O de disco de cada processo
    processos = {}
    completo = True
    for proc in psutil.process_iter():
        try:
            with proc.oneshot():
                chave = (proc.pid, proc.create_time())
                tempos = proc.cpu_times()
                try:
                    io_proc = proc.io_counters()
                    bytes_io = io_proc.read_bytes + io_proc.write_bytes
                except (AttributeError, NotImplementedError, psutil.AccessDenied):
                    bytes_io = None
                processos[chave] = (proc.name(), tempos.user + tempos.system, proc.memory_percent(), bytes_io)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            completo = False
    return processos, completo


def ler_conexoes():
    # Quantidade de conexões de rede abertas por PID
    contagem = {}
    try:
        for conexao in psutil.net_connections(kind="inet"):
            if conexao.pid:
                contagem[conexao.pid] = contagem.get(conexao.pid, 0) + 1
    except (psutil.Error, PermissionError, OSError):
        pass
    return contagem


def calcular_processos(antes, depois, segundos, conexoes):
    # CPU do processo = tempo de CPU usado no intervalo / tempo / núcleos
    nucleos = psutil.cpu_count() or 1
    agrupados = {}
    for chave in depois:
        if chave not in antes:
            continue  # processo nasceu no meio do intervalo
        nome, tempo_cpu, ram, bytes_depois = depois[chave]
        bytes_antes = antes[chave][3]
        if nome not in agrupados:
            agrupados[nome] = {"nome": nome, "cpu": 0.0, "ram": 0.0, "disco_mb_s": 0.0, "conexoes": 0}
        uso_cpu = max(0.0, tempo_cpu - antes[chave][1]) / segundos / nucleos * 100
        agrupados[nome]["cpu"] += uso_cpu
        agrupados[nome]["ram"] += ram
        if bytes_depois is not None and bytes_antes is not None:
            agrupados[nome]["disco_mb_s"] += max(0, bytes_depois - bytes_antes) / 1_048_576 / segundos
        agrupados[nome]["conexoes"] += conexoes.get(chave[0], 0)

    lista = []
    for nome in sorted(agrupados):
        processo = agrupados[nome]
        processo["cpu"] = round(min(processo["cpu"], 100.0), 4)
        processo["ram"] = round(min(processo["ram"], 100.0), 4)
        processo["disco_mb_s"] = round(processo["disco_mb_s"], 4)
        lista.append(processo)
    return lista


def contexto_tempo(momento_utc):
    # Dia da semana (0 = segunda, como o heatmap da dashboard), hora local e expediente
    local = momento_utc.astimezone(FUSO)
    dia = local.weekday()
    dentro = dia < 5 and EXPEDIENTE_INICIO <= local.hour < EXPEDIENTE_FIM
    return {
        "inicio_local": local.isoformat(timespec="seconds"),
        "dia_semana": dia,
        "hora": local.hour,
        "dentro_expediente": int(dentro),
    }


def classificar(valor, atencao, critico):
    if valor is None or atencao is None or critico is None:
        return ""
    if valor >= critico:
        return "CRITICO"
    if valor >= atencao:
        return "ATENCAO"
    return "NORMAL"


def medir_latencia_ms():
    # Latência de rede = mediana de 3 conexões TCP até LATENCIA_HOST:LATENCIA_PORTA
    if not LATENCIA_ATIVA:
        return None
    tempos = []
    for _ in range(3):
        inicio = time.perf_counter()
        try:
            with socket.create_connection((LATENCIA_HOST, LATENCIA_PORTA), timeout=LATENCIA_TIMEOUT):
                tempos.append((time.perf_counter() - inicio) * 1000)
        except OSError:
            pass
    if not tempos:
        return None
    return round(sorted(tempos)[len(tempos) // 2], 2)


def processo_lider(lista, campo):
    candidatos = [
        p for p in lista
        if p["nome"] not in PROCESSOS_IGNORADOS_LIDER and p[campo] > 0
    ]
    if not candidatos:
        return ""
    return max(candidatos, key=lambda p: p[campo])["nome"]


def selecionar_processos(lista, amostra):
    # Só os maiores por CPU e RAM, mais os líderes de disco e rede (evita gravar centenas de linhas)
    candidatos = [p for p in lista if p["nome"] not in PROCESSOS_IGNORADOS_LIDER]
    nomes = set()
    for campo in ("cpu", "ram"):
        for p in sorted(candidatos, key=lambda p: p[campo], reverse=True)[:TOP_PROCESSOS]:
            nomes.add(p["nome"])
    for chave in ("processo_lider_disco", "processo_lider_rede"):
        if amostra.get(chave):
            nomes.add(amostra[chave])
    return [p for p in candidatos if p["nome"] in nomes]


def capturar_amostra():
    # Uma amostra = um intervalo de INTERVALO_SEGUNDOS segundos
    placas = interfaces_de_rede() if "rede" in componentes_dashboard else {}
    rede_antes = ler_contadores_rede(placas)
    processos_antes, completo_antes = ler_processos()
    psutil.cpu_percent(interval=None)  # prepara o contador de CPU

    inicio = datetime.now(timezone.utc)
    relogio = time.monotonic()

    # A latência é medida dentro do intervalo, sem alongá-lo
    latencia = medir_latencia_ms()
    restante = INTERVALO_SEGUNDOS - (time.monotonic() - relogio)
    if restante > 0:
        time.sleep(restante)

    cpu = psutil.cpu_percent(interval=None)
    fim = datetime.now(timezone.utc)
    segundos = time.monotonic() - relogio
    rede_depois = ler_contadores_rede(placas)
    processos_depois, completo_depois = ler_processos()
    conexoes = ler_conexoes()

    amostra = {
        "versao": 2,
        "empresa_id": str(equipamento_atual["fkEmpresa"]),
        "servidor_id": str(equipamento_atual["idEquipamento"]),
        "id_mac": str(id_mac),
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "cpu": None,
        "ram": None,
        "disco": None,
        "rede": None,
    }

    # Só preenche os componentes ativos no cadastro; os demais ficam null
    if "cpu" in componentes_dashboard:
        amostra["cpu"] = cpu
    if "ram" in componentes_dashboard:
        amostra["ram"] = psutil.virtual_memory().percent
    if "disco" in componentes_dashboard:
        amostra["disco"] = psutil.disk_usage(CAMINHO_DISCO).percent
    if "rede" in componentes_dashboard:
        uso, mbps, capacidade, reserva = calcular_uso_rede(placas, rede_antes, rede_depois, segundos)
        amostra["rede"] = uso
        amostra["rede_mbps"] = mbps
        amostra["rede_capacidade_mbps"] = capacidade
        amostra["rede_capacidade_reserva"] = reserva

    amostra["processos"] = calcular_processos(processos_antes, processos_depois, segundos, conexoes)
    amostra["processos_completos"] = completo_antes and completo_depois

    # 1) Quando: hora local, dia da semana e expediente (heatmap, sazonalidade)
    amostra.update(contexto_tempo(inicio))

    # 2) Disponibilidade: um reinício muda o boot_time
    boot = psutil.boot_time()
    amostra["boot_time"] = datetime.fromtimestamp(boot, timezone.utc).isoformat(timespec="seconds")
    amostra["uptime_s"] = int(time.time() - boot)

    # 3) Gravidade: limites + status por componente (sobre o valor gravado no CSV)
    status_geral = "NORMAL"
    em_alerta = []
    for campo in campos_status:
        atencao, critico = limites_alerta.get(campo, LIMITE_FALLBACK)
        status = classificar(amostra[campo], atencao, critico)
        amostra[f"{campo}_limite_atencao"] = atencao
        amostra[f"{campo}_limite_critico"] = critico
        amostra[f"{campo}_status"] = status
        if status in ("ATENCAO", "CRITICO"):
            em_alerta.append(f"{campo}:{status}")
            if ORDEM_STATUS[status] > ORDEM_STATUS[status_geral]:
                status_geral = status

    # 4) Latência de rede (ms); sem resposta conta como crítico
    amostra["latencia_rede_ms"] = latencia
    amostra["latencia_limite_atencao"] = LATENCIA_LIMITE_ATENCAO if LATENCIA_ATIVA else None
    amostra["latencia_limite_critico"] = LATENCIA_LIMITE_CRITICO if LATENCIA_ATIVA else None
    if not LATENCIA_ATIVA:
        amostra["latencia_status"] = ""
    elif latencia is None:
        amostra["latencia_status"] = "SEM_RESPOSTA"
        if LATENCIA_SEM_RESPOSTA_CRITICA:
            em_alerta.append("latencia:SEM_RESPOSTA")
            status_geral = "CRITICO"
    else:
        amostra["latencia_status"] = classificar(latencia, LATENCIA_LIMITE_ATENCAO, LATENCIA_LIMITE_CRITICO)
        if amostra["latencia_status"] in ("ATENCAO", "CRITICO"):
            em_alerta.append(f"latencia:{amostra['latencia_status']}")
            if ORDEM_STATUS[amostra["latencia_status"]] > ORDEM_STATUS[status_geral]:
                status_geral = amostra["latencia_status"]
    amostra["status_geral"] = status_geral
    amostra["componentes_em_alerta"] = "|".join(em_alerta)

    # 5) Processo líder de cada recurso
    lista_processos = amostra["processos"]
    amostra["processo_lider_cpu"] = processo_lider(lista_processos, "cpu")
    amostra["processo_lider_ram"] = processo_lider(lista_processos, "ram")
    amostra["processo_lider_disco"] = processo_lider(lista_processos, "disco_mb_s")
    amostra["processo_lider_rede"] = processo_lider(lista_processos, "conexoes")
    return amostra


# ============================================================
# 10. ENVIO EM LOTES (LOCAL + S3 BRONZE)
# ============================================================
linhas_hardware = []
linhas_processos = []
arquivos_gerados = []


def gerar_csv(colunas, linhas):
    buffer = io.StringIO()
    escritor = csv.writer(buffer, delimiter=';')
    escritor.writerow(colunas)
    escritor.writerows(linhas)
    return buffer.getvalue()


def atualizar_heartbeat():
    # Marca a última vez que o servidor enviou dados (apoio à inatividade real)
    try:
        conexao = conectar_banco()
        with conexao.cursor() as cur:
            cur.execute("UPDATE equipamento SET ultimaAtualizacao = NOW() WHERE idEquipamento = %s;",
                        (equipamento_atual["idEquipamento"],))
        conexao.commit()
        conexao.close()
    except Exception:
        pass


def enviar_lote():
    if not linhas_hardware:
        return
    timestamp = datetime.now(FUSO).strftime("%Y-%m-%d_%H-%M-%S")
    base = f"{nome_empresa}/{equipamento_atual['idEquipamento']}"
    arquivos = [(f"bronze/hardware/{base}/captura_{timestamp}.csv", colunas_captura, linhas_hardware)]
    if linhas_processos:
        arquivos.append((f"bronze/processo/{base}/captura_processos_{timestamp}.csv", colunas_processos, linhas_processos))

    for chave, colunas, linhas in arquivos:
        conteudo = gerar_csv(colunas, linhas)
        caminho = PASTA_LOCAL / chave
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(conteudo, encoding="utf-8")
        destino = "S3"
        try:
            s3.put_object(Bucket=BUCKET, Key=chave, Body=conteudo.encode("utf-8"), ContentType="text/csv")
        except Exception as erro:
            destino = "somente local"
            console.print(f"[yellow]Não foi possível enviar ao S3 ({type(erro).__name__}); o CSV ficou salvo localmente.[/yellow]")
        arquivos_gerados.append((chave, destino))
    atualizar_heartbeat()
    linhas_hardware.clear()
    linhas_processos.clear()


# ============================================================
# 11. EXECUÇÃO DA CAPTURA
# ============================================================
if QUANTIDADE_AMOSTRAS:
    colunas_progresso = [
        TextColumn(f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"),
        BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
        TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
        TimeElapsedColumn(),
    ]
else:
    colunas_progresso = [
        TextColumn(f"[bold {AZUL_CLARO}]Captura contínua (Ctrl+C para encerrar)[/bold {AZUL_CLARO}]"),
        TextColumn("[bold white]{task.completed} amostras[/bold white]"),
        TimeElapsedColumn(),
    ]

feitas = 0
with Progress(*colunas_progresso, console=console) as progress:
    task = progress.add_task("captura", total=QUANTIDADE_AMOSTRAS or None)
    try:
        while QUANTIDADE_AMOSTRAS == 0 or feitas < QUANTIDADE_AMOSTRAS:
            nivel_concorrencia = NIVEIS_CONCORRENCIA[feitas % len(NIVEIS_CONCORRENCIA)]
            metricas_requisicoes = capturar_requisicoes_http(nivel_concorrencia)

            amostra_dashboard = capturar_amostra()
            data_hora = datetime.now(FUSO).strftime("%Y-%m-%d %H:%M:%S")

            # Valores dos componentes: os mesmos usados no status
            valores_capturados = [
                amostra_dashboard.get(CAMPOS_COMPONENTES.get(c['tipo'].upper())) for c in componentes_csv
            ]
            valores_dashboard = [amostra_dashboard["inicio"], amostra_dashboard["fim"]]
            valores_dashboard.extend(amostra_dashboard[campo] for campo in campos_metricas_dashboard)
            valores_dashboard.extend(amostra_dashboard.get(campo) for campo in campos_rede_detalhados)
            valores_dashboard.extend([
                str(equipamento_atual["fkEmpresa"]),
                str(equipamento_atual["idEquipamento"]),
                str(id_mac),
            ])
            valores_dashboard.extend(amostra_dashboard.get(campo, "") for campo in colunas_padroes)

            linhas_hardware.append(
                [equipamento_atual['idEquipamento'], data_hora] +
                valores_capturados + metricas_requisicoes + valores_dashboard
            )

            if enviar_processos:
                for processo in selecionar_processos(amostra_dashboard["processos"], amostra_dashboard):
                    linhas_processos.append([
                        equipamento_atual["idEquipamento"], data_hora, "",
                        processo["nome"], processo["cpu"], processo["ram"],
                        "", "", "",
                        processo["disco_mb_s"], processo["conexoes"], amostra_dashboard["inicio"],
                    ])

            feitas += 1
            progress.advance(task)
            if len(linhas_hardware) >= AMOSTRAS_POR_ARQUIVO:
                enviar_lote()
            time.sleep(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Captura interrompida. Salvando as amostras pendentes...[/yellow]")
    finally:
        enviar_lote()


# ============================================================
# 12. RESUMO
# ============================================================
lista_arquivos = "\n".join(f"[dim white]{destino}:[/dim white] [bold {AZUL_CLARO}]{chave}[/bold {AZUL_CLARO}]"
                           for chave, destino in arquivos_gerados) or "nenhum arquivo gerado"
console.print()
console.print(
    Panel(
        f"[bold white]Captura concluída: {feitas} amostras.[/bold white]\n\n"
        f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n{lista_arquivos}",
        title=f"[bold {AZUL_CLARO}]CAPTURA FINALIZADA[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 2)
    )
)
console.print(f"[dim white]CSVs salvos localmente em: {PASTA_LOCAL}[/dim white]")
console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))