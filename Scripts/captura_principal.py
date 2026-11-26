import psutil
import csv
from datetime import datetime, timedelta, timezone
import time
import os
import uuid
import random
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
# Carrega as variáveis do .env para o ambiente
load_dotenv()

# Campos adicionais para a dashboard histórica, dentro do CSV consolidado.
PASTA_SCRIPT = Path(__file__).resolve().parent
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION") or "us-east-1"
QUANTIDADE_AMOSTRAS = int(os.getenv("QUANTIDADE_AMOSTRAS") or "15")
QUANTIDADE_DADOS_SIMULADOS = int(os.getenv("QUANTIDADE_DADOS_SIMULADOS") or "100")
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS") or "5")
CAMINHO_DISCO = os.getenv("CAMINHO_DISCO") or os.path.abspath(os.sep)
CAPACIDADE_REDE_MBPS = float(os.getenv("CAPACIDADE_REDE_MBPS") or "100")
HORARIOS_PICO_SERVIDOR = os.getenv("HORARIOS_PICO_SERVIDOR") or "09-12,14-17"
BUCKET_BRONZE = (os.getenv("S3_BUCKET_BRONZE") or "").strip()
PREFIXO_BRONZE = (os.getenv("S3_PREFIX_BRONZE") or "bronze/hardware/").strip("/")
PASTA_LOCAL = Path(os.getenv("DIRETORIO_DADOS") or PASTA_SCRIPT / "dados")
CAMPOS_COMPONENTES = {"CPU": "cpu", "RAM": "ram", "ARMAZENAMENTO": "disco", "DISCO": "disco", "REDE": "rede"}
PREFIXOS_REDE_IGNORADOS = ("lo", "docker", "veth", "br-", "virbr", "vmnet", "vboxnet")
if QUANTIDADE_AMOSTRAS < 1 or INTERVALO_SEGUNDOS <= 0 or INTERVALO_SEGUNDOS > 290:
    raise ValueError("Use QUANTIDADE_AMOSTRAS >= 1 e INTERVALO_SEGUNDOS entre 0 e 290.")
if QUANTIDADE_DADOS_SIMULADOS < 0:
    raise ValueError("Use QUANTIDADE_DADOS_SIMULADOS >= 0.")


def carregar_horarios_pico(valor):
    """Converte horários como '09-12,14-17' em intervalos de hora."""
    horarios = []
    for intervalo in valor.split(","):
        try:
            inicio, fim = (int(parte.strip()) for parte in intervalo.split("-", 1))
        except (TypeError, ValueError):
            raise ValueError(
                "HORARIOS_PICO_SERVIDOR deve usar intervalos como '09-12,14-17'."
            ) from None
        if not (0 <= inicio < fim <= 24):
            raise ValueError(
                "Cada intervalo em HORARIOS_PICO_SERVIDOR deve ter início menor "
                "que o fim, entre 0 e 24."
            )
        horarios.append((inicio, fim))
    return horarios


JANELAS_PICO_SERVIDOR = carregar_horarios_pico(HORARIOS_PICO_SERVIDOR)

# Configurações adicionais utilizadas na captura individual do Gustavo.
import socket
from zoneinfo import ZoneInfo

FUSO = ZoneInfo(os.getenv("FUSO_HORARIO") or "America/Sao_Paulo")
EXPEDIENTE_INICIO = int(os.getenv("EXPEDIENTE_INICIO") or "8")
EXPEDIENTE_FIM = int(os.getenv("EXPEDIENTE_FIM") or "18")
LATENCIA_ATIVA = os.getenv("LATENCIA_ATIVA", "true").lower() not in ("0", "false", "nao", "não", "off")
LATENCIA_HOST = os.getenv("LATENCIA_HOST") or "8.8.8.8"
LATENCIA_PORTA = int(os.getenv("LATENCIA_PORTA") or "53")
LATENCIA_TIMEOUT = float(os.getenv("LATENCIA_TIMEOUT") or "1")
LATENCIA_LIMITE_ATENCAO = float(os.getenv("LATENCIA_LIMITE_ATENCAO") or "200")
LATENCIA_LIMITE_CRITICO = float(os.getenv("LATENCIA_LIMITE_CRITICO") or "300")
LATENCIA_SEM_RESPOSTA_CRITICA = os.getenv("LATENCIA_SEM_RESPOSTA_CRITICA", "true").lower() not in ("0", "false", "nao", "não", "off")
PROCESSOS_IGNORADOS_LIDER = {"System Idle Process", "Idle", "System", "Registry", "Memory Compression"}
ORDEM_STATUS = {"": -1, "NORMAL": 0, "ATENCAO": 1, "CRITICO": 2}

# Variáveis AWS
AWS_ACESS_KEY_ID = os.getenv("AWS_ACESS_KEY_ID") or None # Em caso de variável vazia, retorna None
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None

# Variáveis BD
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = int(os.getenv("PORT"))

# Sonda HTTP habilitada por padrão. httpbin.org/post simula temporariamente o
# endpoint do cliente; 
REQUEST_PROBE_URL = os.getenv(
    "INFRAWATCH_REQUEST_PROBE_URL", "https://httpbin.org/post"
).strip()
REQUEST_PROBE_METHOD = os.getenv("INFRAWATCH_REQUEST_PROBE_METHOD", "POST").upper()
REQUEST_PROBE_TIMEOUT = float(os.getenv("INFRAWATCH_REQUEST_PROBE_TIMEOUT", "3"))
REQUEST_PROBE_ENABLED = os.getenv("INFRAWATCH_REQUEST_PROBE_ENABLED", "true").lower() not in ("0", "false", "nao", "não", "off")
REQUESTS_POR_NIVEL = 20
NIVEIS_CONCORRENCIA = [1, 2, 4, 8, 16] * 3

if REQUEST_PROBE_ENABLED and not REQUEST_PROBE_URL:
    raise ValueError("A sonda HTTP está habilitada. Informe INFRAWATCH_REQUEST_PROBE_URL.")
if REQUEST_PROBE_URL and not REQUEST_PROBE_URL.lower().startswith(("http://", "https://")):
    raise ValueError("INFRAWATCH_REQUEST_PROBE_URL precisa começar com http:// ou https://.")
if REQUEST_PROBE_METHOD not in ("GET", "POST"):
    raise ValueError("INFRAWATCH_REQUEST_PROBE_METHOD aceita somente GET ou POST.")



# ============================================================
# 1. CONEXÃO COM O S3
# ============================================================
BUCKET = "infrawatch-server-s3"
if not BUCKET_BRONZE:
    BUCKET_BRONZE = BUCKET

s3 = boto3.client(
"s3",
aws_access_key_id=AWS_ACESS_KEY_ID,
aws_secret_access_key=AWS_SECRET_KEY,
aws_session_token=AWS_SESSION_TOKEN
)



# Captura o idMac único da máquina atual
id_mac = uuid.getnode()

# ============================================================
# 2. CONEXÃO COM O BANCO DE DADOS NOVO (InfraWatch)
# ============================================================
console.print(
    Panel("[bold white]Conectando ao banco de dados InfraWatch...[/bold white]", 
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)

bd = mysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=PORT,
    cursorclass=mysql.cursors.DictCursor
)

cursor = bd.cursor()

# 1. Busca Equipamentos no schema novo e idMac
cursor.execute("SELECT idEquipamento, nome, idMac, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

# 2. Busca Componentes no schema novo 
cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente;")
componentes = cursor.fetchall()

# 4. Busca Usuários
cursor.execute("SELECT nome, email, senha, fkEmpresa FROM usuario;")
funcionarios = cursor.fetchall()

# 5. Busca Empresas
cursor.execute("SELECT idEmpresa, nome FROM empresa;")
empresas = cursor.fetchall()



# ============================================================
# 3. VALIDAÇÃO DO USUARIO
# ============================================================

limpar_tela()
mostrar_banner()

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
ipt_senha = Prompt.ask("[bold white]Senha[/bold white]", password=True) # Para esconder a senha)

usuario_logado = ""

# Validação do Usuário na Tabela 'usuario' e busca do nome da Empresa
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
    os._exit(0)
    cursor.close()
    bd.close()
    
    

    
fk_empresa = usuario_logado["fkEmpresa"]
nome_empresa = usuario_logado["nomeEmpresa"]
nome_usuario = usuario_logado["nome"]



# ============================================================
# 4. VALIDAÇÃO OU CADASTRO DO SERVIDOR
# ============================================================

cadastrar = True

# Valida se o servidor existe
for i in range(len(equipamentos)) :
    if (equipamentos[i]['idMac'] == str(id_mac)) :
        cadastrar = False

        # Define o equipamento atual com as informações encontradas
        equipamento_atual = {
            "idEquipamento": equipamentos[i]['idEquipamento'],
            "nome": equipamentos[i]['nome'],
            "idMac": id_mac,
            "fkEmpresa": equipamentos[i]['fkEmpresa']
        }

# Se não existe, cadastra
if (cadastrar) :
    
    # Cadastrar novo servidor
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
    
    # Define o equipamento atual com as informações cadastradas
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

    # Cadastro dos componentes default (CPU, RAM, PROCESSOS)
    sql = """
        INSERT INTO parametro_alerta 
        (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente) 
        VALUES ('USE_CPU', 60.0, 90.0, '%%', 1, %s, 1)
        """
    cursor.execute(sql, (equipamento_atual["idEquipamento"], ))
    
    bd.commit() 
    console.print(f"[bold green]Métrica USO_CPU salva com sucesso! [/bold green]\n") 
    
    
    sql = """
        INSERT INTO parametro_alerta 
        (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente) 
        VALUES ('USE_RAM', 60.0, 90.0, '%%', 1, %s, 2)
        """
    cursor.execute(sql, (equipamento_atual["idEquipamento"], )) 
    bd.commit() 
    console.print(f"[bold green]Métrica USO_RAM salva com sucesso! [/bold green]\n") 
    
    
    sql = """
        INSERT INTO parametro_alerta 
        (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente) 
        VALUES ('USE_DISCO', 60.0, 90.0, '%%', 1, %s, 6)
        """
    cursor.execute(sql, (equipamento_atual["idEquipamento"], ))
    bd.commit() 
    console.print(f"[bold green]Métrica PROCESSOS_ATIVOS salva com sucesso! [/bold green]\n") 



# ============================================================
# 5. BUSCA MAPEAMENTO DE EQUIPAMENTO E COMPONENTES
# ============================================================

# É necessário fazer essa query aqui para que a tabela venha atualizada
cursor.execute("SELECT DISTINCT fkComponente, fkEquipamento FROM parametro_alerta WHERE ativo = 1;")
equip_comp = cursor.fetchall()


# Busca os componentes cadastrados do equipamento atual
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



# Se não existir, exibe que não há nenhum componente ativo
if not componentes_selecionados_objs: 
    console.print("[bold yellow]Aviso: Nenhum componente ativo configurado no banco. Carregando lista geral...[/bold yellow]") 
    cursor.execute("SELECT idComponente, nome, tipo FROM componente;") 
    componentes_selecionados_objs = cursor.fetchall() 
    
if not componentes_selecionados_objs: 
    console.print("[bold yellow]Nenhum componente selecionado para monitorar. Encerrando.[/bold yellow]") 
    cursor.close() 
    bd.close() 
    sys.exit(0)

# Limites adicionais, consultados antes de fechar a conexão existente.
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
for limite in cursor.fetchall():
    campo = CAMPOS_COMPONENTES.get(str(limite["tipo"]).upper())
    if campo:
        limites_alerta[campo] = (limite["limite_atencao"], limite["limite_critico"])

cursor.close() 
bd.close()


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

# Pergunta se o usuário deseja prosseguir
console.print()
continuar = Confirm.ask("[bold white]Deseja continuar?[/bold white]", default=True)

# Se não, o script é fechado
if not continuar:
    console.print(
        Panel("[bold white]Fechando script...[/bold white]", border_style=AZUL_CLARO, style=f"on {PRETO}")
    )
    os._exit(0)


# ============================================================
# 6. IDENTIFICA COMPONENTES DO EQUIPAMENTO ATUAL
# ============================================================
capturar_comp = []
for ec in equip_comp:
    if ec['fkEquipamento'] == equipamento_atual['idEquipamento']:
        capturar_comp.append(ec['fkComponente'])


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
# 7. MONTAGEM DO CSV EM MEMÓRIA
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel("[bold white]Preparando captura dos dados...[/bold white]",
          title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)
console.print()

# Monta o cabeçalho do CSV sem repetir a mesma métrica com nomes equivalentes.
componentes_csv = []
campos_componentes_csv = set()
for comp_id in capturar_comp:
    if comp_id == 6:
        continue
    for componente in componentes:
        if comp_id != componente['idComponente']:
            continue
        tipo = componente['tipo']
        campo = CAMPOS_COMPONENTES.get(tipo.upper(), tipo.upper())
        if campo not in campos_componentes_csv:
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
]
colunas_captura = (
    titulo_executados + colunas_sonda_http + ['INICIO_AMOSTRA', 'FIM_AMOSTRA']
    + campos_metricas_dashboard + campos_rede_detalhados
    + ['empresa_id', 'servidor_id', 'id_mac']
)

# As novas colunas entram somente depois de todas as colunas originais.
campos_status = [c for c in ("cpu", "ram", "disco", "rede") if c in componentes_dashboard]
colunas_adicionais = (
    ["LATENCIA_MAX_MS", "versao", "processos_completos",
     "inicio_local", "dia_semana", "hora", "dentro_expediente",
     "boot_time", "uptime_s", "status_geral", "componentes_em_alerta"]
    + [f"{c}_{s}" for c in campos_status
       for s in ("limite_atencao", "limite_critico", "status")]
    + ["latencia_rede_ms", "latencia_limite_atencao",
       "latencia_limite_critico", "latencia_status",
       "processo_lider_cpu", "processo_lider_ram",
       "processo_lider_disco", "processo_lider_rede", "origem_hardware"]
)
colunas_captura.extend(colunas_adicionais)

# EM MEMÓRIA
csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer, delimiter=';')
writer.writerow(colunas_captura)

# Os detalhes dos processos permanecem em um arquivo CSV separado do hardware.
processos_buffer = io.StringIO()
processos_writer = csv.writer(processos_buffer, delimiter=';')
processos_writer.writerow([
    'ID_EQUIPAMENTO', 'DATA_HORA', 'PID', 'PROCESSO', 'CPU_AGREGADA',
    'RAM_AGREGADA', 'RAM_PROCESSO', 'TAMANHO_MB', 'CAMINHO',
    'DISCO_MB_S', 'CONEXOES', 'INICIO_AMOSTRA',
])
processos_tem_dados = False

# GERAR ARQUIVO .CSV TESTE
# with open('./dados.csv', 'w', newline='', encoding="utf-8") as csvfile:
#     writer = csv.writer(csvfile, delimiter=';')
#     writer.writerow(titulo_executados)


# ============================================================
# 8. FUNÇÃO PARA CAPTURAR PROCESSOS E GERAR .CSV
# ============================================================
enviar_processos = False

def capturar_processos():
    infos_processos = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            p = psutil.Process(proc.info['pid'])
            proc_ram = round(p.memory_percent(), 2)
            proc_path = p.exe() if p.exe() else 'N/A'
            proc_tam_mb = 0.0
            if proc_path != 'N/A' and os.path.exists(proc_path):
                proc_tam_bytes = os.path.getsize(proc_path)
                proc_tam_mb = round(proc_tam_bytes / (1024 * 1024), 2)
            

            infos_processos.append([
                proc.info['pid'],
                proc.info['name'],
                proc_ram,
                proc_tam_mb,
                proc_path
            ])
        except Exception:
            pass

    # EM MEMÓRIA
    csv_processos = io.StringIO()
    writer = csv.writer(csv_processos, delimiter=';')
    writer.writerow(['PID', 'PROCESSO', 'RAM', 'TAMANHO', 'CAMINHO'])
    writer.writerows(infos_processos)


    # GERAR ARQUIVO .CSV TESTE  
    # with open('./dados_processos.csv', 'w', newline='', encoding="utf-8") as csvfile:
    #     writer = csv.writer(csvfile, delimiter=';')
    #     writer.writerow(['PID', 'PROCESSO', 'RAM', 'TAMANHO', 'CAMINHO'])
    
    # with open('./dados_processos.csv', 'a', newline='', encoding="utf-8") as csvfile:
    #     writer = csv.writer(csvfile, delimiter=';')
    #     writer.writerows(infos_processos)
    
    return csv_processos # Para testar troque por 'csvfile', fora de teste é 'csv_processos'


def capturar_requisicoes_http(nivel_concorrencia):
    """Executa um lote HTTP controlado contra o endpoint configurado."""
    colunas = [
        nivel_concorrencia if REQUEST_PROBE_ENABLED else '', '', '', '', '', '', '', '', ''
    ]
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
# 9. FUNÇÕES DE CAPTURA PARA O CSV COMPLETO
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
    # Tempo de CPU acumulado + % de RAM de cada processo
    processos = {}
    completo = True
    for proc in psutil.process_iter():
        try:
            with proc.oneshot():
                chave = (proc.pid, proc.create_time())
                tempos = proc.cpu_times()
                processos[chave] = (proc.name(), tempos.user + tempos.system, proc.memory_percent())
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            completo = False
    return processos, completo


def calcular_processos(antes, depois, segundos):
    # CPU do processo = tempo de CPU usado no intervalo / tempo / núcleos
    nucleos = psutil.cpu_count() or 1
    agrupados = {}
    for chave in depois:
        if chave not in antes:
            continue  # processo nasceu no meio do intervalo
        nome, tempo_cpu, ram = depois[chave]
        if nome not in agrupados:
            agrupados[nome] = {"nome": nome, "cpu": 0.0, "ram": 0.0}
        uso_cpu = max(0.0, tempo_cpu - antes[chave][1]) / segundos / nucleos * 100
        agrupados[nome]["cpu"] += uso_cpu
        agrupados[nome]["ram"] += ram

    lista = []
    for nome in sorted(agrupados):
        processo = agrupados[nome]
        processo["cpu"] = round(min(processo["cpu"], 100.0), 4)
        processo["ram"] = round(min(processo["ram"], 100.0), 4)
        lista.append(processo)
    return lista


# Funções adicionais sem substituir as funções de captura originais.
def ler_io_processos():
    leituras = {}
    for proc in psutil.process_iter():
        try:
            chave = (proc.pid, proc.create_time())
            contadores = proc.io_counters()
            leituras[chave] = contadores.read_bytes + contadores.write_bytes
        except (psutil.Error, OSError, AttributeError, NotImplementedError):
            pass
    return leituras


def complementar_processos(lista, antes, depois, segundos):
    por_nome = {p["nome"]: p for p in lista}
    for processo in lista:
        processo["disco_mb_s"] = None
        processo["conexoes"] = None
    for chave, bytes_depois in depois.items():
        if chave not in antes:
            continue
        try:
            proc = psutil.Process(chave[0])
            if proc.create_time() != chave[1]:
                continue
            processo = por_nome.get(proc.name())
            if processo is not None:
                taxa = max(0, bytes_depois - antes[chave]) / 1_048_576 / segundos
                processo["disco_mb_s"] = (processo["disco_mb_s"] or 0) + taxa
        except (psutil.Error, OSError):
            pass
    if "rede" in componentes_dashboard:
        try:
            conexoes = psutil.net_connections(kind="inet")
        except (psutil.Error, OSError, NotImplementedError):
            conexoes = None
        if conexoes is not None:
            for processo in lista:
                processo["conexoes"] = 0
            nomes_pid = {}
            for conexao in conexoes:
                if not conexao.pid:
                    continue
                try:
                    if conexao.pid not in nomes_pid:
                        nomes_pid[conexao.pid] = psutil.Process(conexao.pid).name()
                    processo = por_nome.get(nomes_pid[conexao.pid])
                    if processo is not None:
                        processo["conexoes"] += 1
                except (psutil.Error, OSError):
                    pass
    for processo in lista:
        if processo["disco_mb_s"] is not None:
            processo["disco_mb_s"] = round(processo["disco_mb_s"], 4)


def medir_latencia_ms():
    if not LATENCIA_ATIVA or "rede" not in componentes_dashboard:
        return None
    tempos = []
    for _ in range(3):
        inicio = time.perf_counter()
        try:
            with socket.create_connection((LATENCIA_HOST, LATENCIA_PORTA), timeout=LATENCIA_TIMEOUT):
                tempos.append((time.perf_counter() - inicio) * 1000)
        except OSError:
            pass
    return round(sorted(tempos)[len(tempos) // 2], 2) if tempos else None


def classificar_adicional(valor, atencao, critico):
    if valor is None or atencao is None or critico is None:
        return ""
    if valor >= float(critico):
        return "CRITICO"
    if valor >= float(atencao):
        return "ATENCAO"
    return "NORMAL"


def processo_lider_adicional(lista, campo):
    candidatos = [p for p in lista if p["nome"] not in PROCESSOS_IGNORADOS_LIDER
                  and p.get(campo) is not None and p[campo] > 0]
    return max(candidatos, key=lambda p: p[campo])["nome"] if candidatos else ""


def complementar_amostra(amostra):
    local = datetime.fromisoformat(amostra["inicio"]).astimezone(FUSO)
    amostra["inicio_local"] = local.isoformat(timespec="seconds")
    amostra["dia_semana"] = local.weekday()
    amostra["hora"] = local.hour
    amostra["dentro_expediente"] = int(local.weekday() < 5 and EXPEDIENTE_INICIO <= local.hour < EXPEDIENTE_FIM)
    boot = psutil.boot_time()
    amostra["boot_time"] = datetime.fromtimestamp(boot, timezone.utc).isoformat(timespec="seconds")
    amostra["uptime_s"] = int(time.time() - boot)
    status_geral = ""
    alertas = []
    for campo in campos_status:
        atencao, critico = limites_alerta.get(campo, (None, None))
        amostra[f"{campo}_limite_atencao"] = atencao
        amostra[f"{campo}_limite_critico"] = critico
        status = classificar_adicional(amostra[campo], atencao, critico)
        amostra[f"{campo}_status"] = status
        if ORDEM_STATUS[status] > ORDEM_STATUS[status_geral]:
            status_geral = status
        if status in ("ATENCAO", "CRITICO"):
            alertas.append(f"{campo}:{status}")
    ativa = LATENCIA_ATIVA and "rede" in componentes_dashboard
    latencia = medir_latencia_ms()
    amostra["latencia_rede_ms"] = latencia
    amostra["latencia_limite_atencao"] = LATENCIA_LIMITE_ATENCAO if ativa else None
    amostra["latencia_limite_critico"] = LATENCIA_LIMITE_CRITICO if ativa else None
    status = ""
    if ativa:
        if latencia is None:
            status = "SEM_RESPOSTA"
            if LATENCIA_SEM_RESPOSTA_CRITICA:
                status_geral = "CRITICO"
                alertas.append("latencia:SEM_RESPOSTA")
        else:
            status = classificar_adicional(latencia, LATENCIA_LIMITE_ATENCAO, LATENCIA_LIMITE_CRITICO)
            if ORDEM_STATUS[status] > ORDEM_STATUS[status_geral]:
                status_geral = status
            if status in ("ATENCAO", "CRITICO"):
                alertas.append(f"latencia:{status}")
    amostra["latencia_status"] = status
    amostra["status_geral"] = status_geral
    amostra["componentes_em_alerta"] = "|".join(alertas)
    for campo in ("cpu", "ram", "disco", "rede"):
        metrica = {"cpu": "cpu", "ram": "ram", "disco": "disco_mb_s", "rede": "conexoes"}[campo]
        amostra[f"processo_lider_{campo}"] = (
            processo_lider_adicional(amostra["processos"], metrica)
            if campo in componentes_dashboard else ""
        )


def nivel_estresse_horario(instante):
    """Retorna uma carga maior em horários de pico de dias úteis."""
    local = instante.astimezone(FUSO)
    if local.weekday() < 5 and any(
        inicio <= local.hour < fim for inicio, fim in JANELAS_PICO_SERVIDOR
    ):
        return random.uniform(0.72, 1.0)
    if local.weekday() < 5 and EXPEDIENTE_INICIO <= local.hour < EXPEDIENTE_FIM:
        return random.uniform(0.35, 0.62)
    return random.uniform(0.08, 0.32)


def valor_percentual_simulado(campo, valor_real, estresse):
    """Gera percentuais dentro da mesma escala das métricas capturadas."""
    padroes = {"cpu": 20.0, "ram": 35.0, "disco": 50.0, "rede": 10.0}
    base = valor_real if isinstance(valor_real, (int, float)) else padroes[campo]

    if campo == "disco":
        # A ocupação do disco representa espaço usado, não atividade instantânea.
        valor = base + random.uniform(-0.15, 0.2) + estresse * 0.1
    elif campo == "ram":
        valor = base + min(35.0, max(8.0, 90.0 - base)) * estresse
        valor += random.uniform(-3.0, 3.0)
    elif campo == "cpu":
        valor = base + max(18.0, 94.0 - base) * estresse
        valor += random.uniform(-5.0, 5.0)
    else:  # rede
        valor = base + max(25.0, 96.0 - base) * estresse
        valor += random.uniform(-6.0, 6.0)

    return round(min(100.0, max(0.0, valor)), 2)


def criar_amostra_simulada(inicio, amostra_real_recente):
    """Monta uma amostra com os componentes ativos e metadados do CSV atual."""
    local = inicio.astimezone(FUSO)
    estresse = nivel_estresse_horario(inicio)
    amostra = {
        "origem_hardware": "simulado",
        "versao": 2,
        "empresa_id": str(equipamento_atual["fkEmpresa"]),
        "servidor_id": str(equipamento_atual["idEquipamento"]),
        "id_mac": str(id_mac),
        "inicio": inicio.isoformat(),
        "fim": (inicio + timedelta(seconds=INTERVALO_SEGUNDOS)).isoformat(),
        "cpu": None,
        "ram": None,
        "disco": None,
        "rede": None,
        "rede_mbps": None,
        "rede_capacidade_mbps": None,
        "rede_capacidade_reserva": False,
        "processos": [],
        "processos_completos": False,
    }

    for campo in componentes_dashboard:
        valor_real = (amostra_real_recente or {}).get(campo)
        amostra[campo] = valor_percentual_simulado(campo, valor_real, estresse)

    if "rede" in componentes_dashboard:
        capacidade = (amostra_real_recente or {}).get("rede_capacidade_mbps")
        if not isinstance(capacidade, (int, float)) or capacidade <= 0:
            capacidade = CAPACIDADE_REDE_MBPS
        amostra["rede_capacidade_mbps"] = round(float(capacidade), 2)
        amostra["rede_mbps"] = round(
            amostra["rede"] * amostra["rede_capacidade_mbps"] / 100, 3
        )

    amostra["inicio_local"] = local.isoformat(timespec="seconds")
    amostra["dia_semana"] = local.weekday()
    amostra["hora"] = local.hour
    amostra["dentro_expediente"] = int(
        local.weekday() < 5
        and EXPEDIENTE_INICIO <= local.hour < EXPEDIENTE_FIM
    )
    boot = psutil.boot_time()
    amostra["boot_time"] = datetime.fromtimestamp(
        boot, timezone.utc
    ).isoformat(timespec="seconds")
    amostra["uptime_s"] = max(0, int(inicio.timestamp() - boot))

    status_geral = ""
    alertas = []
    for campo in campos_status:
        atencao, critico = limites_alerta.get(campo, (None, None))
        amostra[f"{campo}_limite_atencao"] = atencao
        amostra[f"{campo}_limite_critico"] = critico
        status = classificar_adicional(amostra[campo], atencao, critico)
        amostra[f"{campo}_status"] = status
        if ORDEM_STATUS[status] > ORDEM_STATUS[status_geral]:
            status_geral = status
        if status in ("ATENCAO", "CRITICO"):
            alertas.append(f"{campo}:{status}")

    latencia_ativa = LATENCIA_ATIVA and "rede" in componentes_dashboard
    latencia = (
        round(15 + estresse * 150 + random.uniform(-8, 12), 2)
        if latencia_ativa else None
    )
    amostra["latencia_rede_ms"] = latencia
    amostra["latencia_limite_atencao"] = (
        LATENCIA_LIMITE_ATENCAO if latencia_ativa else None
    )
    amostra["latencia_limite_critico"] = (
        LATENCIA_LIMITE_CRITICO if latencia_ativa else None
    )
    status_latencia = ""
    if latencia_ativa:
        status_latencia = classificar_adicional(
            latencia, LATENCIA_LIMITE_ATENCAO, LATENCIA_LIMITE_CRITICO
        )
        if ORDEM_STATUS[status_latencia] > ORDEM_STATUS[status_geral]:
            status_geral = status_latencia
        if status_latencia in ("ATENCAO", "CRITICO"):
            alertas.append(f"latencia:{status_latencia}")
    amostra["latencia_status"] = status_latencia
    amostra["status_geral"] = status_geral
    amostra["componentes_em_alerta"] = "|".join(alertas)
    for campo in ("cpu", "ram", "disco", "rede"):
        amostra[f"processo_lider_{campo}"] = ""

    return amostra


def capturar_amostra():
    # Uma amostra = um intervalo de INTERVALO_SEGUNDOS segundos
    placas = interfaces_de_rede() if "rede" in componentes_dashboard else {}
    rede_antes = ler_contadores_rede(placas)
    io_antes = ler_io_processos() if "disco" in componentes_dashboard else {}
    relogio_io = time.monotonic()
    processos_antes, completo_antes = ler_processos()
    psutil.cpu_percent(interval=None)  # prepara o contador de CPU

    inicio = datetime.now(timezone.utc)
    relogio = time.monotonic()
    time.sleep(INTERVALO_SEGUNDOS)

    cpu = psutil.cpu_percent(interval=None)
    fim = datetime.now(timezone.utc)
    segundos = time.monotonic() - relogio
    rede_depois = ler_contadores_rede(placas)
    io_depois = ler_io_processos() if "disco" in componentes_dashboard else {}
    segundos_io = max(time.monotonic() - relogio_io, 0.001)
    processos_depois, completo_depois = ler_processos()

    amostra = {
        "origem_hardware": "real",
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

    amostra["processos"] = calcular_processos(processos_antes, processos_depois, segundos)
    amostra["processos_completos"] = completo_antes and completo_depois
    complementar_processos(amostra["processos"], io_antes, io_depois, segundos_io)
    complementar_amostra(amostra)
    return amostra



# ============================================================
# 10. EXECUÇÃO DA CAPTURA CSV E TESTE HTTP (15 AMOSTRAS)
# ============================================================
ultima_amostra_real = None
with Progress(
    TextColumn(f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"),
    BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
    TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
    TimeElapsedColumn(),
    console=console
) as progress:
    task = progress.add_task("captura", total=15)
    for i in range(15):
        nivel_concorrencia = NIVEIS_CONCORRENCIA[i]
        metricas_requisicoes = capturar_requisicoes_http(nivel_concorrencia)

        # A função já retornava a latência máxima sem cabeçalho correspondente.
        # Ela passa para o final, preservando a ordem das colunas originais.
        latencia_max_ms = metricas_requisicoes.pop()

        # Capturo uma vez e reutilizo quando o código no banco estiver NULL.
        amostra_dashboard = capturar_amostra()
        ultima_amostra_real = amostra_dashboard

        valores_capturados = []

        for comp_id in capturar_comp:
            if comp_id == 6:
                enviar_processos = True

        for componente in componentes_csv:
            if componente['codigo'] is None:
                campo = CAMPOS_COMPONENTES.get(
                    componente['tipo'].upper()
                )
                val = amostra_dashboard.get(campo)
            else:
                val = eval(componente['codigo'])

            valores_capturados.append(val)

        data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        valores_dashboard = [amostra_dashboard["inicio"], amostra_dashboard["fim"]]
        valores_dashboard.extend(amostra_dashboard[campo] for campo in campos_metricas_dashboard)
        valores_dashboard.extend(amostra_dashboard.get(campo) for campo in campos_rede_detalhados)
        valores_dashboard.extend([
            str(equipamento_atual["fkEmpresa"]),
            str(equipamento_atual["idEquipamento"]),
            str(id_mac),
        ])

        amostra_dashboard["LATENCIA_MAX_MS"] = latencia_max_ms
        valores_dashboard.extend(amostra_dashboard.get(campo) for campo in colunas_adicionais)

        inventario_processos = []
        if enviar_processos:
            csv_processos = capturar_processos()
            inventario_csv = csv.reader(io.StringIO(csv_processos.getvalue()), delimiter=';')
            next(inventario_csv, None)  # remove o cabeçalho do inventário
            inventario_processos = list(inventario_csv)

        metadados_por_nome = {}
        for processo in inventario_processos:
            pid, nome, ram, tamanho, caminho = processo
            metadados_por_nome.setdefault(nome, []).append((pid, ram, tamanho, caminho))

        for processo in amostra_dashboard["processos"]:
            metadados = metadados_por_nome.get(processo["nome"], [])
            pid, ram_processo, tamanho, caminho = metadados.pop(0) if metadados else ("", "", "", "")
            processos_writer.writerow([
                equipamento_atual["idEquipamento"], data_hora, pid,
                processo["nome"], processo["cpu"], processo["ram"],
                ram_processo, tamanho, caminho,
                processo.get("disco_mb_s"), processo.get("conexoes"),
                amostra_dashboard["inicio"],
            ])
            processos_tem_dados = True

        # Mantém os processos que só aparecem no inventário (PID, tamanho e caminho).
        for nome, metadados in metadados_por_nome.items():
            for pid, ram, tamanho, caminho in metadados:
                processos_writer.writerow([
                    equipamento_atual["idEquipamento"], data_hora, pid,
                    nome, "", "", ram, tamanho, caminho,
                    "", "", amostra_dashboard["inicio"],
                ])
                processos_tem_dados = True

        # EM MEMÓRIA: métricas cadastradas, sonda HTTP e hardware na linha principal.
        writer.writerow(
            [equipamento_atual['idEquipamento'], data_hora] +
            valores_capturados + metricas_requisicoes + valores_dashboard
        )

        # GERAR ARQUIVO .CSV TESTE
        # linha = [equipamento_atual['idEquipamento'], data_hora] + valores_capturados
        # with open('./dados.csv', 'a', newline='', encoding="utf-8") as csvfile:
        #     writer = csv.writer(csvfile, delimiter=';')
        #     writer.writerow(linha)
        
        time.sleep(1)
        progress.advance(task)


# ============================================================
# 11. GERAÇÃO DE MAIS 100 AMOSTRAS SINTÉTICAS
# ============================================================
if QUANTIDADE_DADOS_SIMULADOS:
    if ultima_amostra_real:
        primeiro_inicio_simulado = datetime.fromisoformat(
            ultima_amostra_real["fim"]
        ) + timedelta(seconds=INTERVALO_SEGUNDOS)
    else:
        primeiro_inicio_simulado = datetime.now(timezone.utc)

    with Progress(
        TextColumn(f"[bold {AZUL_CLARO}]Gerando amostras[/bold {AZUL_CLARO}]"),
        BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
        TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("simulacao", total=QUANTIDADE_DADOS_SIMULADOS)
        for indice in range(QUANTIDADE_DADOS_SIMULADOS):
            inicio_simulado = primeiro_inicio_simulado + timedelta(
                seconds=indice * INTERVALO_SEGUNDOS
            )

            # Mede o endpoint em cada amostra, alternando os níveis usados na
            # captura real. A última coluna retornada é a latência máxima.
            nivel_concorrencia = NIVEIS_CONCORRENCIA[
                indice % len(NIVEIS_CONCORRENCIA)
            ]
            metricas_requisicoes = capturar_requisicoes_http(nivel_concorrencia)
            latencia_max_ms = metricas_requisicoes.pop()

            amostra_dashboard = criar_amostra_simulada(
                inicio_simulado, ultima_amostra_real
            )
            data_hora = inicio_simulado.astimezone(FUSO).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            valores_capturados = []
            for componente in componentes_csv:
                campo = CAMPOS_COMPONENTES.get(componente["tipo"].upper())
                valores_capturados.append(
                    amostra_dashboard.get(campo) if campo else None
                )

            valores_dashboard = [
                amostra_dashboard["inicio"], amostra_dashboard["fim"]
            ]
            valores_dashboard.extend(
                amostra_dashboard[campo] for campo in campos_metricas_dashboard
            )
            valores_dashboard.extend(
                amostra_dashboard.get(campo) for campo in campos_rede_detalhados
            )
            valores_dashboard.extend([
                str(equipamento_atual["fkEmpresa"]),
                str(equipamento_atual["idEquipamento"]),
                str(id_mac),
            ])
            amostra_dashboard["LATENCIA_MAX_MS"] = latencia_max_ms
            valores_dashboard.extend(
                amostra_dashboard.get(campo) for campo in colunas_adicionais
            )

            writer.writerow(
                [equipamento_atual["idEquipamento"], data_hora]
                + valores_capturados
                + metricas_requisicoes
                + valores_dashboard
            )
            progress.advance(task)


# ============================================================
# 12. ENVIO DOS ARQUIVOS CSV PARA O S3 (BRONZE)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# -- COMENTAR PARA TESTE
nome_arquivo = f"captura_{timestamp}.csv"

# Exemplo de caminho: bronze/InfraWatch/1/captura_2026-10-01_19-45-00.csv
arquivo_s3 = f"bronze/hardware/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo}"
caminho_csv_local = PASTA_LOCAL / arquivo_s3
caminho_csv_local.parent.mkdir(parents=True, exist_ok=True)
caminho_csv_local.write_text(csv_buffer.getvalue(), encoding="utf-8")

with console.status(f"[bold {AZUL_CLARO}]Enviando captura CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
    s3.put_object(
        Bucket=BUCKET,
        Key=arquivo_s3,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

arquivo_processos_s3 = None
caminho_processos_local = None
if processos_tem_dados:
    nome_arquivo_processos = f"captura_processos_{timestamp}.csv"
    arquivo_processos_s3 = f"bronze/processo/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo_processos}"
    caminho_processos_local = PASTA_LOCAL / arquivo_processos_s3
    caminho_processos_local.parent.mkdir(parents=True, exist_ok=True)
    caminho_processos_local.write_text(processos_buffer.getvalue(), encoding="utf-8")
    with console.status(f"[bold {AZUL_CLARO}]Enviando captura de processos para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_processos_s3,
            Body=processos_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )


console.print()
console.print(
    Panel(
        f"[bold white]Captura enviada para o S3 com sucesso![/bold white]\n\n"
        f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Caminho Bronze hardware:[/dim white] [bold {AZUL_CLARO}]{arquivo_s3}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Caminho Bronze processos:[/dim white] [bold {AZUL_CLARO}]{arquivo_processos_s3 or 'não gerado'}[/bold {AZUL_CLARO}]",
        title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 2)
    )
)
# -- FIM COMENTAR PARA TESTE


# ============================================================
# 13. ARQUIVOS CSV SEPARADOS DE HARDWARE E PROCESSOS
# ============================================================
console.print(f"[dim white]CSV de hardware salvo localmente em: {caminho_csv_local}[/dim white]")

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))