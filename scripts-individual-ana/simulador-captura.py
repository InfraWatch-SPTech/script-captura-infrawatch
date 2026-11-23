import psutil
import csv
import random                                   # NOVO: stdlib (simulação)
from datetime import datetime, timedelta        # NOVO: timedelta (stdlib)
import time
import os
import uuid
import boto3
import io
import pymysql as mysql
import sys
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
# CONFIGURAÇÃO DA SIMULAÇÃO (DADOS FICTÍCIOS)
# ============================================================
QTD_SERVIDORES_SIMULADOS = 5
DIAS_SIMULADOS = 7
INTERVALO_MINUTOS = 60          # 1 amostra/hora => 168 amostras por servidor
LIMIAR_CRITICO = 95.0
DESVIO_PADRAO = 3.0
SEMENTE_SIMULACAO = 2026


# ============================================================
# 0. CARREGANDO VARIÁVEIS DE AMBIENTE
# ============================================================
load_dotenv()

AWS_ACESS_KEY_ID = os.getenv("AWS_ACESS_KEY_ID") or None
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = int(os.getenv("PORT"))


# ============================================================
# 1. CONEXÃO COM O S3
# ============================================================
BUCKET = "data-lake-infrawatch"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

id_mac = uuid.getnode()


# ============================================================
# 2. CONEXÃO COM O BANCO DE DADOS (InfraWatch)
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

cursor.execute("SELECT idEquipamento, nome, idMac, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente;")
componentes = cursor.fetchall()

cursor.execute("SELECT nome, email, senha, fkEmpresa FROM usuario;")
funcionarios = cursor.fetchall()

cursor.execute("SELECT idEmpresa, nome FROM empresa;")
empresas = cursor.fetchall()


# ============================================================
# 3. VALIDAÇÃO DO USUÁRIO
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
    os._exit(0)

fk_empresa = usuario_logado["fkEmpresa"]
nome_empresa = usuario_logado["nomeEmpresa"]
nome_usuario = usuario_logado["nome"]


# ============================================================
# 4. VALIDAÇÃO OU CADASTRO DO SERVIDOR
# ============================================================
cadastrar = True

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

    for metrica, id_comp in (('USE_CPU', 1), ('USE_RAM', 2), ('USE_DISCO', 6)):
        cursor.execute(
            """
            INSERT INTO parametro_alerta
            (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente)
            VALUES (%s, 60.0, 90.0, '%%', 1, %s, %s)
            """,
            (metrica, equipamento_atual["idEquipamento"], id_comp),
        )
        bd.commit()
        console.print(f"[bold green]Métrica {metrica} salva com sucesso![/bold green]\n")


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
    console.print("[bold yellow]Aviso: Nenhum componente ativo configurado. Carregando lista geral...[/bold yellow]")
    cursor.execute("SELECT idComponente, nome, tipo FROM componente;")
    componentes_selecionados_objs = cursor.fetchall()

if not componentes_selecionados_objs:
    console.print("[bold yellow]Nenhum componente selecionado para monitorar. Encerrando.[/bold yellow]")
    cursor.close()
    bd.close()
    sys.exit(0)

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

console.print()
continuar = Confirm.ask("[bold white]Deseja continuar?[/bold white]", default=True)

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

titulo_executados = "id;data_hora"

for comp_id in capturar_comp:
    for c in componentes:
        if comp_id == 6:
            continue
        elif comp_id == c['idComponente']:
            titulo_executados += f";{c['tipo']}"

cabecalho_csv = titulo_executados.split(';')

csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer, delimiter=';')
writer.writerow(cabecalho_csv)


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

    csv_processos = io.StringIO()
    writer_proc = csv.writer(csv_processos, delimiter=';')
    writer_proc.writerow(['PID', 'PROCESSO', 'RAM', 'TAMANHO', 'CAMINHO'])
    writer_proc.writerows(infos_processos)
    return csv_processos


# ============================================================
# 9. EXECUÇÃO DA CAPTURA (5 AMOSTRAS REAIS)
# ============================================================
with Progress(
    TextColumn(f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"),
    BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
    TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
    TimeElapsedColumn(),
    console=console
) as progress:
    task = progress.add_task("captura", total=5)
    for i in range(5):
        valores_capturados = []
        for comp_id in capturar_comp:
            if comp_id == 6:
                csv_processos = capturar_processos()
                enviar_processos = True
            else:
                for c in componentes:
                    if comp_id == c['idComponente']:
                        val = eval(c['codigo'])   # <-- origem dos valores-base
                        valores_capturados.append(val)

        data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        writer.writerow([equipamento_atual['idEquipamento'], data_hora] + valores_capturados)

        time.sleep(1)
        progress.advance(task)


# ============================================================
# 10. SIMULAÇÃO DOS DADOS (5 SERVIDORES / 1 SEMANA)  <-- NOVO
# ------------------------------------------------------------
# Dados FICTÍCIOS derivados da captura inicial:
#   - a linha de base é a média das 5 amostras reais acima;
#   - NÃO há novas chamadas psutil (valores vêm do eval());
#   - apenas bibliotecas da stdlib: random, datetime, csv, io;
#   - um CSV por servidor, com 168 amostras horárias.
# ============================================================
def capturar_medias_iniciais(csv_em_memoria):
    linhas = list(csv.reader(io.StringIO(csv_em_memoria.getvalue()), delimiter=';'))
    if len(linhas) < 2:
        return [], {}

    cabecalho = linhas[0]
    nomes_metricas = [nome for nome in cabecalho if nome not in ('id', 'data_hora')]
    medias = {}

    for indice, nome in enumerate(cabecalho):
        if nome in ('id', 'data_hora'):
            continue
        valores = []
        for linha in linhas[1:]:
            if indice < len(linha) and linha[indice] != '':
                try:
                    valores.append(float(linha[indice]))
                except ValueError:
                    pass
        medias[nome] = round(sum(valores) / len(valores), 2) if valores else 0.0

    return nomes_metricas, medias


def limitar(valor):
    return round(min(100.0, max(0.0, valor)), 2)


def em_janela_critica(instante, indice_servidor, nome_metrica, metricas_criticas):
    """Define quando um componente simulado deve ultrapassar 95%."""
    # Apenas os 3 primeiros servidores entram em estado crítico.
    if indice_servidor >= 3 or not metricas_criticas:
        return False

    critica = metricas_criticas[indice_servidor % len(metricas_criticas)]
    if nome_metrica != critica:
        return False

    dia = instante.weekday()   # 0=segunda ... 6=domingo
    hora = instante.hour

    if indice_servidor == 0:       # 1º: dias úteis, das 10h às 17h
        return dia < 5 and 10 <= hora < 17
    if indice_servidor == 1:       # 2º: fim de semana inteiro
        return dia >= 5
    return 20 <= hora < 23         # 3º: todas as noites


def buscar_ids_servidores(fk_empresa, limite=QTD_SERVIDORES_SIMULADOS):
    """Busca IDs de equipamentos reais (tipo 'Servidor') da empresa do usuário."""
    conexao = mysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=PORT,
        cursorclass=mysql.cursors.DictCursor
    )
    try:
        with conexao.cursor() as cur:
            cur.execute(
                """
                SELECT idEquipamento, nome
                FROM equipamento
                WHERE fkEmpresa = %s AND tipo = 'Servidor'
                ORDER BY idEquipamento
                LIMIT %s;
                """,
                (fk_empresa, limite),
            )
            return [linha['idEquipamento'] for linha in cur.fetchall()]
    finally:
        conexao.close()


def simular_servidores(csv_em_memoria, componentes, cabecalho, ids_servidores):
    nomes_metricas, medias = capturar_medias_iniciais(csv_em_memoria)
    if not nomes_metricas or not ids_servidores:
        return {}, 0

    # Só componentes medidos em '%' podem "estourar" 95%.
    medida_por_tipo = {c['tipo']: c['medida'] for c in componentes}
    metricas_percentuais = [m for m in nomes_metricas if medida_por_tipo.get(m) == '%']
    metricas_criticas = metricas_percentuais or nomes_metricas

    fatores = [1.08, 0.92, 1.12, 0.85, 1.00]

    fim = datetime.now().replace(minute=0, second=0, microsecond=0)
    inicio = fim - timedelta(days=DIAS_SIMULADOS)

    arquivos = {}
    total_amostras = 0

    # IDs reais do banco, mantendo a ordem da consulta
    for indice, servidor_id in enumerate(ids_servidores):
        aleatorio = random.Random(SEMENTE_SIMULACAO + servidor_id)
        fator = fatores[indice % len(fatores)]

        buffer = io.StringIO()
        writer_sim = csv.writer(buffer, delimiter=';')
        writer_sim.writerow(cabecalho)

        instante = inicio
        while instante < fim:
            valores = []
            for nome in nomes_metricas:
                base = medias[nome]
                valor = base * fator + aleatorio.gauss(0, DESVIO_PADRAO)

                if em_janela_critica(instante, indice, nome, metricas_criticas):
                    valor = aleatorio.uniform(LIMIAR_CRITICO + 0.5, 99.9)

                valores.append(limitar(valor))

            writer_sim.writerow(
                [servidor_id, instante.strftime("%Y-%m-%d %H:%M:%S")] + valores
            )
            total_amostras += 1
            instante += timedelta(minutes=INTERVALO_MINUTOS)

        arquivos[servidor_id] = buffer

    return arquivos, total_amostras


ids_servidores = buscar_ids_servidores(fk_empresa)

if len(ids_servidores) < QTD_SERVIDORES_SIMULADOS:
    console.print(
        f"[bold yellow]Aviso: a empresa possui apenas {len(ids_servidores)} "
        f"servidor(es) cadastrado(s). A simulação usará os IDs disponíveis.[/bold yellow]"
    )

console.print(
    Panel(
        f"[bold white]Simulando {len(ids_servidores)} servidores fictícios "
        f"({DIAS_SIMULADOS} dias) a partir da captura inicial...[/bold white]",
        title=f"[bold {AZUL_CLARO}]SIMULAÇÃO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}"
    )
)

arquivos_simulados, total_amostras_simuladas = simular_servidores(
    csv_buffer, componentes, cabecalho_csv, ids_servidores
)

console.print(
    f"[bold green]Simulação pronta: {len(arquivos_simulados)} servidores, "
    f"{total_amostras_simuladas} amostras fictícias.[/bold green]"
)


# ============================================================
# 11. ENVIO DOS ARQUIVOS CSV PARA O S3 (BRONZE)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# --- Captura real (comportamento original) ---
nome_arquivo = f"captura_{timestamp}.csv"
arquivo_s3 = f"bronze/hardware/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo}"

with console.status(
    f"[bold {AZUL_CLARO}]Enviando captura CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]",
    spinner="dots"
):
    s3.put_object(
        Bucket=BUCKET,
        Key=arquivo_s3,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

# --- Capturas simuladas (NOVO) ---
with console.status(
    f"[bold {AZUL_CLARO}]Enviando {len(arquivos_simulados)} capturas simuladas...[/bold {AZUL_CLARO}]",
    spinner="dots"
):
    for servidor_id, buffer in arquivos_simulados.items():
        nome_simulado = f"captura_{timestamp}.csv"
        arquivo_simulado_s3 = (
            f"bronze/hardware/{nome_empresa}/{servidor_id}/{nome_simulado}"
        )
        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_simulado_s3,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )

# --- Processos (comportamento original) ---
nome_arquivo_processos = f"captura_processos_{timestamp}.csv"
arquivo_processos_s3 = (
    f"bronze/processo/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo_processos}"
)

if enviar_processos:
    with console.status(
        f"[bold {AZUL_CLARO}]Enviando processos CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]",
        spinner="dots"
    ):
        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_processos_s3,
            Body=csv_processos.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )

console.print()
console.print(
    Panel(
        f"[bold white]Capturas enviadas para o S3 com sucesso![/bold white]\n\n"
        f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Captura real:[/dim white] [bold {AZUL_CLARO}]{arquivo_s3}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Simuladas:[/dim white] [bold {AZUL_CLARO}]{len(arquivos_simulados)} arquivos "
        f"({total_amostras_simuladas} amostras)[/bold {AZUL_CLARO}]",
        title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 2)
    )
)

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))
