# ============================================================
# CAPTURA INDIVIDUAL - MATHEUS (Dashboard Histórico e Padrões)
# ------------------------------------------------------------
# Segue a mesma lógica do Scripts/captura.py do grupo:
#   1. conecta no banco InfraWatch
#   2. valida o usuário
#   3. verifica se o idMac já existe no banco
#      -> se não existir, cadastra o servidor e os limites padrão
#   4. busca os componentes ativos do servidor (parametro_alerta)
#   5. captura uma quantidade FINITA de amostras
#   6. envia o arquivo para a Bronze no S3
#
# Diferenças necessárias para a dashboard individual:
#   - gera JSON (e não CSV), no formato que o leitura.py espera
#   - captura REDE quando ela estiver ativa no cadastro do servidor
#   - captura processos (CPU e RAM) para o gráfico de processos
#   - não usa eval() nem IDs fixos de componente: busca pelo TIPO
# ============================================================
import psutil
from datetime import datetime, timezone
import time
import os
import uuid
import json
import boto3
import pymysql as mysql
from pathlib import Path
from dotenv import load_dotenv

# Rich para interface no terminal (mesmo visual do original)
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

def mostrar_aviso(mensagem, titulo="ATENÇÃO"):
    console.print(
        Panel(
            f"[bold white]{mensagem}[/bold white]",
            title=f"[bold {AZUL_CLARO}]{titulo}[/bold {AZUL_CLARO}]",
            border_style=AZUL_CLARO,
            style=f"on {PRETO}"
        )
    )


# ============================================================
# 0. CARREGANDO VARIÁVEIS DE AMBIENTE
# ============================================================
# Carrega o .env que está na mesma pasta deste script
PASTA_SCRIPT = Path(__file__).resolve().parent
load_dotenv(PASTA_SCRIPT / ".env", override=True)

# Variáveis AWS (aceita os nomes oficiais e os nomes usados no original)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACESS_KEY_ID") or None
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None
AWS_REGION = os.getenv("AWS_REGION") or "us-east-1"

# Variáveis BD
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT") or os.getenv("PORT") or "3306")

# Variáveis da captura
QUANTIDADE_AMOSTRAS = int(os.getenv("QUANTIDADE_AMOSTRAS") or "15")
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS") or "5")
CAMINHO_DISCO = os.getenv("CAMINHO_DISCO") or os.path.abspath(os.sep)

# Capacidade de reserva da placa de rede (Mbps).
# Usada SOMENTE quando o sistema informa velocidade 0 (comum em VM e Wi-Fi).
CAPACIDADE_REDE_MBPS = float(os.getenv("CAPACIDADE_REDE_MBPS") or "100")

# Bronze: bucket/prefixo do S3 e pasta local (cópia de segurança)
BUCKET_BRONZE = (os.getenv("S3_BUCKET_BRONZE") or "").strip()
PREFIXO_BRONZE = (os.getenv("S3_PREFIX_BRONZE") or "bronze/hardware/").strip("/")
PASTA_LOCAL = Path(os.getenv("DIRETORIO_DADOS") or PASTA_SCRIPT / "dados")

if QUANTIDADE_AMOSTRAS < 1 or INTERVALO_SEGUNDOS <= 0 or INTERVALO_SEGUNDOS > 290:
    raise ValueError("Use QUANTIDADE_AMOSTRAS >= 1 e INTERVALO_SEGUNDOS entre 0 e 290.")

# Limites padrão do cadastro automático (mesmo comportamento do original)
LIMITES_PADRAO = [
    # (tipo do componente, nomeMetrica, atenção, crítico)
    ("CPU", "USO_CPU", 60.0, 90.0),
    ("RAM", "USO_RAM", 60.0, 90.0),
    ("ARMAZENAMENTO", "USO_DISCO", 60.0, 90.0),
]

# Tipo do componente no banco -> campo no JSON
CAMPOS_COMPONENTES = {
    "CPU": "cpu",
    "RAM": "ram",
    "ARMAZENAMENTO": "disco",
    "DISCO": "disco",
    "REDE": "rede",
}

# Interfaces virtuais/loopback não representam a placa física
PREFIXOS_REDE_IGNORADOS = ("lo", "docker", "veth", "br-", "virbr", "vmnet", "vboxnet")


# Captura o idMac único da máquina atual
id_mac = str(uuid.getnode())

# ============================================================
# 1. CONEXÃO COM O BANCO DE DADOS (InfraWatch)
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
    port=DB_PORT,
    cursorclass=mysql.cursors.DictCursor
)

cursor = bd.cursor()

# Busca Equipamentos e idMac
cursor.execute("SELECT idEquipamento, nome, idMac, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

# Busca Componentes (catálogo)
cursor.execute("SELECT idComponente, nome, tipo FROM componente;")
componentes = cursor.fetchall()


# ============================================================
# 2. VALIDAÇÃO DO USUARIO
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

# Validação do Usuário na Tabela 'usuario' e busca do nome da Empresa
cursor.execute(
    """
    SELECT u.idUsuario, u.nome, u.email, u.fkEmpresa, e.nome AS nomeEmpresa
    FROM usuario u
    JOIN empresa e ON u.fkEmpresa = e.idEmpresa
    WHERE (u.email = %s OR u.nome = %s) AND u.senha = %s;
    """,
    (ipt_usuario, ipt_usuario, ipt_senha),
)
usuario_logado = cursor.fetchone()

if not usuario_logado:
    console.print()
    mostrar_aviso("Você não pode acessar nosso serviço (Acesso Negado).", "ACESSO NEGADO")
    cursor.close()
    bd.close()
    os._exit(0)

fk_empresa = usuario_logado["fkEmpresa"]
nome_empresa = usuario_logado["nomeEmpresa"]
nome_usuario = usuario_logado["nome"]


# ============================================================
# 3. VALIDAÇÃO OU CADASTRO DO SERVIDOR (pelo idMac)
# ============================================================
cadastrar = True
equipamento_atual = None

# Valida se o servidor existe
for i in range(len(equipamentos)):
    if equipamentos[i]["idMac"] == id_mac:
        cadastrar = False

        # Define o equipamento atual com as informações encontradas
        equipamento_atual = {
            "idEquipamento": equipamentos[i]["idEquipamento"],
            "nome": equipamentos[i]["nome"],
            "idMac": id_mac,
            "fkEmpresa": equipamentos[i]["fkEmpresa"],
        }

# Segurança: a máquina já cadastrada precisa ser da empresa do usuário
if equipamento_atual is not None and equipamento_atual["fkEmpresa"] != fk_empresa:
    mostrar_aviso("Esta máquina já está cadastrada em outra empresa. Entre com um usuário dessa empresa.")
    cursor.close()
    bd.close()
    os._exit(0)

# Se não existe, cadastra
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
        INSERT INTO equipamento (nome, tipo, status, idMac, fkEmpresa, ultimaAtualizacao)
        VALUES (%s, 'Servidor', 'Online', %s, %s, NOW());
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

    # Cadastro dos limites padrão (CPU, RAM e DISCO).
    # O id do componente é buscado pelo TIPO, e não fixo no código.
    for tipo, nome_metrica, atencao, critico in LIMITES_PADRAO:
        id_componente = None
        for c in componentes:
            if c["tipo"].upper() == tipo:
                id_componente = c["idComponente"]

        if id_componente is None:
            console.print(f"[bold yellow]Componente {tipo} não existe no catálogo. Limite não cadastrado.[/bold yellow]")
            continue

        cursor.execute(
            """
            INSERT INTO parametro_alerta
            (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente)
            VALUES (%s, %s, %s, '%%', 1, %s, %s);
            """,
            (nome_metrica, atencao, critico, equipamento_atual["idEquipamento"], id_componente),
        )
        bd.commit()
        console.print(f"[bold green]Métrica {nome_metrica} salva com sucesso![/bold green]")

    console.print("[dim white]Para monitorar a REDE, ative a opção 'Rede' no cadastro do servidor no site.[/dim white]\n")

# Atualiza a data da última atualização do servidor
cursor.execute(
    "UPDATE equipamento SET ultimaAtualizacao = NOW() WHERE idEquipamento = %s;",
    (equipamento_atual["idEquipamento"],),
)
bd.commit()


# ============================================================
# 4. BUSCA OS COMPONENTES ATIVOS DO EQUIPAMENTO ATUAL
# ============================================================
# É necessário fazer essa query aqui para que a tabela venha atualizada
cursor.execute(
    """
    SELECT DISTINCT c.idComponente, c.nome, c.tipo
    FROM componente c
    JOIN parametro_alerta pa ON c.idComponente = pa.fkComponente
    WHERE pa.fkEquipamento = %s AND pa.ativo = 1;
    """,
    (equipamento_atual["idEquipamento"],),
)
componentes_selecionados = cursor.fetchall()

cursor.close()
bd.close()

# Monta a lista dos campos que serão capturados (cpu, ram, disco, rede)
capturar_comp = []
for c in componentes_selecionados:
    campo = CAMPOS_COMPONENTES.get(c["tipo"].upper())
    if campo is not None and campo not in capturar_comp:
        capturar_comp.append(campo)

if not capturar_comp:
    mostrar_aviso("Nenhum componente (CPU, RAM, Disco ou Rede) ativo para este servidor. "
                  "Configure os componentes no site e execute novamente.")
    os._exit(0)


limpar_tela()
mostrar_banner()

console.print(
    Panel(
        f"[bold white]Olá, {nome_usuario}![/bold white]\n\n"
        "[dim white]As informações da sua máquina serão coletadas automaticamente.[/dim white]\n"
        f"Empresa: {nome_empresa} · Servidor: {equipamento_atual['nome']}\n"
        f"Componentes: {', '.join(capturar_comp).upper()} + PROCESSOS\n"
        f"Amostras: {QUANTIDADE_AMOSTRAS} · intervalo: {INTERVALO_SEGUNDOS:g} s",
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
    mostrar_aviso("Fechando script...", "InfraWatch")
    os._exit(0)


# ============================================================
# 5. FUNÇÕES DE CAPTURA
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


def capturar_amostra():
    # Uma amostra = um intervalo de INTERVALO_SEGUNDOS segundos
    placas = interfaces_de_rede() if "rede" in capturar_comp else {}
    rede_antes = ler_contadores_rede(placas)
    processos_antes, completo_antes = ler_processos()
    psutil.cpu_percent(interval=None)  # prepara o contador de CPU

    inicio = datetime.now(timezone.utc)
    relogio = time.monotonic()
    time.sleep(INTERVALO_SEGUNDOS)

    cpu = psutil.cpu_percent(interval=None)
    fim = datetime.now(timezone.utc)
    segundos = time.monotonic() - relogio
    rede_depois = ler_contadores_rede(placas)
    processos_depois, completo_depois = ler_processos()

    amostra = {
        "versao": 2,
        "empresa_id": str(equipamento_atual["fkEmpresa"]),
        "servidor_id": str(equipamento_atual["idEquipamento"]),
        "id_mac": id_mac,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "cpu": None,
        "ram": None,
        "disco": None,
        "rede": None,
    }

    # Só preenche os componentes ativos no cadastro; os demais ficam null
    if "cpu" in capturar_comp:
        amostra["cpu"] = cpu
    if "ram" in capturar_comp:
        amostra["ram"] = psutil.virtual_memory().percent
    if "disco" in capturar_comp:
        amostra["disco"] = psutil.disk_usage(CAMINHO_DISCO).percent
    if "rede" in capturar_comp:
        uso, mbps, capacidade, reserva = calcular_uso_rede(placas, rede_antes, rede_depois, segundos)
        amostra["rede"] = uso
        amostra["rede_mbps"] = mbps
        amostra["rede_capacidade_mbps"] = capacidade
        amostra["rede_capacidade_reserva"] = reserva

    amostra["processos"] = calcular_processos(processos_antes, processos_depois, segundos)
    amostra["processos_completos"] = completo_antes and completo_depois
    return amostra


def salvar_local(caminho, dados):
    # Grava primeiro em .tmp e depois troca, para não corromper o arquivo
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temporario = caminho.with_suffix(".tmp")
    with open(temporario, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2)
    temporario.replace(caminho)


# ============================================================
# 6. EXECUÇÃO DA CAPTURA (QUANTIDADE_AMOSTRAS)
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel("[bold white]Preparando captura dos dados...[/bold white]",
          title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)
console.print()

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
nome_arquivo = f"captura_{timestamp}_{uuid.uuid4().hex[:8]}.json"

# Exemplo de caminho: bronze/hardware/2/1/captura_2026-11-15_19-45-00_ab12cd34.json
arquivo_s3 = f"{PREFIXO_BRONZE}/{equipamento_atual['fkEmpresa']}/{equipamento_atual['idEquipamento']}/{nome_arquivo}"
caminho_local = PASTA_LOCAL / arquivo_s3

amostras = []
try:
    with Progress(
        TextColumn(f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"),
        BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
        TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        task = progress.add_task("captura", total=QUANTIDADE_AMOSTRAS)
        for i in range(QUANTIDADE_AMOSTRAS):
            amostras.append(capturar_amostra())
            salvar_local(caminho_local, amostras)  # salva após cada amostra
            progress.advance(task)
except KeyboardInterrupt:
    console.print("[yellow]Captura interrompida; as amostras concluídas foram preservadas.[/yellow]")

if not amostras:
    mostrar_aviso("Nenhuma amostra foi gerada.")
    os._exit(0)


# ============================================================
# 7. ENVIO DO ARQUIVO JSON PARA O S3 (BRONZE)
# ============================================================
enviado = False
if BUCKET_BRONZE:
    try:
        with console.status(f"[bold {AZUL_CLARO}]Enviando captura JSON para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
            s3 = boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN
            )
            s3.put_object(
                Bucket=BUCKET_BRONZE,
                Key=arquivo_s3,
                Body=json.dumps(amostras, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json"
            )
        enviado = True
    except Exception as erro:
        console.print(f"[yellow]Não foi possível enviar ao S3 ({type(erro).__name__}). O JSON ficou salvo localmente.[/yellow]")
        console.print("Detalhes:", str(erro), markup=False)

console.print()
if enviado:
    console.print(
        Panel(
            f"[bold white]Captura enviada para o S3 com sucesso![/bold white]\n\n"
            f"[dim white]Amostras:[/dim white] [bold {AZUL_CLARO}]{len(amostras)}[/bold {AZUL_CLARO}]\n"
            f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET_BRONZE}[/bold {AZUL_CLARO}]\n"
            f"[dim white]Caminho Bronze:[/dim white] [bold {AZUL_CLARO}]{arquivo_s3}[/bold {AZUL_CLARO}]\n"
            f"[dim white]Cópia local:[/dim white] {caminho_local}",
            title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
            border_style=AZUL_CLARO,
            style=f"on {PRETO}",
            padding=(1, 2)
        )
    )
else:
    console.print(
        Panel(
            f"[bold white]Captura salva no seu computador.[/bold white]\n\n"
            f"[dim white]Amostras:[/dim white] {len(amostras)}\n"
            f"[dim white]Arquivo:[/dim white] {caminho_local}\n"
            ("[dim white]Confira o erro acima e o destino configurado no .env.[/dim white]"
             if BUCKET_BRONZE else
             "[dim white]Configure S3_BUCKET_BRONZE no .env para enviar ao S3.[/dim white]"),
            title=f"[bold {AZUL_CLARO}]JSON SALVO LOCALMENTE[/bold {AZUL_CLARO}]",
            border_style=AZUL_CLARO,
            style=f"on {PRETO}",
            padding=(1, 2)
        )
    )

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))
