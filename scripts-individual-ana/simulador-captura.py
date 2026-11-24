"""
simulador-captura.py

Baseado em ana-captura.py.
Captura inicial via eval() -> simula 5 servidores da mesma empresa
durante 1 semana -> envia os CSVs simulados para a camada Bronze do S3.

A simulação em si usa apenas bibliotecas da stdlib.
"""

# ============================================================
# IMPORTS
# ============================================================
# Stdlib usada pela SIMULAÇÃO
import csv
import io
import os
import sys
import uuid
import random
import time
from datetime import datetime, timedelta

# Já presentes no fluxo original: banco, S3, terminal e eval()
import psutil  # apenas para o eval() resolver o código vindo do banco
import boto3
import pymysql as mysql
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Prompt
from rich.align import Align
from rich import box

# ============================================================
# CONFIGURAÇÃO VISUAL (igual ao ana-captura.py)
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
            box=box.ROUNDED,
        )
    )


def limpar_tela():
    console.clear()


# ============================================================
# PARÂMETROS DA SIMULAÇÃO (somente stdlib)
# ============================================================
QTD_SERVIDORES = 5
DIAS_SIMULADOS = 7
INTERVALO_MINUTOS = 60          # 1 amostra/hora => 168 amostras + a atual
AMOSTRAS_BASE = 5               # amostras reais usadas como semente
LIMIAR_CRITICO = 95.0           # apenas rótulo dos picos gerados (95,5% a 99,7%)
DESVIO_PADRAO = 2.5
SEMENTE = 2026
FATORES_SERVIDOR = [1.10, 0.95, 1.05, 0.88, 1.00]

# Atualizado com a quantidade real de servidores após a consulta ao banco.
TOTAL_SERVIDORES_SIMULACAO = QTD_SERVIDORES

# Limites usados quando parametro_alerta não trouxer regra para a métrica.
# São os mesmos gravados em garantir_parametros_padrao() e no ana-captura.py.
LIMITE_ATENCAO_PADRAO = 60.0
LIMITE_CRITICO_PADRAO = 90.0

CAMPOS_COMPONENTES = {
    "CPU": "CPU", "RAM": "RAM", "ARMAZENAMENTO": "ARMAZENAMENTO",
    "DISCO": "ARMAZENAMENTO", "REDE": "REDE",
}

# Mesmo mapeamento de ana-teste-leitura.py: nomeMetrica -> colunas do CSV.
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

# ============================================================
# 0. AMBIENTE
# ============================================================
load_dotenv()

AWS_ACESS_KEY_ID = os.getenv("AWS_ACESS_KEY_ID") or None
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or None
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN") or None
AWS_REGION = os.getenv("AWS_REGION") or "us-east-1"

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = int(os.getenv("PORT"))

BUCKET = "data-lake-infrawatch"

# ============================================================
# 1. S3
# ============================================================
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)

id_mac = uuid.getnode()

# ============================================================
# 2. BANCO DE DADOS
# ============================================================
console.print(
    Panel(
        "[bold white]Conectando ao banco de dados InfraWatch...[/bold white]",
        border_style=AZUL_CLARO,
        style=f"on {AZUL_ESCURO}",
    )
)

bd = mysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=PORT,
    cursorclass=mysql.cursors.DictCursor,
)
cursor = bd.cursor()

cursor.execute("SELECT idEquipamento, nome, idMac, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente;")
componentes = cursor.fetchall()

# ============================================================
# 3. LOGIN
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel(
        Align.center(
            "[bold white]Bem-vindo à InfraWatch![/bold white]\n"
            "[dim white]Insira suas credenciais para continuar.[/dim white]"
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
    SELECT u.idUsuario, u.nome, u.email, u.senha, u.fkEmpresa, e.nome AS nomeEmpresa
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
            style=f"on {PRETO}",
        )
    )
    cursor.close()
    bd.close()
    os._exit(0)

fk_empresa = usuario_logado["fkEmpresa"]
nome_empresa = usuario_logado["nomeEmpresa"]

# ============================================================
# 4. VALIDAÇÃO / CADASTRO DO SERVIDOR ATUAL
# ============================================================
cadastrar = True

for equipamento in equipamentos:
    if equipamento["idMac"] == str(id_mac):
        cadastrar = False
        equipamento_atual = {
            "idEquipamento": equipamento["idEquipamento"],
            "nome": equipamento["nome"],
            "idMac": id_mac,
            "fkEmpresa": equipamento["fkEmpresa"],
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
        (nome_novo_servidor, str(id_mac), fk_empresa),
    )
    bd.commit()

    equipamento_atual = {
        "idEquipamento": cursor.lastrowid,
        "nome": nome_novo_servidor,
        "idMac": id_mac,
        "fkEmpresa": fk_empresa,
    }
    console.print(
        f"[bold green]Novo servidor '{nome_novo_servidor}' cadastrado "
        f"(ID: {equipamento_atual['idEquipamento']})[/bold green]\n"
    )


def garantir_parametros_padrao(id_equipamento):
    """Grava CPU, RAM e PROCESSOS como default, como no ana-captura.py.

    O INSERT IGNORE também recupera equipamentos que já existem no banco mas
    ficaram sem parâmetros de alerta (por exemplo, cadastrados por uma execução
    anterior deste simulador).
    """
    padroes = (
        ("USE_CPU", 1),
        ("USE_RAM", 2),
        ("PROCESSOS_ATIVOS", 6),
    )
    for nome_metrica, id_componente in padroes:
        cursor.execute(
            """
            INSERT IGNORE INTO parametro_alerta
            (nomeMetrica, limite_atencao, limite_critico, unidade, ativo,
             fkEquipamento, fkComponente)
            VALUES (%s, 60.0, 90.0, '%%', 1, %s, %s);
            """,
            (nome_metrica, id_equipamento, id_componente),
        )
    bd.commit()


garantir_parametros_padrao(equipamento_atual["idEquipamento"])

# ============================================================
# 5. COMPONENTES ATIVOS DO EQUIPAMENTO
# ============================================================
cursor.execute(
    "SELECT DISTINCT fkComponente, fkEquipamento FROM parametro_alerta WHERE ativo = 1;"
)
equip_comp = cursor.fetchall()

cursor.execute(
    """
    SELECT DISTINCT c.idComponente, c.nome, c.tipo, c.medida, c.codigo
    FROM componente c
    JOIN parametro_alerta pa ON c.idComponente = pa.fkComponente
    WHERE pa.fkEquipamento = %s AND pa.ativo = 1;
    """,
    (equipamento_atual["idEquipamento"],),
)
componentes_selecionados = cursor.fetchall()

if not componentes_selecionados:
    cursor.execute("SELECT idComponente, nome, tipo, medida, codigo FROM componente;")
    componentes_selecionados = cursor.fetchall()

if not componentes_selecionados:
    console.print("[bold yellow]Nenhum componente para monitorar. Encerrando.[/bold yellow]")
    cursor.close()
    bd.close()
    sys.exit(0)

cursor.close()
bd.close()

capturar_comp = [
    ec["fkComponente"]
    for ec in equip_comp
    if ec["fkEquipamento"] == equipamento_atual["idEquipamento"]
]

limpar_tela()
mostrar_banner()

# ============================================================
# 6. CAPTURA INICIAL — SEM CÓDIGO psutil ESCRITO AQUI
#    Os valores vêm exclusivamente de eval(codigo_do_componente).
# ============================================================
def capturar_linha_base():
    """Retorna {metrica: valor} usando somente o eval() dos componentes."""
    linha = {}
    for comp_id in capturar_comp:
        if comp_id == 6:          # PROCESSOS não é métrica numérica
            continue
        for componente in componentes:
            if componente["idComponente"] != comp_id:
                continue

            codigo = (componente.get("codigo") or "").strip()
            if not codigo:
                continue

            campo = CAMPOS_COMPONENTES.get(
                str(componente["tipo"]).upper(),
                str(componente["tipo"]).upper(),
            )
            if campo in linha:
                continue

            try:
                valor = float(eval(codigo))  # psutil fica dentro do código do banco
            except Exception:
                continue

            linha[campo] = round(valor, 2)
    return linha


console.print(
    Panel(
        "[bold white]Coletando captura inicial (semente da simulação)...[/bold white]",
        title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {AZUL_ESCURO}",
    )
)

primeira = capturar_linha_base()
nomes_metricas = list(primeira.keys())

if not nomes_metricas:
    console.print(
        "[bold red]Nenhum componente com código eval() válido. Encerrando.[/bold red]\n"
        f"[dim white]Equipamento: {equipamento_atual['idEquipamento']} | "
        f"componentes ativos: {capturar_comp} | "
        f"códigos: "
        + ", ".join(
            f"{c['tipo']}={c.get('codigo')!r}" for c in componentes
        )
        + "[/dim white]"
    )
    sys.exit(1)

cabecalho_csv = ["id", "data_hora"] + nomes_metricas
linhas_base = [
    [equipamento_atual["idEquipamento"],
     datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    + [primeira[nome] for nome in nomes_metricas]
]

with Progress(
    TextColumn(f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"),
    BarColumn(complete_style=AZUL_CLARO, finished_style=AZUL_CLARO),
    TextColumn("[bold white]{task.percentage:>3.0f}%[/bold white]"),
    TimeElapsedColumn(),
    console=console,
) as progress:
    task = progress.add_task("captura", total=AMOSTRAS_BASE - 1)
    for _ in range(AMOSTRAS_BASE - 1):
        amostra = capturar_linha_base()
        linhas_base.append(
            [equipamento_atual["idEquipamento"],
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            + [amostra.get(nome, primeira[nome]) for nome in nomes_metricas]
        )
        time.sleep(1)
        progress.advance(task)

medias_base = {
    nome: round(
        sum(float(linha[2 + i]) for linha in linhas_base) / len(linhas_base), 2
    )
    for i, nome in enumerate(nomes_metricas)
}

console.print(
    f"[bold green]Semente pronta:[/bold green] "
    + ", ".join(f"{k}={v}%" for k, v in medias_base.items())
)

# ============================================================
# 7. SIMULAÇÃO — SOMENTE STDLIB
# ============================================================
def limitar(valor):
    return round(max(0.0, min(100.0, valor)), 2)


def em_janela_critica(indice_servidor, metrica, instante):
    """Cada servidor 'estoura' um componente diferente em uma janela da semana."""
    alvo = nomes_metricas[indice_servidor % len(nomes_metricas)]
    if metrica != alvo:
        return False

    dia = instante.weekday()   # 0=seg ... 6=dom
    hora = instante.hour

    # "Plantão" por hora: cobre os 168 horários da semana para que sempre
    # exista um servidor em alerta — inclusive na amostra do momento atual,
    # que pode cair numa hora não coberta pelas janelas específicas abaixo.
    if indice_servidor == (dia * 24 + hora) % TOTAL_SERVIDORES_SIMULACAO:
        return True

    if indice_servidor == 0:   # CPU em dias úteis, das 9h às 12h
        return dia < 5 and 9 <= hora < 12
    if indice_servidor == 1:   # RAM todas as noites, das 19h às 22h
        return 19 <= hora < 22
    if indice_servidor == 2:   # ARMAZENAMENTO no fim de semana inteiro
        return dia >= 5 and 10 <= hora < 18
    if indice_servidor == 3:   # componente da vez na madrugada de terça
        return dia == 1 and 0 <= hora < 6
    return 13 <= hora < 16     # 5º servidor: todas as tardes


def buscar_ids_servidores():
    """IDs de até 5 servidores reais da empresa do usuário logado."""
    conexao = mysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=PORT,
        cursorclass=mysql.cursors.DictCursor,
    )
    try:
        with conexao.cursor() as cur:
            cur.execute(
                """
                SELECT idEquipamento
                FROM equipamento
                WHERE fkEmpresa = %s AND tipo = 'Servidor'
                ORDER BY idEquipamento
                LIMIT %s;
                """,
                (fk_empresa, QTD_SERVIDORES),
            )
            return [linha["idEquipamento"] for linha in cur.fetchall()]
    finally:
        conexao.close()


def buscar_regras_alerta(ids_servidores):
    """Regras ativas de parametro_alerta dos servidores simulados.

    Mesma consulta usada em ana-teste-leitura.py: os limites vêm do banco
    (60/90 para os componentes padrão, 70/90, 75/90, 80/95 nos seeds).
    """
    conexao = mysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=PORT,
        cursorclass=mysql.cursors.DictCursor,
    )
    try:
        with conexao.cursor() as cur:
            cur.execute(
                """
                SELECT pa.fkEquipamento AS id_servidor,
                       pa.fkComponente  AS id_componente,
                       pa.nomeMetrica,
                       pa.limite_atencao,
                       pa.limite_critico,
                       c.nome AS componente
                FROM parametro_alerta pa
                JOIN componente c ON c.idComponente = pa.fkComponente
                WHERE pa.ativo = 1
                  AND pa.fkEquipamento IN (%s)
                ORDER BY pa.fkEquipamento, pa.fkComponente;
                """ % ", ".join(["%s"] * len(ids_servidores)),
                tuple(ids_servidores),
            )
            regras = cur.fetchall()
    finally:
        conexao.close()

    por_servidor = {}
    for regra in regras:
        por_servidor.setdefault(int(regra["id_servidor"]), []).append(
            {
                "metrica": regra["nomeMetrica"],
                "componente": regra["componente"],
                "atencao": float(regra["limite_atencao"]),
                "critico": float(regra["limite_critico"]),
            }
        )
    return por_servidor


def nivel_alerta(valor, limite_atencao, limite_critico):
    """CRITICO, ATENCAO ou None — igual à leitura real."""
    if valor is None:
        return None
    if valor >= float(limite_critico):
        return "CRITICO"
    if valor >= float(limite_atencao):
        return "ATENCAO"
    return None


def avaliar_alerta_atual(buffer, regras):
    """Avalia a ÚLTIMA amostra/leitura do CSV contra os limites do banco.

    Espelha calcular_alertas_por_servidor(): ordena por data_hora, pega a
    última linha e aplica os limites de cada regra ativa do servidor.
    """
    linhas = [linha for linha in csv.reader(
        io.StringIO(buffer.getvalue()), delimiter=";"
    ) if linha]
    if len(linhas) < 2:
        return "SEM LEITURA", [], ""

    colunas = linhas[0]
    ultima = dict(zip(colunas[1:], linhas[-1][1:]))  # ignora a coluna "id"
    data_hora = ultima.pop("data_hora", "")

    alertas = []
    for regra in regras:
        aliases = ALIAS_METRICAS.get(
            regra["metrica"], {regra["metrica"]}
        )
        valor = None
        for alias in aliases:
            chave = next(
                (c for c in ultima if c.strip().lower() == alias.strip().lower()),
                None,
            )
            if chave is not None and ultima[chave] != "":
                try:
                    valor = float(ultima[chave])
                    break
                except ValueError:
                    pass

        # Sem regra no banco para a métrica, usa os limites padrão 60/90.
        nivel = nivel_alerta(
            valor,
            regra.get("atencao", LIMITE_ATENCAO_PADRAO),
            regra.get("critico", LIMITE_CRITICO_PADRAO),
        )
        if nivel is not None:
            alertas.append(
                {
                    "componente": regra.get("componente", regra["metrica"]),
                    "metrica": regra["metrica"],
                    "nivel": nivel,
                    "valor": valor,
                    "atencao": regra.get("atencao", LIMITE_ATENCAO_PADRAO),
                    "critico": regra.get("critico", LIMITE_CRITICO_PADRAO),
                }
            )

    if any(a["nivel"] == "CRITICO" for a in alertas):
        nivel_geral = "CRITICO"
    elif alertas:
        nivel_geral = "ATENCAO"
    else:
        nivel_geral = "NORMAL"

    return nivel_geral, alertas, data_hora


def simular_servidor(servidor_id, indice, inicio, fim):
    aleatorio = random.Random(SEMENTE + int(servidor_id))
    fator = FATORES_SERVIDOR[indice % len(FATORES_SERVIDOR)]

    buffer = io.StringIO()
    escritor = csv.writer(buffer, delimiter=";")
    escritor.writerow(cabecalho_csv)

    instante = inicio
    total = 0
    criticos = 0

    # "<= fim" inclui a amostra do instante atual (hora corrente). Sem isso a
    # última leitura ficava em (fim - 1h) e nunca representava o "agora".
    while instante <= fim:
        valores = []
        for nome in nomes_metricas:
            base = medias_base[nome]
            valor = base * fator + aleatorio.gauss(0, DESVIO_PADRAO)

            # leve crescimento gradual do armazenamento ao longo da semana
            if "ARMAZENAMENTO" in nome.upper() or "DISCO" in nome.upper():
                valor += 0.35 * (instante - inicio).days

            if em_janela_critica(indice, nome, instante):
                valor = aleatorio.uniform(95.5, 99.7)
                criticos += 1

            valores.append(limitar(valor))

        escritor.writerow(
            [servidor_id, instante.strftime("%Y-%m-%d %H:%M:%S")] + valores
        )
        total += 1
        instante += timedelta(minutes=INTERVALO_MINUTOS)

    return buffer, total, criticos


ids_servidores = buscar_ids_servidores()

if not ids_servidores:
    console.print("[bold red]Nenhum servidor da empresa encontrado. Encerrando.[/bold red]")
    sys.exit(1)

TOTAL_SERVIDORES_SIMULACAO = len(ids_servidores)

if len(ids_servidores) < QTD_SERVIDORES:
    console.print(
        f"[bold yellow]Aviso: a empresa possui apenas {len(ids_servidores)} "
        f"servidor(es). Simulando os disponíveis.[/bold yellow]"
    )

console.print(
    Panel(
        f"[bold white]Simulando {len(ids_servidores)} servidor(es) da empresa "
        f"'{nome_empresa}' por {DIAS_SIMULADOS} dias...[/bold white]",
        title=f"[bold {AZUL_CLARO}]SIMULAÇÃO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {AZUL_ESCURO}",
    )
)

fim_simulacao = datetime.now().replace(minute=0, second=0, microsecond=0)
inicio_simulacao = fim_simulacao - timedelta(days=DIAS_SIMULADOS)

regras_por_servidor = buscar_regras_alerta(ids_servidores)

arquivos_simulados = {}
total_amostras = 0
total_alertas_atuais = 0

for indice, servidor_id in enumerate(ids_servidores):
    buffer, total, criticos = simular_servidor(
        servidor_id, indice, inicio_simulacao, fim_simulacao
    )
    arquivos_simulados[servidor_id] = buffer
    total_amostras += total

    # Alerta atual = última leitura acima dos limites de parametro_alerta.
    # Se o servidor não tiver regras no banco, usa os padrões 60/90.
    regras_servidor = list(regras_por_servidor.get(servidor_id, []))
    cobertas = {
        alias.lower()
        for regra in regras_servidor
        for alias in ALIAS_METRICAS.get(regra["metrica"], {regra["metrica"]})
    }
    for nome in nomes_metricas:
        if nome.lower() not in cobertas:
            regras_servidor.append(
                {
                    "metrica": nome,
                    "componente": nome,
                    "atencao": LIMITE_ATENCAO_PADRAO,
                    "critico": LIMITE_CRITICO_PADRAO,
                }
            )

    nivel_atual, alertas_atuais, data_ultima = avaliar_alerta_atual(
        buffer, regras_servidor
    )
    if alertas_atuais:
        total_alertas_atuais += 1

    detalhes = ", ".join(
        f"{a['metrica']}={a['valor']}% (limite {a['critico']:g}%)"
        for a in alertas_atuais
    )
    cor_nivel = "bold red" if nivel_atual == "CRITICO" else "bold yellow"
    selo = (
        f" [bold {cor_nivel}]● ALERTA ATUAL: {nivel_atual}[/bold {cor_nivel}]"
        f" [dim white]{detalhes}[/dim white]" if alertas_atuais else ""
    )
    console.print(
        f"  [dim white]servidor {servidor_id}:[/dim white] {total} amostras, "
        f"{criticos} leituras acima de {LIMIAR_CRITICO}% "
        f"[dim white]({nomes_metricas[indice % len(nomes_metricas)]})[/dim white]"
        f"\n      [dim white]última leitura {data_ultima}: {nivel_atual}[/dim white]{selo}"
    )

# ============================================================
# 8. ENVIO PARA O S3 (após a simulação)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

with console.status(
    f"[bold {AZUL_CLARO}]Enviando {len(arquivos_simulados)} capturas simuladas "
    f"para a Bronze do S3...[/bold {AZUL_CLARO}]",
    spinner="dots",
):
    for servidor_id, buffer in arquivos_simulados.items():
        arquivo_s3 = (
            f"bronze/hardware/{nome_empresa}/{servidor_id}/captura_{timestamp}.csv"
        )

        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_s3,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

console.print()
console.print(
    Panel(
        f"[bold white]Capturas simuladas enviadas com sucesso![/bold white]\n\n"
        f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Empresa:[/dim white] [bold {AZUL_CLARO}]{nome_empresa}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Servidores:[/dim white] [bold {AZUL_CLARO}]{len(arquivos_simulados)}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Amostras:[/dim white] [bold {AZUL_CLARO}]{total_amostras}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Alertas na última leitura:[/dim white] "
        f"[bold red]{total_alertas_atuais} servidor(es)[/bold red]\n"
        f"[dim white]Período:[/dim white] [bold {AZUL_CLARO}]"
        f"{inicio_simulacao:%d/%m/%Y %H:%M} a {fim_simulacao:%d/%m/%Y %H:%M}[/bold {AZUL_CLARO}]",
        title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 2),
    )
)

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))
