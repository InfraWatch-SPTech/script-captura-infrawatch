import psutil
import csv
from datetime import datetime
import time
import os
#from getpass import getpass
import boto3
import io
import pymysql as mysql

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
[bold {AZUL_CLARO}]██╗   ██╗ ██╗   ██╗[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]██║   ██║ ██║   ██║[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]██║   ██║ ██║   ██║[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]╚██╗ ██╔╝ ╚██╗ ██╔╝[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}] ╚████╔╝   ╚████╔╝[/bold {AZUL_CLARO}]
[bold {AZUL_CLARO}]  ╚═══╝     ╚═══╝[/bold {AZUL_CLARO}]
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
# CONEXÃO COM O S3
# ============================================================
BUCKET = "infrawatch-server-s3"

s3 = boto3.client(
"s3",
aws_access_key_id=,
aws_secret_access_key=
aws_session_token=
)



# ============================================================
# CONEXÃO COM O BANCO DE DADOS NOVO (InfraWatch)
# ============================================================
console.print(
    Panel("[bold white]Conectando ao banco de dados InfraWatch...[/bold white]", 
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)

bd = mysql.connect(
    host='localhost',
    user='infra_watch_captura',
    password='Urubu100',
    database='InfraWatch',
    cursorclass=mysql.cursors.DictCursor
)

cursor = bd.cursor()

# 1. Busca Equipamentos no schema novo
cursor.execute("SELECT idEquipamento, nome, fkEmpresa FROM equipamento;")
equipamentos = cursor.fetchall()

# 2. Busca Componentes no schema novo
cursor.execute("SELECT idComponente, nome, tipo FROM componente;")
componentes = cursor.fetchall()

# 3. Busca Mapeamento Equipamento-Componente via parametro_alerta
cursor.execute("SELECT DISTINCT fkComponente, fkEquipamento FROM parametro_alerta WHERE ativo = 1;")
equip_comp = cursor.fetchall()

# 4. Busca Usuários
cursor.execute("SELECT nome, email, senha, fkEmpresa FROM usuario;")
funcionarios = cursor.fetchall()

# 5. Busca Empresas
cursor.execute("SELECT idEmpresa, nome FROM empresa;")
empresas = cursor.fetchall()

cursor.close()
bd.close()

# ============================================================
# TELA INICIAL: IDENTIFICAÇÃO DO EQUIPAMENTO
# ============================================================
limpar_tela()
mostrar_banner()
console.print(
    Align.center(
        f"""
        [bold white]Olá, usuário.[/bold white]
        [bold {AZUL_CLARO}]Bem-vindo à configuração do seu ambiente InfraWatch![/bold {AZUL_CLARO}]
        [dim white]Para continuar, informe o ID ou Nome do seu equipamento.[/dim white]
        """
    )
)
console.print()

equipamento_input = Prompt.ask("[bold white]ID ou Nome do equipamento[/bold white]")

equipamento_atual = None
equipamento_cadastrado = False

for eq in equipamentos:
    if equipamento_input == str(eq['idEquipamento']) or equipamento_input.lower() == str(eq['nome']).lower():
        equipamento_cadastrado = True
        equipamento_atual = eq
        break

if not equipamento_cadastrado:
    console.print(
        Panel(
            "[bold white]Equipamento não encontrado no sistema InfraWatch![/bold white]",
            title="[bold #38BDF8]InfraWatch[/bold #38BDF8]",
            border_style=AZUL_CLARO,
            style=f"on {AZUL_ESCURO}"
        )
    )
    os._exit(0)

nome_empresa = ""
for emp in empresas:
    if emp["idEmpresa"] == equipamento_atual["fkEmpresa"]:
        nome_empresa = emp["nome"]
        break

# ============================================================
# AUTENTICAÇÃO DO USUÁRIO
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel(
        Align.center(
            "[bold white]Bem-vindo à configuração do seu ambiente InfraWatch![/bold white]\n\n"
            f"[dim white]Equipamento:[/dim white] [bold {AZUL_CLARO}]{equipamento_atual['nome']} (ID: {equipamento_atual['idEquipamento']})[/bold {AZUL_CLARO}]\n"
            f"[dim white]Empresa:[/dim white] [bold {AZUL_CLARO}]{nome_empresa}[/bold {AZUL_CLARO}]\n\n"
            "[dim white]Para continuar, insira suas credenciais.[/dim white]"
        ),
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 3)
    )
)

time.sleep(1)
ipt_usuario = Prompt.ask("[bold white]Usuário ou E-mail[/bold white]")
ipt_senha = Prompt.ask("[bold white]Senha[/bold white]")

accesso = False
usuario_logado = ""

for func in funcionarios:
    if (ipt_usuario == func["nome"] or ipt_usuario == func["email"]) and ipt_senha == func["senha"]:
        accesso = True
        usuario_logado = func["nome"]
        break

if not accesso:
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

# ============================================================
# CONFIRMAÇÃO DE INÍCIO DA CAPTURA
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel(
        f"[bold white]Olá, {usuario_logado}![/bold white]\n\n"
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
# IDENTIFICA COMPONENTES DO EQUIPAMENTO
# ============================================================
capturar_comp = []
for ec in equip_comp:
    if ec['fkEquipamento'] == equipamento_atual['idEquipamento']:
        capturar_comp.append(ec['fkComponente'])

# ============================================================
# MONTAGEM DO CSV EM MEMÓRIA
# ============================================================
limpar_tela()
mostrar_banner()

console.print(
    Panel("[bold white]Preparando captura dos dados...[/bold white]",
          title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)
console.print()

# Monta o cabeçalho do CSV
titulo_executados = "id;data_hora"

for comp_id in capturar_comp:
    for c in componentes:
        if comp_id == c['idComponente']:
            titulo_executados += f";{c['nome']}"

csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer, delimiter=';')
writer.writerow(titulo_executados.split(';'))

# ============================================================
# EXECUÇÃO DA CAPTURA (5 AMOSTRAS)
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
            for c in componentes:
                if comp_id == c['idComponente']:
                    tipo_comp = c['tipo'].upper()
                    if tipo_comp == 'CPU':
                        val = psutil.cpu_percent(interval=1)
                    elif tipo_comp == 'RAM':
                        val = psutil.virtual_memory().percent
                    elif tipo_comp in ['ARMAZENAMENTO', 'DISCO']:
                        val = psutil.disk_usage("/").percent
                    else:
                        val = 0.0
                    valores_capturados.append(str(val))

        data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Escreve a linha no CSV
        writer.writerow([equipamento_atual['idEquipamento'], data_hora] + valores_capturados)

        time.sleep(1)
        progress.advance(task)

console.print()
console.print(
    Panel("[bold white]Captura finalizada com sucesso.[/bold white]",
          title=f"[bold {AZUL_CLARO}]CONCLUÍDO[/bold {AZUL_CLARO}]",
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)

# ============================================================
# ENVIO DO ARQUIVO CSV PARA O S3 (BRONZE)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
nome_arquivo = f"captura_{timestamp}.csv"

# Exemplo de caminho: bronze/InfraWatch/1/captura_2026-10-01_19-45-00.csv
arquivo_s3 = f"bronze/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo}"

with console.status(f"[bold {AZUL_CLARO}]Enviando captura CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
    s3.put_object(
        Bucket=BUCKET,
        Key=arquivo_s3,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

console.print()
console.print(
    Panel(
        f"[bold white]Captura enviada para o S3 com sucesso![/bold white]\n\n"
        f"[dim white]Bucket:[/dim white] [bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Caminho Bronze:[/dim white] [bold {AZUL_CLARO}]{arquivo_s3}[/bold {AZUL_CLARO}]",
        title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 2)
    )
)

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))
