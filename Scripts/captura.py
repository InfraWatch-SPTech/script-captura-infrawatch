import psutil
import csv
from datetime import datetime
import time
import json
import os
from getpass import getpass
import boto3
import io
import pymysql as mysql

# Rich
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn
)
from rich.prompt import Prompt, Confirm
from rich.text import Text
from rich.align import Align
from rich.rule import Rule
from rich import box


# ============================================================
# CONFIGURAÇÃO VISUAL
# ============================================================

console = Console()

AZUL_ESCURO = "#0B1F3A"
AZUL_CLARO = "#38BDF8"
PRETO = "#000000"
BRANCO = "#FFFFFF"
CINZA = "#A7B0BA"


# ============================================================
# BANNER
# ============================================================

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

s3 = boto3.client(
    "s3",

)


# ============================================================
# NOME DO BUCKET
# ============================================================

BUCKET = "infrawatch-server-s3"


# ============================================================
# CONEXÃO COM BANCO DE DADOS
# ============================================================

console.print(
    Panel(
        "[bold white]Conectando ao banco de dados...[/bold white]",
        border_style=AZUL_CLARO,
        style=f"on {AZUL_ESCURO}"
    )
)

bd = mysql.connect(
    host='localhost',
    user='infra_watch_captura',
    password='Urubu100',
    database='InfraWatch',
    cursorclass=mysql.cursors.DictCursor
)


# ============================================================
# EXECUTANDO CONSULTAS
# ============================================================

cursor = bd.cursor()

# EQUIPAMENTOS
cursor.execute(
    "SELECT id, codigo, fk_empresa FROM equipamento;"
)
equipamentos = cursor.fetchall()

# COMPONENTES
cursor.execute("SELECT id, codigo, nome, medida FROM componente;")
componentes = cursor.fetchall()

# EQUIPAMENTO_COMPONENTE
cursor.execute(
    "SELECT fk_componente, fk_equipamento FROM equipamento_componente;"
)
equip_comp = cursor.fetchall()

# FUNCIONARIOS
cursor.execute("SELECT nome, senha FROM usuario;")
funcionarios = cursor.fetchall()

# EMPRESAS
cursor.execute("SELECT id, razao_social FROM empresa;")
empresas = cursor.fetchall()


# ============================================================
# FECHANDO CONEXÃO
# ============================================================

cursor.close()
bd.close()


# ============================================================
# TELA INICIAL
# ============================================================

limpar_tela()

mostrar_banner()

console.print(
    Align.center(
        f"""
[bold white]Olá, usuário.[/bold white]

[bold {AZUL_CLARO}]Bem-vindo à configuração do seu ambiente InfraWatch![/bold {AZUL_CLARO}]

[dim white]Para continuar, informe o código de série do seu equipamento.[/dim white]
"""
    )
)

console.print()

equipamento_atual = Prompt.ask(
    "[bold white]Código de série do equipamento[/bold white]"
)


# ============================================================
# LOCALIZAÇÃO DO EQUIPAMENTO
# ============================================================

equipamento_cadastrado = False

for i in range(len(equipamentos)):

    if equipamento_atual == str(equipamentos[i]['codigo']):

        equipamento_cadastrado = True
        equipamento_atual = equipamentos[i]
        if equipamento_cadastrado:

            nome_empresa = ""

    for empresa in empresas:

        if empresa["id"] == equipamento_atual["fk_empresa"]:

            nome_empresa = empresa["razao_social"]
            break


if not equipamento_cadastrado:

    console.print(
        Panel(
            "[bold white]Equipamento não encontrado no sistema![/bold white]",
            title="[bold #38BDF8]InfraWatch[/bold #38BDF8]",
            border_style=AZUL_CLARO,
            style=f"on {AZUL_ESCURO}"
        )
    )

else:

    # ========================================================
    # AUTENTICAÇÃO
    # ========================================================

    limpar_tela()

    mostrar_banner()

    console.print(
        Panel(
            Align.center(
                "[bold white]Bem-vindo à configuração do seu ambiente InfraWatch![/bold white]\n\n"
                f"[dim white]Equipamento:[/dim white] "
                f"[bold {AZUL_CLARO}]{equipamento_atual['codigo']}[/bold {AZUL_CLARO}]\n\n"
                "[dim white]Para continuar, insira suas credenciais.[/dim white]"
            ),
            border_style=AZUL_CLARO,
            style=f"on {PRETO}",
            padding=(1, 3)
        )
    )

    time.sleep(1)

    ipt_usuario = Prompt.ask(
        "[bold white]Usuário[/bold white]"
    )

    ipt_senha = getpass(
        "Senha: "
    )

    limpar_tela()

    # ========================================================
    # CARREGAMENTO
    # ========================================================

    with console.status(
        f"[bold {AZUL_CLARO}]Validando credenciais...[/bold {AZUL_CLARO}]",
        spinner="dots"
    ):

        time.sleep(1)

        acesso = False

        for i in range(len(funcionarios)):

            if (
                ipt_usuario == funcionarios[i]["nome"]
                and
                ipt_senha == funcionarios[i]["senha"]
            ):

                acesso = True
                usuario = funcionarios[i]["nome"]


    # ========================================================
    # ACESSO NEGADO
    # ========================================================

    if not acesso:

        console.print()

        console.print(
            Panel(
                "[bold white]Você não pode acessar nosso serviço.[/bold white]",
                title="[bold #38BDF8]ACESSO NEGADO[/bold #38BDF8]",
                border_style=AZUL_CLARO,
                style=f"on {PRETO}"
            )
        )

        os._exit(0)


    # ========================================================
    # USUÁRIO AUTENTICADO
    # ========================================================

    limpar_tela()

    mostrar_banner()

    console.print(
        Panel(
            f"[bold white]Olá, {usuario}![/bold white]\n\n"
            "[dim white]"
            "As informações da sua máquina serão coletadas automaticamente."
            "[/dim white]",
            title=f"[bold {AZUL_CLARO}]InfraWatch[/bold {AZUL_CLARO}]",
            border_style=AZUL_CLARO,
            style=f"on {AZUL_ESCURO}"
        )
    )

    console.print()

    continuar = Confirm.ask(
        "[bold white]Deseja continuar?[/bold white]",
        default=True
    )


    # ========================================================
    # IDENTIFICA COMPONENTES DO EQUIPAMENTO
    # ========================================================

    capturar_comp = []

    for i in range(len(equip_comp)):

        if (
            equip_comp[i]['fk_equipamento']
            ==
            equipamento_atual['id']
        ):

            capturar_comp.append(
                equip_comp[i]['fk_componente']
            )


    # ========================================================
    # INÍCIO DA CAPTURA
    # ========================================================

    if continuar:

        limpar_tela()

        mostrar_banner()

        console.print(
            Panel(
                "[bold white]Preparando captura dos dados...[/bold white]",
                title=f"[bold {AZUL_CLARO}]CAPTURA[/bold {AZUL_CLARO}]",
                border_style=AZUL_CLARO,
                style=f"on {AZUL_ESCURO}"
            )
        )

        console.print()

        titulo_executados = 'id;data_hora'


        # ====================================================
        # MONTA CABEÇALHO DO CSV
        # ====================================================

        for i in range(len(capturar_comp)):

            for j in range(len(componentes)):

                if capturar_comp[i] == componentes[j]['id']:

                    nome_comp = componentes[j]['nome']
                    medida_comp = componentes[j]['medida']

                    titulo_executados += f';{nome_comp}'


        # ====================================================
        # CSV NA MEMÓRIA
        # ====================================================

        csv_buffer = io.StringIO()

        writer = csv.writer(
            csv_buffer,
            delimiter=';'
        )

        writer.writerow(
            titulo_executados.split(';')
        )


        # ====================================================
        # CAPTURA
        # ====================================================

        with Progress(

            TextColumn(
                f"[bold {AZUL_CLARO}]Capturando[/bold {AZUL_CLARO}]"
            ),

            BarColumn(
                complete_style=AZUL_CLARO,
                finished_style=AZUL_CLARO
            ),

            TextColumn(
                "[bold white]{task.percentage:>3.0f}%[/bold white]"
            ),

            TimeElapsedColumn(),

            console=console

        ) as progress:

            task = progress.add_task(
                "captura",
                total=5
            )

            for i in range(0, 5):

                executados = ''

                for k in range(len(capturar_comp)):

                    for j in range(len(componentes)):

                        if (
                            capturar_comp[k]
                            ==
                            componentes[j]['id']
                        ):

                            codigo_comp = componentes[j]['codigo']

                            executados += (
                                f';{eval(codigo_comp)}'
                            )


                # ============================================
                # DATA / HORA
                # ============================================

                date = datetime.now()

                data_hora = date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )


                # ============================================
                # ESCREVE CAPTURA NO CSV
                # ============================================

                writer.writerow(
                    [
                        equipamento_atual['id'],
                        data_hora
                    ]
                    +
                    executados
                    .lstrip(';')
                    .split(';')
                )


                time.sleep(1)

                progress.advance(task)


        # ====================================================
        # FINALIZAÇÃO
        # ====================================================

        console.print()

        console.print(
            Panel(
                "[bold white]"
                "Captura finalizada com sucesso."
                "[/bold white]",
                title=f"[bold {AZUL_CLARO}]CONCLUÍDO[/bold {AZUL_CLARO}]",
                border_style=AZUL_CLARO,
                style=f"on {AZUL_ESCURO}"
            )
        )


        # ====================================================
        # NOME DO ARQUIVO
        # ====================================================

        timestamp = datetime.now().strftime(
            "%Y-%m-%d_%H-%M-%S"
        )

        nome_arquivo = f"captura_{timestamp}.csv"

        
        # ====================================================
        # CAMINHO DO S3
        # ====================================================

        arquivo_s3 = (
            f"bronze/"
            f"{nome_empresa}/"
            f"{equipamento_atual['codigo']}/"
            f"{nome_arquivo}"
        )


        # ====================================================
        # ENVIA CSV PARA O S3
        # ====================================================

        with console.status(
            f"[bold {AZUL_CLARO}]Enviando captura para o S3...[/bold {AZUL_CLARO}]",
            spinner="dots"
        ):

            s3.put_object(
                Bucket=BUCKET,
                Key=arquivo_s3,
                Body=csv_buffer.getvalue().encode("utf-8"),
                ContentType="text/csv"
            )


        # ====================================================
        # SUCESSO
        # ====================================================

        console.print()

        console.print(
            Panel(
                f"[bold white]"
                "Captura enviada para o S3 com sucesso!"
                "[/bold white]\n\n"
                f"[dim white]Bucket:[/dim white] "
                f"[bold {AZUL_CLARO}]{BUCKET}[/bold {AZUL_CLARO}]\n"
                f"[dim white]Arquivo:[/dim white] "
                f"[bold {AZUL_CLARO}]{arquivo_s3}[/bold {AZUL_CLARO}]",
                title=f"[bold {AZUL_CLARO}]UPLOAD CONCLUÍDO[/bold {AZUL_CLARO}]",
                border_style=AZUL_CLARO,
                style=f"on {PRETO}",
                padding=(1, 2)
            )
        )

        console.print()

        console.print(
            Align.center(
                "[dim white]"
                "Obrigada por escolher a InfraWatch!"
                "[/dim white]"
            )
        )


    # ========================================================
    # USUÁRIO NÃO QUER CONTINUAR
    # ========================================================

    else:

        console.print()

        console.print(
            Panel(
                "[bold white]Fechando script...[/bold white]",
                title=f"[bold {AZUL_CLARO}]InfraWatch[/bold {AZUL_CLARO}]",
                border_style=AZUL_CLARO,
                style=f"on {PRETO}"
            )
        )

        os._exit(0)