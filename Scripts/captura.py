import psutil
import csv
from datetime import datetime
import time
import os
import socket # para pegar hostname da maquina atual. 
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
# coloque as credenciais aqui da aws 
)


# 1. CAPTURA DO HOSTNAME DA MÁQUINA ATUAL

hostname_atual = socket.gethostname()

# ============================================================
# CONEXÃO COM O BANCO DE DADOS NOVO (InfraWatch)
# ============================================================
console.print(
    Panel("[bold white]Conectando ao banco de dados InfraWatch...[/bold white]", 
          border_style=AZUL_CLARO, style=f"on {AZUL_ESCURO}")
)

bd = mysql.connect(
    host='localhost', # colocar ip public da aws; 
    user='infra_watch_captura',
    password='Urubu100',
    database='InfraWatch',
    cursorclass=mysql.cursors.DictCursor
)

cursor = bd.cursor()

# 1. Busca Equipamentos no schema novo e hostname
cursor.execute("SELECT idEquipamento, nome, hostname, fkEmpresa FROM equipamento;")
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





# print (equipamentos)


# ============================================================
# TELA INICIAL: IVALIDAÇÃO DO USUARIO 
# ============================================================


limpar_tela()
mostrar_banner()

console.print(
    Panel(
        Align.center(
            "[bold white]Bem-vindo à InfraWatch![/bold white]\n\\n"
            "[dim white]Insira suas credenciais para continuar.[/dim white] "
        ), 
        border_style=AZUL_CLARO,
        style=f"on {PRETO}",
        padding=(1, 3),

    )
)




time.sleep(1)
ipt_usuario = Prompt.ask("[bold white]Usuário ou E-mail[/bold white]")
ipt_senha = Prompt.ask("[bold white]Senha[/bold white]")

accesso = False
usuario_logado = ""

# Validação do Usuário na Tabela 'usuario' e busca do nome da Empresa

cursor.execute(
    """
    SELECT u.idUsuario, u.nome, u.email, u.senha, u.fkEmpresa, e.nome AS
    nomeEmpresa 
    FROM usuario u
    JOIN empresa e ON u.fkEmpresa = e.idEmpresa 
    WHERE (u.email = %s OR u.nome = %s) AND u.senha = %s; """, 
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
# Lista os equipamentos da empresa do usuario 
# ============================================================
cursor.execute( # busca os servidores da empresa do usuario; que não tem hostname cadastrado e as que tem o mesmo hostname do equipamento atual 
    """
    SELECT 
        idEquipamento, 
        nome, 
        hostname, 
        fkEmpresa 
    FROM equipamento 
    WHERE fkEmpresa = %s 
      AND (hostname IS NULL OR hostname = '' OR hostname = %s); 
    """,
    (fk_empresa, hostname_atual),
)
equipamentos_disponiveis = cursor.fetchall()

limpar_tela()
mostrar_banner()

console.print(
    Panel(
        f"[bold white]Olá, {nome_usuario}![/bold white]\n"
        f"[dim white]Empresa:[/dim white] [bold {AZUL_CLARO}]{nome_empresa}[/bold {AZUL_CLARO}]\n"
        f"[dim white]Hostname da máquina atual:[/dim white] [bold {AZUL_CLARO}]{hostname_atual}[/bold {AZUL_CLARO}]\n\n"
        "[bold white]Abaixo estão os equipamentos disponíveis. Escolha uma opção:[/bold white]",
        border_style=AZUL_CLARO,
        style=f"on {AZUL_ESCURO}",
    )
)

console.print("[bold yellow]0.[/bold yellow] Cadastrar um [bold green]NOVO servidor[/bold green]")

for equip, eq in enumerate(equipamentos_disponiveis, start=1):
    status_host = (
        f"(Hostname: {eq['hostname']})"
        if eq["hostname"]
        else "[Novo - Sem Hostname registrado]" # exibe na tela os servidores sem ou com host igual ao da maquina 
    )
    console.print(f"[bold yellow]{equip}.[/bold yellow] {eq['nome']} {status_host}")

opcao_equipamento = Prompt.ask(
    "\n[bold white]Digite o número do servidor desejado ou 0 para cadastrar um novo[/bold white]",
    default="0",
)
equipamento_atual = None


# ============================================================ 
# 4. VALIDAÇÃO OU CADASTRO DO SERVIDOR 
# ============================================================ 
if opcao_equipamento == "0": 
    # Opção para cadastrar um novo servidor 
    console.print("\n[bold #38BDF8]--- Cadastro de Novo Servidor ---[/bold #38BDF8]") 
    
    # Gera um nome pré-definido (ex: NomeEmpresa.server_id#X) 
    cursor.execute( 
        "SELECT COUNT(*) AS total FROM equipamento WHERE fkEmpresa = %s;", 
        (fk_empresa,), 
    ) 
    qtd_equip = cursor.fetchone()["total"] + 1 
    nome_padrao = f"{nome_empresa}.server_id#{qtd_equip}" 
    
    nome_novo_servidor = Prompt.ask( 
        "[bold white]Nome para o novo servidor[/bold white]", 
        default=nome_padrao 
    ) 
    
    cursor.execute( 
        """ 
        INSERT INTO equipamento (nome, tipo, status, hostname, fkEmpresa) 
        VALUES (%s, 'Servidor', 'Online', %s, %s); 
        """, 
        (nome_novo_servidor, hostname_atual, fk_empresa), 
    ) 
    bd.commit() 
    
    id_novo_eq = cursor.lastrowid 
    equipamento_atual = { 
        "idEquipamento": id_novo_eq, 
        "nome": nome_novo_servidor, 
        "hostname": hostname_atual, 
        "fkEmpresa": fk_empresa, 
    } 
    console.print( 
        f"[bold green]Novo servidor '{nome_novo_servidor}' cadastrado com sucesso! (ID: {id_novo_eq})[/bold green]\n" 
    ) 
else: 
    try: 
        idx_selecionado = int(opcao_equipamento) - 1 
        eq_selecionado = equipamentos_disponiveis[idx_selecionado] 
    except (ValueError, IndexError): 
        console.print( 
            "[bold red]Opção de equipamento inválida! Encerrando o script.[/bold red]" 
        ) 
        cursor.close() 
        bd.close() 
        sys.exit(1) 
    
    # Validação do Hostname 
    if eq_selecionado["hostname"] and eq_selecionado["hostname"] != hostname_atual: 
        console.print( 
            Panel( 
                f"[bold red]ERRO DE VALIDAÇÃO![/bold red]\n\n" 
                f"O servidor '[bold white]{eq_selecionado['nome']}[/bold white]' já está cadastrado com o hostname '[bold yellow]{eq_selecionado['hostname']}[/bold yellow]'.\n" 
                f"Este computador atual tem o hostname '[bold yellow]{hostname_atual}[/bold yellow]'.\n" 
                f"O script não pode ser executado para esta máquina.", 
                title="[bold red]HOSTNAME INCOMPATÍVEL[/bold red]", 
                border_style="red", 
                style=f"on {PRETO}", 
            ) 
        ) 
        cursor.close() 
        bd.close() 
        sys.exit(1) 
    
    # Se o servidor não tinha hostname gravado, vincula o hostname atual 
    if not eq_selecionado["hostname"]: 
        cursor.execute( 
            "UPDATE equipamento SET hostname = %s WHERE idEquipamento = %s;", 
            (hostname_atual, eq_selecionado["idEquipamento"]), 
        ) 
        bd.commit() 
        eq_selecionado["hostname"] = hostname_atual 
        console.print( 
            f"[bold green]Hostname '{hostname_atual}' associado com sucesso ao servidor '{eq_selecionado['nome']}'![/bold green]\n" 
        ) 
    
    equipamento_atual = eq_selecionado 


# ============================================================ 
# 5. SELEÇÃO E CONFIGURAÇÃO DE COMPONENTES E MÉTRICAS 
# ============================================================ 
componentes_selecionados_objs = [] 

if opcao_equipamento == "0": 
    cursor.execute("SELECT idComponente, nome, tipo FROM componente;") 
    componentes_catalogo = cursor.fetchall() 
    
    console.print( 
        Panel( 
            "[bold white]Configuração de Métricas para Novo Servidor[/bold white]\n" 
            "[dim white]Escolha os componentes e defina os limites de alerta desejados.[/dim white]", 
            border_style=AZUL_CLARO, 
            style=f"on {AZUL_ESCURO}", 
        ) 
    ) 
    
    for comp in componentes_catalogo: 
        if Confirm.ask( 
            f"[bold white]Deseja monitorar [bold {AZUL_CLARO}]{comp['nome']}[/bold {AZUL_CLARO}] ({comp['tipo']})?[/bold white]", 
            default=True, 
        ): 
            componentes_selecionados_objs.append(comp) 
            tipo_upper = comp["tipo"].upper() 
            
            # Nome da métrica gerado automaticamente pelo sistema (sem prompt para o cliente) 
            if tipo_upper == "CPU": 
                nome_metrica = "USO_CPU" 
                sugestao_unidade = "%" 
            elif tipo_upper == "RAM": 
                nome_metrica = "USO_RAM" 
                sugestao_unidade = "%" 
            elif tipo_upper in ["DISCO", "ARMAZENAMENTO"]: 
                nome_metrica = "USO_DISCO" 
                sugestao_unidade = "%" 
            elif tipo_upper == "REDE": 
                nome_metrica = "USO_REDE" 
                sugestao_unidade = "%" 
            elif tipo_upper == "TEMPERATURA": 
                nome_metrica = "TEMPERATURA_CPU" 
                sugestao_unidade = "°C" 
            else: 
                nome_metrica = f"USO_{tipo_upper}" 
                sugestao_unidade = "%" 
            
            console.print(f"\n[bold #38BDF8]--- Configurando Alertas: {comp['nome']} ({nome_metrica}) ---[/bold #38BDF8]") 
            
            # O cliente cadastra apenas limites e unidade de medida 
            lim_atencao = float(Prompt.ask("[bold white]Limite de Atenção[/bold white]", default="70.0")) 
            lim_critico = float(Prompt.ask("[bold white]Limite Crítico[/bold white]", default="90.0")) 
            unidade = Prompt.ask("[bold white]Unidade de medida (ex: %, GB, °C)[/bold white]", default=sugestao_unidade) 
            
            cursor.execute( 
                """ 
                INSERT INTO parametro_alerta (nomeMetrica, limite_atencao, limite_critico, unidade, ativo, fkEquipamento, fkComponente) 
                VALUES (%s, %s, %s, %s, 1, %s, %s); 
                """, 
                ( 
                    nome_metrica, 
                    lim_atencao, 
                    lim_critico, 
                    unidade, 
                    equipamento_atual["idEquipamento"], 
                    comp["idComponente"], 
                ), 
            ) 
            bd.commit() 
            console.print(f"[bold green]Métrica '{nome_metrica}' salva com sucesso! [/bold green]\n") 
else: 
    # Servidor existente: busca os parâmetros já salvos no banco de dados sem fazer perguntas no terminal 
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

cursor.close() 
bd.close()

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
