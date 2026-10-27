
import psutil
import csv
from datetime import datetime
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
# 0. CARREGANDO VARIÁVEIS DE AMBIENTE
# ============================================================
# Carrega as variáveis do .env para o ambiente
load_dotenv()

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



# ============================================================
# 1. CONEXÃO COM O S3
# ============================================================
BUCKET = "infrawatch-server-s3"

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

# Monta o cabeçalho do CSV
titulo_executados = "id;data_hora"

for comp_id in capturar_comp:
    for c in componentes:
        if comp_id == 6:
            continue
        elif comp_id == c['idComponente']:
            titulo_executados += f";{c['tipo']}"

# EM MEMÓRIA
csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer, delimiter=';')
writer.writerow(titulo_executados.split(';'))

# GERAR ARQUIVO .CSV TESTE
# with open('./dados.csv', 'w', newline='', encoding="utf-8") as csvfile:
#     writer = csv.writer(csvfile, delimiter=';')
#     writer.writerow(titulo_executados.split(';'))


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

# ============================================================
# 7. EXECUÇÃO DA CAPTURA (5 AMOSTRAS)
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
                        val = eval(c['codigo'])
                        valores_capturados.append(val)
        
        data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # EM MEMÓRIA
        writer.writerow([equipamento_atual['idEquipamento'], data_hora] + valores_capturados)

        # GERAR ARQUIVO .CSV TESTE
        # linha = [equipamento_atual['idEquipamento'], data_hora] + valores_capturados
        # with open('./dados.csv', 'a', newline='', encoding="utf-8") as csvfile:
        #     writer = csv.writer(csvfile, delimiter=';')
        #     writer.writerow(linha)

        
        time.sleep(1)
        progress.advance(task)


# ============================================================
# 8. ENVIO DO ARQUIVO CSV PARA O S3 (BRONZE)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# -- COMENTAR PARA TESTE
nome_arquivo = f"captura_{timestamp}.csv"

# Exemplo de caminho: bronze/InfraWatch/1/captura_2026-10-01_19-45-00.csv
arquivo_s3 = f"bronze/hardware/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo}"

with console.status(f"[bold {AZUL_CLARO}]Enviando captura CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
    s3.put_object(
        Bucket=BUCKET,
        Key=arquivo_s3,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

nome_arquivo_processos = f"captura_processos_{timestamp}.csv"
arquivo_processos_s3 = f"bronze/processo/{nome_empresa}/{equipamento_atual['idEquipamento']}/{nome_arquivo_processos}"

if (enviar_processos) :
    with console.status(f"[bold {AZUL_CLARO}]Enviando processos CSV para a zona Bronze do S3...[/bold {AZUL_CLARO}]", spinner="dots"):
        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_processos_s3,
            Body=csv_processos.getvalue().encode("utf-8"),
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
# -- FIM COMENTAR PARA TESTE

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))