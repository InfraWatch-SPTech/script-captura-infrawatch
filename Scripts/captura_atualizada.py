
import psutil
import csv
from datetime import datetime
import time
import os
import uuid
from getpass import getpass
import boto3
import io
import pymysql as mysql
import sys

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



# 1. CAPTURA DO idMac DA MÁQUINA ATUAL
id_mac = uuid.getnode()

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








# TELA INICIAL: VALIDAÇÃO DO USUARIO 



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



cadastrar = True


for i in range(len(equipamentos)) :
    if (equipamentos[i]['idMac'] == str(id_mac)) :
        cadastrar = False
        equipamento_atual = {
            "idEquipamento": equipamentos[i]['idEquipamento'],
            "nome": equipamentos[i]['nome'],
            "idMac": id_mac,
            "fkEmpresa": equipamentos[i]['fkEmpresa']
        }


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
        VALUES ('USE_DISCO', 60.0, 90.0, '%%', 1, %s, 3)
        """
    cursor.execute(sql, (equipamento_atual["idEquipamento"], ))
    bd.commit() 
    console.print(f"[bold green]Métrica PROCESSOS_ATIVOS salva com sucesso! [/bold green]\n") 

# 3. Busca Mapeamento Equipamento-Componente via parametro_alerta
# É necessário fazer essa query aqui para que a tabela venha atualizada
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
            titulo_executados += f";{c['tipo']}"

with open('./dados.csv', 'w', newline='', encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile, delimiter=';')
    writer.writerow(titulo_executados.split(';'))


# ============================================================
# FUNÇÃO PARA CAPTURAR PROCESSOS E GERAR .CSV
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
            
            # MUDANÇA 1: Passar como lista [...] em vez de dicionário {...}
            infos_processos.append([
                proc.info['pid'],
                proc.info['name'],
                proc_ram,
                proc_tam_mb,
                proc_path
            ])
        except Exception:
            pass
            
    with open('./dados_processos.csv', 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(['PID', 'Processo', 'RAM', 'TAMANHO', 'CAMINHO'])
    
    # MUDANÇA 2: 'writer' com w minúsculo (estava Writer)
    with open('./dados_processos.csv', 'a', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerows(infos_processos)
    
    # MUDANÇA 3: Retornar a variável para ela existir fora da função
    return csvfile

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
            if comp_id == 6:
                csv_processos = capturar_processos()
                enviar_processos = True
            else:
                for c in componentes:
                    if comp_id == c['idComponente']:
                        val = eval(c['codigo'])
                        valores_capturados.append(val)
        
        data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Escreve a linha completa com ID, data_hora e todas as métricas capturadas
        linha = [equipamento_atual['idEquipamento'], data_hora] + valores_capturados
        with open('./dados.csv', 'a', newline='', encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(linha)
        time.sleep(1)
        progress.advance(task)


# ============================================================
# ENVIO DO ARQUIVO CSV PARA O S3 (BRONZE)
# ============================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

console.print(Align.center("[dim white]Obrigada por escolher a InfraWatch![/dim white]"))