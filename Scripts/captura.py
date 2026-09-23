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


#  conexão com o S3
s3 = boto3.client(
    "s3",
)
    

#  nome do bucket
BUCKET = "infrawatch-server-s3"



# Criando conexção com banco de dados
bd = mysql.connect(
    host='LOCAL',
    user='USER',
    password='USER_PASSWORD',
    database='DATABASE',
    cursorclass=mysql.cursors.DictCursor # As consultas retornam um dicionário
)

# Executando consulta e guardando em variáveis

# EQUIPAMENTOS
cursor = bd.cursor()
cursor.execute("SELECT id, codigo FROM equipamento;")
equipamentos = cursor.fetchall()

# COMPONENTES
cursor.execute("SELECT id, codigo, nome, medida FROM componente;")
componentes = cursor.fetchall()

# EQUIPAMENTO_COMPONENTE
cursor.execute("SELECT fk_componente, fk_equipamento FROM equipamento_componente;")
equip_comp = cursor.fetchall()

# FUNCIONARIOS
cursor.execute("SELECT nome, senha FROM usuario;")
funcionarios = cursor.fetchall()

# EMPRESAS
cursor.execute("SELECT id, razao_social FROM empresa;")
empresas = cursor.fetchall()

# Fechando conexão
cursor.close()
bd.close()



os.system('cls' if os.name == 'nt' else 'clear')

equipamento_atual = str(input("Digite o código de série do seu equipamento:"))

equipamento_cadastrado = False

for i in range(len(equipamentos)):
    if equipamento_atual == str(equipamentos[i]['codigo']):
        equipamento_cadastrado = True
        equipamento_atual = equipamentos[i]

if not equipamento_cadastrado:

    print("Equipamento não encontrado no sistema! =[")

else:

    print("""
 ██╗   ██╗ ██╗   ██╗
 ██║   ██║ ██║   ██║
 ██║   ██║ ██║   ██║
 ╚██╗ ██╔╝ ╚██╗ ██╔╝
  ╚████╔╝   ╚████╔╝
   ╚═══╝     ╚═══╝

Olá usuário.
Bem-vindo à configuração do seu ambiente InfraWatch!
Para continuar, por favor insira suas credenciais:
    """)

    time.sleep(1)

    ipt_usuario = input("Insira seu usuário:")
    ipt_senha = getpass("Insira sua senha:")

    os.system('cls' if os.name == 'nt' else 'clear')

    print("""
Carregando script...""")

    acesso = False

    for i in range(len(funcionarios)):
        if ipt_usuario == funcionarios[i]["nome"] and ipt_senha == funcionarios[i]["senha"]:
            acesso = True
            usuario = funcionarios[i]["nome"]

    if not acesso:

        print("Você não pode acessar nosso serviço =[")
        os._exit(0)

    print(
        f"""Olá {usuario}! As informações da sua máquina serão coletadas automaticamente."""
    )

    continuar = input("Deseja continuar? (s/n)")

    capturar_comp = []

    for i in range(len(equip_comp)):
        if equip_comp[i]['fk_equipamento'] == equipamento_atual['id']:
            capturar_comp.append(equip_comp[i]['fk_componente'])

    if continuar == 's':

        print('')

        titulo_executados = 'id;data_hora'

        for i in range(len(capturar_comp)):
            for j in range(len(componentes)):
                if capturar_comp[i] == componentes[j]['id']:

                    nome_comp = componentes[j]['nome']
                    medida_comp = componentes[j]['medida']

                    titulo_executados += f';{nome_comp}'

# ---------------------------- TESTE --------------------------------
        # Código que acha a pasta indicada e cria o arquivo csv
        # with open('./arquivo.csv', 'w', newline='', encoding="utf-8") as csvfile:

        #     writer = csv.writer(csvfile)
        #     writer.writerow([f"{titulo_executados}"])
# ---------------------------- TESTE --------------------------------

        # cria o CSV na memória, sem gerar arquivo local
        csv_buffer = io.StringIO()

        #  cria o escritor do CSV
        writer = csv.writer(csv_buffer, delimiter=';')

        # adiciona o cabeçalho ao CSV
        writer.writerow(titulo_executados.split(';'))

        qtd = 0
        carregamento = ""

        for i in range(0, 5):

            executados = ''

            for k in range(len(capturar_comp)):

                for j in range(len(componentes)):

                    if capturar_comp[k] == componentes[j]['id']:

                        codigo_comp = componentes[j]['codigo']

                        executados += f';{eval(codigo_comp)}'

            os.system('cls' if os.name == 'nt' else 'clear')

            carregamento = ""

            qtd += 1

            for j in range(1, 5):

                if qtd <= j:
                    carregamento += "----"
                else:
                    carregamento += "◻◻◻◻"

            print("""
 ██╗   ██╗ ██╗   ██╗
 ██║   ██║ ██║   ██║
 ██║   ██║ ██║   ██║
 ╚██╗ ██╔╝ ╚██╗ ██╔╝
  ╚████╔╝   ╚████╔╝
   ╚═══╝     ╚═══╝

Executando simulação de captura.
Capturando dados
            """)

            print("[", carregamento, "]      ", (qtd * 20), "%")

            date = datetime.now()
            data_hora = date.strftime("%Y-%m-%d %H:%M:%S")

            #  adiciona a captura ao CSV que está na memória
            writer.writerow(
                [equipamento_atual['id'], data_hora]
                + executados.lstrip(';').split(';')
            )

# ---------------------------- TESTE --------------------------------
            # # Passando os parâmetros para a escrita no arquivo csv
            # with open('./arquivo.csv', 'a', encoding="utf-8") as csvfile:

            #     writer = csv.writer(csvfile)
            #     writer.writerow([f"{equipamento_atual['id']};{data_hora}{executados}"]) # Exibe equipamento, data e os valores guardados
# ---------------------------- TESTE --------------------------------

        print("Finalizando a captura. Obrigada por escolher a InfraWatch! =]")

        # cria um nome único para cada captura
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nome_arquivo = f"captura_{timestamp}.csv"

        #  define o caminho do arquivo dentro do S3
        arquivo_s3 = f"bronze/{equipamento_atual['id']}/{nome_arquivo}"

        #  envia o CSV diretamente da memória para o S3
        s3.put_object(
            Bucket=BUCKET,
            Key=arquivo_s3,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )

        #  informa onde o arquivo foi armazenado
        print("Captura enviada para o S3 com sucesso!")
        print(f"s3://{BUCKET}/{arquivo_s3}")

    elif continuar == 'n':

        print("Fechando script...")
        os._exit(0)