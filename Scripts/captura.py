import psutil
import csv
from datetime import datetime
import time
import json
import os
from getpass import getpass
import boto3
import io


#  conexão com o S3
s3 = boto3.client(
    "s3",
    

#  nome do bucket
BUCKET = "infrawatch-server-s3"


with open('./dados/funcionarios.js', 'r', encoding='utf-8') as jsonfile:
    funcionarios = json.load(jsonfile)

with open('./dados/equipamentos.js', 'r', encoding='utf-8') as jsonfile:
    equipamentos = json.load(jsonfile)

with open('./dados/componentes.js', 'r', encoding='utf-8') as jsonfile:
    componentes = json.load(jsonfile)

with open('./dados/equip-comp.js', 'r', encoding='utf-8') as jsonfile:
    equip_comp = json.load(jsonfile)

os.system('cls' if os.name == 'nt' else 'clear')

equipamento_atual = str(input("Digite o código de série do seu equipamento:"))

equipamento_cadastrado = False

for i in range(len(equipamentos)):
    if equipamento_atual == equipamentos[i]["codigo"]:
        equipamento_cadastrado = True

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
        if ipt_usuario == funcionarios[i]["usuario"] and ipt_senha == funcionarios[i]["senha"]:
            acesso = True
            usuario = funcionarios[i]["usuario"]

    if not acesso:

        print("Você não pode acessar nosso serviço =[")
        os._exit(0)

    print(
        f"""Olá {usuario}! As informações da sua máquina serão coletadas automaticamente."""
    )

    continuar = input("Deseja continuar? (s/n)")

    capturar_comp = []

    for i in range(len(equip_comp)):
        if equip_comp[i]['fk_equipamento'] == equipamento_atual:
            capturar_comp.append(equip_comp[i]['fk_componente'])

    if continuar == 's':

        print('')

        titulo_executados = 'id;data_hora'

        for i in range(len(capturar_comp)):
            for j in range(len(componentes)):
                if capturar_comp[i] == componentes[j]['id_componente']:

                    nome_comp = componentes[j]['nome']
                    medida_comp = componentes[j]['medida']

                    titulo_executados += f';{nome_comp}'

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

                    if capturar_comp[k] == componentes[j]['id_componente']:

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
                [equipamento_atual, data_hora]
                + executados.lstrip(';').split(';')
            )

        print("Finalizando a captura. Obrigada por escolher a BioTrace! =]")

        # cria um nome único para cada captura
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nome_arquivo = f"captura_{timestamp}.csv"

        #  define o caminho do arquivo dentro do S3
        arquivo_s3 = f"bronze/{equipamento_atual}/{nome_arquivo}"

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