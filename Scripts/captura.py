# -- IMPORTANDO BIBLIOTECAS ---------------------------------------

import psutil # para capturar dados do pc
import csv # para o csv
from datetime import datetime # para o timestamp
import time # para o time sleep
import json # para ler o JSON
import os # para limpar o terminal

# -----------------------------------------------------------------

# 
# 

# -- TRANSFORMANDO JSON PARA LEITURA DO PYTHON --------------------

with open ('./dados/empresas.js', 'r', encoding='utf-8') as jsonfile: # Mudar endereço para o JSON do BD
    empresas = json.load(jsonfile)

with open ('./dados/funcionarios.js', 'r', encoding='utf-8') as jsonfile: # Mudar endereço para o JSON do BD
    funcionarios = json.load(jsonfile)

with open ('./dados/equipamentos.js', 'r', encoding='utf-8') as jsonfile: # Mudar endereço para o JSON do BD
    equipamentos = json.load(jsonfile)

with open ('./dados/componentes.js', 'r', encoding='utf-8') as jsonfile: # Mudar endereço para o JSON do BD
    componentes = json.load(jsonfile)

with open ('./dados/equip-comp.js', 'r', encoding='utf-8') as jsonfile: # Mudar endereço para o JSON do BD
    equip_comp = json.load(jsonfile)

# -----------------------------------------------------------------

# 
# 

# -- IDENTIFICAÇÃO DO USUÁRIO -------------------------------------

# Definindo componente atual
equipamento_atual = '010101'

print("""

    █████             ████
    █████           ██   █    
    █████          ██    █
    █████          ████████████
    █████ █████    ██████████  █
    █████████████  ████████████
    █████████████  ███████
    █████████████  ███████
    █████ █████   ████████
                █████████
            █████     ██
            █       ██
            ████████

Olá usuário.
Bem-vindo à configuração do seu ambiente BioTrace!
Para continuar, por favor insira suas credenciais:
""")

time.sleep(1)

# Solicitando usuario e senha para acessar o script
ipt_usuario = input("Insira seu usuário:")
ipt_senha = input("Insira sua senha:")

os.system('cls' if os.system == 'nt' else 'clear')

print("""
Carregando...
""")

# For que verifica se usuario e senha estao cadastrados, se sim libera o acesso
acesso = False
for i in range(len(funcionarios)) :
    if (ipt_usuario == funcionarios[i]["usuario"] and ipt_senha == funcionarios[i]["senha"]):
        acesso = True
        empresa = empresas[funcionarios[i]["fk_empresa"]]["razao-social"]
        usuario = funcionarios[i]["usuario"]

# Se o usuário é cadastrado continua o processo, se não finaliza a execução
if (not acesso) :
    print("Você não pode acessar nosso serviço =[")
    os._exit(0)

print(f"""
Olá {usuario}! As informações da sua máquina serão coletadas automaticamente.
""")

continuar = input("Deseja continuar? (s/n)")

# -----------------------------------------------------------------

# 
# 

# -- CAPTURA DAS INFORMAÇÕES --------------------------------------
# For que identifica os componentes a serem monitorados de determinado equipamento
capturar_comp = [];
for i in range(len(equip_comp)):
    if (equip_comp[i]['fk_equipamento'] == equipamento_atual) :
            capturar_comp.append(equip_comp[i]['fk_componente'])

# Se o usuario deseja continuar o processo, a leitura é iniciada
if continuar == 's':
    print('')
    titulo_executados = ''
    for i in range(len(capturar_comp)):
                for j in range(len(componentes)):
                    if (capturar_comp[i] == componentes[j]['id_componente']) :
                        titulo_executados += f';{componentes[j]['nome']}' # Adiciona os nomes em uma string, delimitados por ';'

    # Código que acha a pasta indicada e cria o arquivo csv
    with open('./arquivo.csv', 'w', newline='', encoding="utf-8") as csvfile:

        writer = csv.writer(csvfile)
        writer.writerow([f"id;data_hora{titulo_executados}"])


    # For que captura os dados 10 vezes com intervalo de 10 segundos para cada leitura
    for i in range(0, 10):
        executados = '' # Reseta os valores

        for i in range(len(capturar_comp)):
            for j in range(len(componentes)):
                if (capturar_comp[i] == componentes[j]['id_componente']) :
                    executados += f';{eval(componentes[j]['codigo'])}' # Adiciona os valores em uma string, delimitados por ';'

        print(executados)
        #Coletando data e hora
        date = datetime.now()
        data_hora = date.strftime("%Y-%m-%d %H:%M:%S")

        # Passando os parâmetros para a escrita no arquivo csv
        with open('./arquivo.csv', 'a', encoding="utf-8") as csvfile:

            writer = csv.writer(csvfile)
            writer.writerow([f"{equipamento_atual};{data_hora}{executados}"]) # Exibe equipamento, data e os valores guardados

elif continuar == 'n':
    print("Fechando script...")
    os._exit(0)

# -----------------------------------------------------------------