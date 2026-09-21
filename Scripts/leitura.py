import csv
from datetime import datetime

timestamp = []
cpu = []
ram = []
disco = []
user = []

for i in range (len(nomes)):

    primeira = True

    with open(f"./arquivo.csv", newline='') as dados:
        reader = csv.reader(dados, delimiter=';')

        for row in reader:

            # Pula o cabeçalho
            if not primeira:
                user.append(row[0])
                timestamp.append(row[1])
                cpu.append(row[2])
                ram.append(row[3])
                disco.append(row[4])

            primeira = False


# Convertendo números
for i in range(len(ram)):
    timestamp[i] = datetime.strptime(f"{timestamp[i]}", "%Y-%m-%d %H:%M:%S")
    cpu[i] = float(cpu[i])
    ram[i] = float(ram[i])
    disco[i] = float(disco[i])



# Média de uso de ram da última hora
media = 0
total = 0
media_ram_hora = 0.0
tempo_atual =  datetime.now()
ultima_hora = (int(tempo_atual.hour) - 1)
pico_cpu = cpu[0]

param_seg = int(600) # 10 min
param_seg_ram = int(3600) # 10 min
hatual_em_segundos = int(tempo_atual.hour) * 3600
matual_em_segundos = int(tempo_atual.minute) * 60
segundos_atuais = int(tempo_atual.second)

tempo_atual_s = hatual_em_segundos + matual_em_segundos + segundos_atuais
restricao_tempo = tempo_atual_s - param_seg
restricao_tempo_hr = tempo_atual_s - param_seg_ram


for i in range(len(timestamp)):

    if (tempo_atual_s >= restricao_tempo_hr):
        media += ram[i]
        total += 1

if not(media == 0.0):
    media_ram_hora = media / total


# Pico de uso da CPU da última hora

for i in range(len(timestamp)):

    if (tempo_atual_s <= restricao_tempo_hr and pico_cpu <= cpu[i] ) :
        pico_cpu = cpu[i]



# Média do uso de Disco dos últimos N minutos
# H:M:S atual, transformar em segundos e depois subtrair pela qtd de minutos em segundos que eu quero
# Nisso pega um for com if que pega só os minutos que eu quero até hora atual
media_disco = 0
qtd_dados = 0

for i in range(len(timestamp)):

    timestamp_h = int(timestamp[i].hour) * 3600
    timestamp_m = int(timestamp[i].minute) * 60
    timestamp_s = int(timestamp[i].second)
    timestamp_em_segundos = timestamp_h + timestamp_m + timestamp_s

    if (timestamp_em_segundos >= restricao_tempo):
        media_disco += disco[i]
        qtd_dados += 1

if not (media_disco == 0):
    media_disco = media_disco / qtd_dados


# Print dos resultados
print(f"""
-- CPU --
Pico de uso CPU ({ultima_hora}h): {pico_cpu:.2f}
De: {user_pico}

-- RAM --
Média de uso ({ultima_hora}hrs à {tempo_atual.hour}hrs): {media_ram_hora:.2f}

-- Disco --
Média de uso (últimos {int(param_seg / 60)}m): {media_disco:.2f}
""")