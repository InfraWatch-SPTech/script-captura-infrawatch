# ============================================================
# GERADOR DE DADOS FICTÍCIOS PARA TESTE LOCAL DA DASHBOARD
# ------------------------------------------------------------
# Cria arquivos JSON na Bronze LOCAL (dados/bronze/hardware/...)
# no mesmo formato da captura.py, para testar o leitura.py e a
# dashboard sem esperar dias de captura real.
#
# ATENÇÃO: os valores são FICTÍCIOS. Não use em apresentação
# como se fossem dados reais.
#
# Uso: python3 gerar_dados_teste.py --empresa 2 --servidores 1 2 --dias 7
# (os IDs devem existir no banco, na tabela equipamento)
# ============================================================
import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

PASTA_SCRIPT = Path(__file__).resolve().parent
INTERVALO_MINUTOS = 5

PROCESSOS = ["java", "mysqld", "node", "backup", "python3", "nginx", "sshd", "cron"]


def limitar(valor):
    return round(min(100.0, max(0.0, valor)), 2)


def gerar_amostra(empresa_id, servidor_id, inicio, perfil, aleatorio):
    hora_local = (inicio - timedelta(hours=3)).hour  # America/Sao_Paulo
    fim_semana = (inicio - timedelta(hours=3)).weekday() >= 5
    pico = 14 <= hora_local < 19 and not fim_semana

    cpu = perfil["cpu"] + (25 if pico else 0) + aleatorio.uniform(-8, 8)
    ram = perfil["ram"] + (perfil["pico_ram"] if pico else 0) + aleatorio.uniform(-4, 4)
    disco = perfil["disco"] + aleatorio.uniform(-0.5, 0.5)
    rede = perfil["rede"] + (30 if pico else 0) + aleatorio.uniform(-10, 10)

    processos = []
    for i in range(len(PROCESSOS)):
        peso = 1 / (i + 1)
        processos.append({
            "nome": PROCESSOS[i],
            "cpu": round(max(0.0, cpu * peso * 0.4 * aleatorio.uniform(0.7, 1.3)), 4),
            "ram": round(max(0.0, ram * peso * 0.3 * aleatorio.uniform(0.7, 1.3)), 4),
        })
    if fim_semana:
        processos[3]["cpu"] = round(processos[3]["cpu"] + 12, 4)  # backup no fim de semana

    return {
        "versao": 2,
        "empresa_id": str(empresa_id),
        "servidor_id": str(servidor_id),
        "inicio": inicio.isoformat(),
        "fim": (inicio + timedelta(minutes=INTERVALO_MINUTOS)).isoformat(),
        "cpu": limitar(cpu),
        "ram": limitar(ram),
        "disco": limitar(disco),
        "rede": limitar(rede),
        "processos": processos,
        "processos_completos": True,
    }


def main():
    argumentos = argparse.ArgumentParser()
    argumentos.add_argument("--empresa", type=int, default=2)
    argumentos.add_argument("--servidores", type=int, nargs="+", default=[1, 2])
    argumentos.add_argument("--dias", type=int, default=7)
    argumentos.add_argument("--pasta", default=str(PASTA_SCRIPT / "dados"))
    opcoes = argumentos.parse_args()

    # Cada servidor recebe um perfil diferente para a pontuação variar
    perfis = [
        {"cpu": 35, "ram": 62, "pico_ram": 22, "disco": 83, "rede": 25},
        {"cpu": 20, "ram": 45, "pico_ram": 10, "disco": 55, "rede": 15},
        {"cpu": 50, "ram": 70, "pico_ram": 15, "disco": 70, "rede": 40},
    ]

    fim = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    fim = fim - timedelta(minutes=fim.minute % INTERVALO_MINUTOS)
    inicio = fim - timedelta(days=opcoes.dias)

    for posicao in range(len(opcoes.servidores)):
        servidor_id = opcoes.servidores[posicao]
        perfil = perfis[posicao % len(perfis)]
        aleatorio = random.Random(servidor_id)
        amostras = []
        atual = inicio
        while atual < fim:
            amostras.append(gerar_amostra(opcoes.empresa, servidor_id, atual, perfil, aleatorio))
            atual += timedelta(minutes=INTERVALO_MINUTOS)

        pasta = Path(opcoes.pasta) / "bronze" / "hardware" / str(opcoes.empresa) / str(servidor_id)
        pasta.mkdir(parents=True, exist_ok=True)
        arquivo = pasta / f"teste_ficticio_{opcoes.dias}dias.json"
        arquivo.write_text(json.dumps(amostras), encoding="utf-8")
        print(f"Gerado: {arquivo} ({len(amostras)} amostras fictícias)")


if __name__ == "__main__":
    main()
