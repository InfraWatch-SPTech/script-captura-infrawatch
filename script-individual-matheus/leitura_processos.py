# ============================================================
# LEITURA E FORMATAÇÃO DOS PROCESSOS CAPTURADOS
# ============================================================

MAX_PROCESSOS_GOLD = 15


def ler_processos(processos_capturados):
    agrupados = {}
    for processo in processos_capturados or []:
        nome = str(processo.get("nome") or "").strip()
        if not nome:
            continue
        if nome not in agrupados:
            agrupados[nome] = {"nome": nome, "cpu": 0.0, "ram": 0.0}
        agrupados[nome]["cpu"] += float(processo.get("cpu") or 0)
        agrupados[nome]["ram"] += float(processo.get("ram") or 0)

    processos = []
    for nome in sorted(agrupados):
        processo = agrupados[nome]
        processo["cpu"] = round(min(max(processo["cpu"], 0.0), 100.0), 4)
        processo["ram"] = round(min(max(processo["ram"], 0.0), 100.0), 4)
        processos.append(processo)
    return processos


def normalizar_campos_processos(linha):
    return {
        "processos": ler_processos(linha.get("processos")),
        "processos_completos": bool(linha.get("processos_completos")),
    }


def maiores_processos(processos):
    por_cpu = sorted(processos, key=lambda p: p["cpu"], reverse=True)[:MAX_PROCESSOS_GOLD]
    por_ram = sorted(processos, key=lambda p: p["ram"], reverse=True)[:MAX_PROCESSOS_GOLD]
    escolhidos = {}
    for processo in por_cpu + por_ram:
        escolhidos[processo["nome"]] = {
            "nome": processo["nome"],
            "cpu": processo["cpu"],
            "ram": processo["ram"],
        }
    return [escolhidos[nome] for nome in sorted(escolhidos)]


def formatar_campos_processos_gold(amostra):
    return {
        "processos": maiores_processos(amostra["processos"]),
        "processos_completos": amostra["processos_completos"],
    }
