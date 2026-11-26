import boto3
import csv
import io
import json
import os
import urllib.parse
import math
import random
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

s3 = boto3.client('s3')

def buscar_ultimo_arquivo_s3(bucket, prefixo='bronze/hardware/'):
    resposta = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo)
    if 'Contents' not in resposta:
        raise FileNotFoundError(
            f'Nenhum arquivo encontrado no bucket {bucket} com o prefixo {prefixo}'
        )

    # Filtra apenas arquivos que terminam com .csv
    arquivos_csv = [
        obj for obj in resposta['Contents']
        if obj['Key'].endswith('.csv')
    ]
    if not arquivos_csv:
        raise FileNotFoundError(
            f'Nenhum arquivo .csv encontrado em s3://{bucket}/{prefixo}'
        )

    # Ordena pelo campo LastModified para pegar o mais recente
    arquivo_mais_recente = sorted(
        arquivos_csv, key=lambda x: x['LastModified'], reverse=True
    )[0]
    return arquivo_mais_recente['Key']

def sufixo_dashboard(arquivo=None):
    nome = (arquivo or '').rsplit('/', 1)[-1]
    data = re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$', nome, re.I)
    if data:
        return data.group(1)
    return datetime.now(FUSO_PADROES).strftime('%Y-%m-%d_%H-%M-%S')


def processar_arquivo_s3(bucket, arquivo):
    print(f'Processando arquivo: s3://{bucket}/{arquivo}')
    
    partes = arquivo.split('/')
    if len(partes) != 5:
        raise ValueError(
            f'Caminho inválido: {arquivo}. Esperado:'
            ' bronze/categoria/empresa/equipamento/arquivo.csv'
        )
    if partes[0] != 'bronze':
        raise ValueError(f'O arquivo não está na camada Bronze: {arquivo}')

    categoria = partes[1]
    empresa = partes[2]
    equipamento = partes[3]
    nome_arquivo = partes[4]
    
    if categoria != 'hardware':
        raise ValueError(
            f'Categoria inválida para este leitor: {categoria}. Esperado: hardware.'
        )

    print(f'Categoria: {categoria}')
    print(f'Empresa: {empresa}')
    print(f'Equipamento: {equipamento}')
    print(f'Arquivo: {nome_arquivo}')
    
    resposta = s3.get_object(Bucket=bucket, Key=arquivo)
    conteudo = resposta['Body'].read().decode('utf-8')
    csv_buffer = io.StringIO(conteudo)
    leitor = csv.DictReader(csv_buffer, delimiter=';')
    registros = list(leitor)
    
    if not registros:
        raise ValueError('O arquivo CSV não possui registros.')

    dados_tratados = []
    for linha in registros:
        registro = {}
        for campo, valor in linha.items():
            if valor is not None:
                valor = valor.strip()
            else:
                valor = ''
            if campo in ('id', 'data_hora', 'inicio', 'fim', 'id_mac', 'empresa_id', 'servidor_id'):
                registro[campo] = valor
            else:
                try:
                    registro[campo] = float(valor)
                except ValueError:
                    registro[campo] = valor
        dados_tratados.append(registro)

    quantidade_amostras = len(dados_tratados)
    
    metricas = []
    if dados_tratados:
        for campo, valor in dados_tratados[0].items():
            if campo not in ['id', 'data_hora'] and isinstance(valor, (int, float)):
                metricas.append(campo)
    print(f'Métricas encontradas: {metricas}')
    
    estatisticas = {}
    for metrica in metricas:
        valores = [
            registro[metrica] for registro in dados_tratados
            if isinstance(registro[metrica], (int, float))
        ]
        if valores:
            estatisticas[metrica] = {
                'media': round(sum(valores) / len(valores), 2),
                'minimo': round(min(valores), 2),
                'maximo': round(max(valores), 2),
            }

    inicio = dados_tratados[0].get('data_hora') or dados_tratados[0].get('inicio')
    fim = dados_tratados[-1].get('data_hora') or dados_tratados[-1].get('fim') or dados_tratados[-1].get('inicio')
    if not inicio or not fim:
        raise ValueError('O CSV precisa conter data_hora ou inicio/fim.')
    
    silver = {
        'empresa': empresa,
        'equipamento': equipamento,
        'quantidade_amostras': quantidade_amostras,
        'inicio': inicio,
        'fim': fim,
        'metricas': estatisticas,
    }
    
    nome_silver = nome_arquivo.replace('.csv', '.json')
    arquivo_silver = f'silver/{empresa}/{equipamento}/{nome_silver}'
    
    s3.put_object(
        Bucket=bucket,
        Key=arquivo_silver,
        Body=json.dumps(silver, ensure_ascii=False, indent=4).encode('utf-8'),
        ContentType='application/json',
    )
    print(f'Arquivo Silver saved em: s3://{bucket}/{arquivo_silver}')
    
    gold = {
        'empresa': empresa,
        'equipamento': equipamento,
        'data': inicio[:10],
        'quantidade_amostras': quantidade_amostras,
    }
    for metrica, valores in estatisticas.items():
        gold[f'{metrica}_media'] = valores['media']

    nome_gold = nome_arquivo.replace('.csv', '.json')
    arquivo_gold = f'gold/{empresa}/{equipamento}/{nome_gold}'
    
    s3.put_object(
        Bucket=bucket,
        Key=arquivo_gold,
        Body=json.dumps(gold, ensure_ascii=False, indent=4).encode('utf-8'),
        ContentType='application/json',
    )
    print(f'Arquivo Gold saved em: s3://{bucket}/{arquivo_gold}')

    # Gera arquivos próprios da dashboard sem alterar os arquivos existentes.
    gerar_dash_previsibilidade_s3(bucket, arquivo, empresa, equipamento)
    gerar_dashboard_matheus(bucket, empresa, arquivo)
    gerar_gestao_problemas_ana(bucket, arquivo, empresa)
    try:
        resultado = gerar_dash_padroes(bucket, empresa, arquivo)
        print(f'Dashboard de padrões: {resultado}')
    except Exception as erro:
        print(f'Erro ao gerar dashboard de padrões para {empresa}: {erro}')


# ============================================================
# ARQUIVOS ADICIONAIS PARA A DASHBOARD DE PREVISIBILIDADE
# ============================================================
# Estas funções leem o histórico Bronze do equipamento, pois uma execução
# da captura contém somente cinco amostras e não basta para uma tendência.

JANELA_PREVISIBILIDADE_DIAS = 30
INTERVALO_AGREGACAO_MINUTOS = 10
MINIMO_AMOSTRAS_PREVISAO = 3

ALIAS_RECURSOS = {
    'cpu': {'CPU', 'USO_CPU', 'PROCESSADOR', 'USO_PROCESSADOR'},
    'ram': {'RAM', 'USO_RAM', 'MEMORIA', 'MEMORIA_RAM', 'USO_MEMORIA'},
    'disco': {'DISCO', 'USO_DISCO', 'ARMAZENAMENTO', 'USO_ARMAZENAMENTO'},
    'rede': {'REDE', 'USO_REDE', 'REDE_PERCENTUAL'},
    'temperatura': {'TEMPERATURA', 'TEMPERATURA_CPU', 'CPU_TEMPERATURA'},
}

ALIAS_REQUISICOES = {
    'requisicoes': {'REQUISICOES', 'REQUESTS', 'REQUEST_COUNT', 'REQ_TOTAL'},
    'requisicoes_por_segundo': {'REQUISICOES_POR_SEGUNDO', 'REQUESTS_PER_SECOND', 'RPS'},
    'sucessos': {'REQUISICOES_SUCESSO', 'REQUESTS_SUCCESS', 'REQ_SUCESSO'},
    'erros': {'REQUISICOES_ERRO', 'REQUESTS_ERROR', 'REQ_ERROS'},
    'concorrencia': {'CONCORRENCIA_TESTE', 'REQUEST_CONCURRENCY', 'REQ_CONCORRENCIA'},
    'latencia_media_ms': {'LATENCIA_MEDIA_MS', 'REQUEST_LATENCY_MEAN_MS'},
    'latencia_p95_ms': {'LATENCIA_P95_MS', 'REQUEST_LATENCY_P95_MS'},
    'latencia_p99_ms': {'LATENCIA_P99_MS', 'REQUEST_LATENCY_P99_MS'},
    'latencia_maxima_ms': {'LATENCIA_MAX_MS', 'REQUEST_LATENCY_MAX_MS'},
}


def _numero_previsibilidade(valor):
    try:
        numero = float(str(valor).strip().replace(',', '.'))
        return numero if math.isfinite(numero) else None
    except (TypeError, ValueError):
        return None


def _timestamp_previsibilidade(valor):
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor).strip().replace('Z', '+00:00'))
    except ValueError:
        for formato in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
            try:
                return datetime.strptime(str(valor).strip(), formato)
            except ValueError:
                pass
    return None


def _nome_recurso_previsibilidade(campo):
    normalizado = str(campo).strip().upper().replace('-', '_').replace(' ', '_')
    for nome, aliases in ALIAS_RECURSOS.items():
        if normalizado in aliases:
            return nome
    return None


def _nome_limite_previsibilidade(campo):
    normalizado = str(campo).strip().upper().replace('-', '_').replace(' ', '_')
    for sufixo, nome_limite in (
        ('_LIMITE_ATENCAO', 'atencao'),
        ('_LIMITE_CRITICO', 'critico'),
        ('_LIMITE_ALERTA', 'atencao'),
    ):
        if normalizado.endswith(sufixo):
            recurso = _nome_recurso_previsibilidade(normalizado[:-len(sufixo)])
            if recurso:
                return recurso, nome_limite
    for prefixo, nome_limite in (
        ('LIMITE_ATENCAO_', 'atencao'),
        ('LIMITE_CRITICO_', 'critico'),
    ):
        if normalizado.startswith(prefixo):
            recurso = _nome_recurso_previsibilidade(normalizado[len(prefixo):])
            if recurso:
                return recurso, nome_limite
    return None


def _alias_requisicao_previsibilidade(campo):
    normalizado = str(campo).strip().upper().replace('-', '_').replace(' ', '_')
    for nome, aliases in ALIAS_REQUISICOES.items():
        if normalizado in aliases:
            return nome
    return None


def _percentil_previsibilidade(valores, percentil):
    ordenados = sorted(valores)
    if not ordenados:
        return None
    posicao = (len(ordenados) - 1) * percentil
    inferior = int(math.floor(posicao))
    superior = int(math.ceil(posicao))
    if inferior == superior:
        return round(ordenados[inferior], 2)
    peso = posicao - inferior
    return round(ordenados[inferior] * (1 - peso) + ordenados[superior] * peso, 2)


def _listar_csv_historico(bucket, prefixo, arquivo_atual):
    chaves = []
    paginador = s3.get_paginator('list_objects_v2')
    limite = datetime.now(timezone.utc) - timedelta(days=JANELA_PREVISIBILIDADE_DIAS)
    for pagina in paginador.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get('Contents', []):
            chave = objeto.get('Key', '')
            if not chave.lower().endswith('.csv'):
                continue
            modificado = objeto.get('LastModified')
            if chave == arquivo_atual or not modificado or modificado >= limite:
                chaves.append((chave, modificado))
    chaves.sort(key=lambda item: item[1] or datetime.min.replace(tzinfo=timezone.utc))
    return [chave for chave, _ in chaves]


def _ler_amostras_historicas(bucket, chaves):
    amostras = []
    for chave in chaves:
        resposta = s3.get_object(Bucket=bucket, Key=chave)
        conteudo = resposta['Body'].read().decode('utf-8-sig')
        leitor = csv.DictReader(io.StringIO(conteudo), delimiter=';')
        for linha in leitor:
            instante = _timestamp_previsibilidade(
                linha.get('data_hora') or linha.get('inicio') or linha.get('INICIO_AMOSTRA')
            )
            if instante is None:
                continue
            registro = {'data_hora': instante.isoformat(sep=' '), '_instante': instante}
            for campo, valor in linha.items():
                if campo in ('id', 'data_hora', 'inicio', 'fim', 'INICIO_AMOSTRA', 'FIM_AMOSTRA') or campo is None:
                    continue
                numero = _numero_previsibilidade(valor)
                if numero is not None:
                    registro[campo] = numero
            amostras.append(registro)
    amostras.sort(key=lambda item: item['_instante'])
    return amostras


def _agregar_janela_previsibilidade(amostras, nome_recurso):
    por_intervalo = {}
    for amostra in amostras:
        instante = amostra['_instante']
        chave = instante.replace(
            minute=(instante.minute // INTERVALO_AGREGACAO_MINUTOS) * INTERVALO_AGREGACAO_MINUTOS,
            second=0,
            microsecond=0,
        )
        for campo, valor in amostra.items():
            if campo.startswith('_') or campo == 'data_hora':
                continue
            if _nome_recurso_previsibilidade(campo) == nome_recurso:
                por_intervalo.setdefault(chave, []).append(valor)
    serie = []
    for instante in sorted(por_intervalo):
        valores = por_intervalo[instante]
        serie.append({
            'data_hora': instante.isoformat(sep=' '),
            'media': round(sum(valores) / len(valores), 2),
            'minimo': round(min(valores), 2),
            'maximo': round(max(valores), 2),
            'amostras': len(valores),
        })
    return serie


def _regressao_previsibilidade(pontos):
    if len(pontos) < 2:
        return None
    inicio = pontos[0]['_instante']
    xs = [(ponto['_instante'] - inicio).total_seconds() / 86400 for ponto in pontos]
    ys = [ponto['valor'] for ponto in pontos]
    media_x = sum(xs) / len(xs)
    media_y = sum(ys) / len(ys)
    soma_xx = sum((x - media_x) ** 2 for x in xs)
    if soma_xx == 0:
        return None
    inclinacao = sum((x - media_x) * (y - media_y) for x, y in zip(xs, ys)) / soma_xx
    intercepto = media_y - inclinacao * media_x
    residuos = [y - (intercepto + inclinacao * x) for x, y in zip(xs, ys)]
    erro_residual = math.sqrt(sum(residuo ** 2 for residuo in residuos) / len(residuos))
    return inclinacao, intercepto, erro_residual


def _construir_recurso_previsibilidade(nome, amostras):
    pontos = []
    limites = {'atencao': None, 'critico': None}
    for amostra in amostras:
        valor_recurso = None
        for campo, valor in amostra.items():
            if campo.startswith('_') or campo == 'data_hora':
                continue
            if _nome_recurso_previsibilidade(campo) == nome:
                valor_recurso = valor
            limite = _nome_limite_previsibilidade(campo)
            if limite and limite[0] == nome:
                limites[limite[1]] = valor
        if valor_recurso is not None:
            pontos.append({'_instante': amostra['_instante'], 'data_hora': amostra['data_hora'], 'valor': valor_recurso})
    if not pontos:
        return None

    valores = [ponto['valor'] for ponto in pontos]
    regressao = _regressao_previsibilidade(pontos)
    duracao_dias = (pontos[-1]['_instante'] - pontos[0]['_instante']).total_seconds() / 86400
    ultima = pontos[-1]
    recursos = {
        'quantidade_amostras': len(pontos),
        'inicio': pontos[0]['data_hora'],
        'fim': ultima['data_hora'],
        'duracao_historico_dias': round(duracao_dias, 4),
        'atual': round(ultima['valor'], 2),
        'media': round(sum(valores) / len(valores), 2),
        'minimo': round(min(valores), 2),
        'maximo': round(max(valores), 2),
        'p50': _percentil_previsibilidade(valores, .50),
        'p95': _percentil_previsibilidade(valores, .95),
        'p99': _percentil_previsibilidade(valores, .99),
        'limite_atencao': limites['atencao'],
        'limite_critico': limites['critico'],
        'serie_10_minutos': _agregar_janela_previsibilidade(amostras, nome),
        'tendencia_por_dia': None,
        'previsao_30_dias': [],
        'runway_dias': None,
        'qualidade_previsao': 'amostras_insuficientes',
        'previsao_disponivel': False,
        'mensagem_previsao': f'Previsão indisponível: são necessárias pelo menos {MINIMO_AMOSTRAS_PREVISAO} amostras; disponíveis: {len(pontos)}.',
    }
    if regressao:
        inclinacao, intercepto, erro = regressao
        recursos['tendencia_por_dia'] = round(inclinacao, 4)
        recursos['erro_residual_estimado'] = round(erro, 2)
        if len(pontos) >= MINIMO_AMOSTRAS_PREVISAO:
            recursos['qualidade_previsao'] = 'estimativa_linear_experimental'
            recursos['previsao_disponivel'] = True
            recursos['mensagem_previsao'] = 'Estimativa linear experimental calculada com base nas amostras disponíveis.'
            dias_futuros = (7, 14, 21, 30)
            for dia in dias_futuros:
                previsto = max(0, min(100, intercepto + inclinacao * ((ultima['_instante'] - pontos[0]['_instante']).total_seconds() / 86400 + dia)))
                margem = erro * 1.96
                recursos['previsao_30_dias'].append({
                    'dias_a_frente': dia,
                    'estimativa': round(previsto, 2),
                    'faixa_inferior': round(max(0, previsto - margem), 2),
                    'faixa_superior': round(min(100, previsto + margem), 2),
                })
            critico = limites['critico']
            if critico is not None:
                recursos['runway_dias'] = 0 if ultima['valor'] >= critico else (
                    round((critico - ultima['valor']) / inclinacao, 2) if inclinacao > 0 else None
                )
    return recursos


def _construir_requisicoes_previsibilidade(amostras):
    registros = []
    for amostra in amostras:
        valores = {}
        for campo, valor in amostra.items():
            metrica = _alias_requisicao_previsibilidade(campo)
            if metrica:
                valores[metrica] = valor
        if valores:
            for campo, valor in amostra.items():
                if _nome_recurso_previsibilidade(campo) == 'cpu':
                    valores['cpu_percentual'] = valor
                    break
            registros.append({'data_hora': amostra['data_hora'], **valores})
    if not registros:
        return {
            'disponivel': False,
            'motivo': 'A captura Bronze atual não registra volume de requisições, concorrência ou latência HTTP.',
            'metricas': ['requisicoes', 'sucessos', 'erros', 'concorrencia', 'latencia_media_ms', 'latencia_p95_ms', 'latencia_p99_ms'],
            'serie': [],
        }
    latencias_p95 = [r['latencia_p95_ms'] for r in registros if 'latencia_p95_ms' in r]
    taxas = [r['requisicoes_por_segundo'] for r in registros if 'requisicoes_por_segundo' in r]
    return {
        'disponivel': True,
        'fonte': 'colunas de captura HTTP presentes nos arquivos Bronze',
        'ultima_amostra': registros[-1],
        'total_requisicoes': round(sum(r.get('requisicoes', 0) for r in registros), 2),
        'total_sucessos': round(sum(r.get('sucessos', 0) for r in registros), 2),
        'total_erros': round(sum(r.get('erros', 0) for r in registros), 2),
        'latencia_p95_ms': _percentil_previsibilidade(latencias_p95, .95),
        'requisicoes_por_segundo_media': round(sum(taxas) / len(taxas), 2) if taxas else None,
        'requisicoes_por_segundo_maxima': round(max(taxas), 2) if taxas else None,
        'serie': registros,
    }


def gerar_dash_previsibilidade_s3(bucket, arquivo_origem, empresa, equipamento):
    prefixo = '/'.join(arquivo_origem.split('/')[:4]) + '/'
    chaves = _listar_csv_historico(bucket, prefixo, arquivo_origem)
    amostras = _ler_amostras_historicas(bucket, chaves)
    recursos = {}
    for nome in ALIAS_RECURSOS:
        recurso = _construir_recurso_previsibilidade(nome, amostras)
        if recurso:
            recursos[nome] = recurso

    horizonte = (amostras[-1]['_instante'] - amostras[0]['_instante']).total_seconds() / 86400 if len(amostras) > 1 else 0
    status_recursos = {}
    for nome in ALIAS_RECURSOS:
        recurso = recursos.get(nome)
        quantidade = recurso['quantidade_amostras'] if recurso else 0
        status_recursos[nome] = {
            'quantidade_amostras': quantidade,
            'previsao_disponivel': bool(recurso and recurso['previsao_disponivel']),
            'mensagem': recurso['mensagem_previsao'] if recurso else (
                f'Previsão indisponível: são necessárias pelo menos {MINIMO_AMOSTRAS_PREVISAO} amostras; disponíveis: 0.'
            ),
        }
    dashboard = {
        'tipo_arquivo': 'dash-previsibilidade',
        'versao_schema': 1,
        'empresa': empresa,
        'equipamento': equipamento,
        'gerado_em_utc': datetime.now(timezone.utc).isoformat(),
        'janela_historico_dias': JANELA_PREVISIBILIDADE_DIAS,
        'quantidade_arquivos_bronze': len(chaves),
        'quantidade_amostras': len(amostras),
        'inicio': amostras[0]['data_hora'] if amostras else None,
        'fim': amostras[-1]['data_hora'] if amostras else None,
        'duracao_historico_dias': round(horizonte, 4),
        'minimo_amostras_previsao': MINIMO_AMOSTRAS_PREVISAO,
        'status_previsao': status_recursos,
        'recursos': recursos,
        'requisicoes': _construir_requisicoes_previsibilidade(amostras),
        'disponibilidade_sla': {
            'disponivel': False,
            'motivo': 'A captura atual não registra eventos de indisponibilidade ou duração de falhas.',
        },
        'observacoes': [
            f'Previsões lineares são estimativas experimentais e exigem pelo menos {MINIMO_AMOSTRAS_PREVISAO} amostras por recurso.',
            'As faixas de previsão não são intervalos de confiança de um modelo validado.',
            'Limites só são preenchidos se forem incluídos como colunas no CSV Bronze.',
        ],
    }

    nome_origem = arquivo_origem.rsplit('/', 1)[-1].replace('.csv', '.json')
    nome_dashboard = f'dash-previsibilidade_{nome_origem}'
    arquivo_silver_dashboard = f'silver/{empresa}/{equipamento}/{nome_dashboard}'
    arquivo_gold_dashboard = f'gold/{empresa}/{equipamento}/{nome_dashboard}'
    conteudo = json.dumps(dashboard, ensure_ascii=False, indent=4).encode('utf-8')
    s3.put_object(Bucket=bucket, Key=arquivo_silver_dashboard, Body=conteudo, ContentType='application/json')
    s3.put_object(Bucket=bucket, Key=arquivo_gold_dashboard, Body=conteudo, ContentType='application/json')
    print(f'Arquivo Silver dashboard salvo em: s3://{bucket}/{arquivo_silver_dashboard}')
    print(f'Arquivo Gold dashboard salvo em: s3://{bucket}/{arquivo_gold_dashboard}')
    return dashboard


# ============================================================
# ADIÇÕES: LEITURA DO MATHEUS E GESTÃO DE PROBLEMAS DA ANA
# As saídas originais do grupo e a previsibilidade são mantidas.
# ============================================================

def listar_arquivos_adicionais(bucket, prefixo, extensoes=('.csv', '.json')):
    arquivos = []
    paginador = s3.get_paginator('list_objects_v2')
    for pagina in paginador.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get('Contents', []):
            if objeto['Key'].lower().endswith(extensoes):
                arquivos.append(objeto['Key'])
    return sorted(arquivos)


def ler_registros_adicionais(bucket, chave):
    conteudo = s3.get_object(Bucket=bucket, Key=chave)['Body'].read().decode('utf-8-sig')
    if chave.lower().endswith('.csv'):
        return list(csv.DictReader(io.StringIO(conteudo), delimiter=';'))
    dados = json.loads(conteudo)
    if isinstance(dados, list):
        return dados
    if isinstance(dados, dict):
        return dados.get('amostras', [dados])
    raise ValueError(f'Conteúdo inválido em {chave}')


def salvar_adicional(bucket, chave, dados):
    s3.put_object(
        Bucket=bucket, Key=chave,
        Body=json.dumps(dados, ensure_ascii=False, allow_nan=False).encode('utf-8'),
        ContentType='application/json',
    )
    print(f'Arquivo adicional salvo: s3://{bucket}/{chave}')


def numero_adicional(valor):
    if valor in (None, ''):
        return None
    if isinstance(valor, bool):
        raise ValueError('Booleano não é uma medição numérica.')
    numero = float(str(valor).strip().replace(',', '.'))
    if not math.isfinite(numero):
        raise ValueError('Medição numérica não finita.')
    return numero


def campo_adicional(linha, *nomes):
    for nome in nomes:
        if linha.get(nome) not in (None, ''):
            return linha[nome]
    return None


def data_adicional(valor):
    data = datetime.fromisoformat(str(valor).strip().replace('Z', '+00:00'))
    if data.tzinfo is None:
        raise ValueError('Amostra sem fuso horário.')
    return data.astimezone(timezone.utc)


def id_adicional(valor):
    texto = str(valor).strip()
    if not texto.isdigit() or int(texto) <= 0:
        raise ValueError(f'ID inválido: {texto}')
    return str(int(texto))


def normalizar_processos_adicionais(processos):
    agrupados = {}
    for processo in processos or []:
        nome = str(campo_adicional(processo, 'nome', 'processo', 'PROCESSO', 'Processo') or '').strip()
        cpu = numero_adicional(campo_adicional(processo, 'cpu', 'CPU_AGREGADA'))
        ram = numero_adicional(campo_adicional(processo, 'ram', 'RAM_AGREGADA'))
        # RAM_PROCESSO é inventário; não substituir RAM_AGREGADA por ela.
        if not nome or (cpu is None and ram is None):
            continue
        atual = agrupados.setdefault(nome, {'nome': nome, 'cpu': None, 'ram': None})
        for campo, valor in (('cpu', cpu), ('ram', ram)):
            if valor is not None:
                if not 0 <= valor <= 100:
                    raise ValueError(f'Percentual inválido em {nome}/{campo}')
                atual[campo] = min(100, (atual[campo] or 0) + valor)
    return [agrupados[nome] for nome in sorted(agrupados)]


def processos_por_intervalo(bucket, empresa_pasta):
    agrupados = {}
    prefixo = f'bronze/processo/{empresa_pasta}/'
    for chave in listar_arquivos_adicionais(bucket, prefixo, ('.csv',)):
        servidor_pasta = id_adicional(chave.split('/')[3])
        por_arquivo = {}
        for linha in ler_registros_adicionais(bucket, chave):
            inicio = campo_adicional(linha, 'inicio', 'INICIO_AMOSTRA')
            if not inicio:
                continue  # Inventário antigo sem vínculo temporal: não inventar vínculo.
            servidor = id_adicional(campo_adicional(linha, 'servidor_id', 'ID_EQUIPAMENTO') or servidor_pasta)
            if servidor != servidor_pasta:
                raise ValueError(f'Equipamento diferente do caminho: {chave}')
            instante = data_adicional(inicio).isoformat()
            por_arquivo.setdefault((servidor, instante), []).append(linha)
        for identificador, linhas in por_arquivo.items():
            # Uma cópia repetida do CSV não soma o consumo novamente.
            agrupados[identificador] = normalizar_processos_adicionais(linhas)
    return agrupados


def normalizar_amostra_adicional(linha, empresa_pasta, servidor_pasta):
    empresa = campo_adicional(linha, 'empresa_id')
    if empresa is None and empresa_pasta.isdigit():
        empresa = empresa_pasta
    empresa = id_adicional(empresa)
    servidor = id_adicional(campo_adicional(linha, 'servidor_id', 'id') or servidor_pasta)
    if servidor != id_adicional(servidor_pasta):
        raise ValueError('Servidor diferente do caminho.')
    if empresa_pasta.isdigit() and empresa != id_adicional(empresa_pasta):
        raise ValueError('Empresa diferente do caminho.')

    inicio = data_adicional(campo_adicional(linha, 'inicio', 'INICIO_AMOSTRA'))
    fim = data_adicional(campo_adicional(linha, 'fim', 'FIM_AMOSTRA'))
    if not 0 < (fim - inicio).total_seconds() <= 300:
        raise ValueError('Intervalo inválido ou maior que cinco minutos.')
    amostra = {
        'empresa_id': empresa, 'servidor_id': servidor,
        'inicio': inicio.isoformat(), 'fim': fim.isoformat(),
    }
    aliases = {
        'cpu': ('cpu', 'CPU', 'CPU_PCT'),
        'ram': ('ram', 'RAM', 'RAM_PCT'),
        'disco': ('disco', 'ARMAZENAMENTO', 'DISCO', 'DISCO_PCT'),
        'rede': ('rede', 'REDE', 'REDE_PCT'),
    }
    for componente, nomes in aliases.items():
        valor = numero_adicional(campo_adicional(linha, *nomes))
        if valor is not None and not 0 <= valor <= 100:
            raise ValueError(f'Percentual inválido: {componente}')
        amostra[componente] = valor
    for campo in ('rede_mbps', 'rede_capacidade_mbps', 'rede_capacidade_reserva'):
        if campo in linha:
            amostra[campo] = linha[campo]
    processos = campo_adicional(linha, 'processos', 'PROCESSOS_CPU_RAM') or []
    if isinstance(processos, str):
        processos = json.loads(processos)
    amostra['processos'] = normalizar_processos_adicionais(processos)
    amostra['processos_completos'] = str(linha.get('processos_completos', '')).lower() in ('1', 'true', 'sim')
    return amostra


def gerar_dashboard_matheus(bucket, empresa_pasta, arquivo=None):
    # Reprocessa o histórico da empresa, sem apagar arquivos Bronze.
    # IDs e limites do MySQL continuam sendo responsabilidade do backend.
    separados = processos_por_intervalo(bucket, empresa_pasta)
    por_empresa = {}
    invalidas = 0
    for chave in listar_arquivos_adicionais(bucket, f'bronze/hardware/{empresa_pasta}/'):
        partes = chave.split('/')
        if len(partes) != 5:
            continue
        for linha in ler_registros_adicionais(bucket, chave):
            try:
                amostra = normalizar_amostra_adicional(linha, empresa_pasta, partes[3])
            except (ValueError, TypeError, KeyError) as erro:
                invalidas += 1
                print(f'Amostra não incluída na nova dashboard: {chave}: {erro}')
                continue
            identificador = (amostra['servidor_id'], amostra['inicio'])
            if not amostra['processos'] and identificador in separados:
                amostra['processos'] = separados[identificador]
            por_empresa.setdefault(amostra['empresa_id'], {})[identificador] = amostra

    if len(por_empresa) > 1:
        raise ValueError('A pasta Bronze contém mais de um empresa_id. Verifique antes de gerar a Gold.')
    gerado_em = datetime.now(timezone.utc).isoformat()
    for empresa, unicas in por_empresa.items():
        amostras = []
        ultimo_fim = {}
        for amostra in sorted(unicas.values(), key=lambda a: (int(a['servidor_id']), a['inicio'])):
            servidor = amostra['servidor_id']
            if servidor in ultimo_fim and amostra['inicio'] < ultimo_fim[servidor]:
                continue
            ultimo_fim[servidor] = amostra['fim']
            amostras.append(amostra)
        silver = {'versao': 2, 'empresa_id': empresa, 'gerado_em': gerado_em, 'amostras': amostras}
        salvar_adicional(bucket, f'silver/empresas/{empresa}/normalizado.json', silver)
        gold_amostras = []
        for amostra in amostras:
            resumo = {campo: amostra[campo] for campo in (
                'servidor_id', 'inicio', 'fim', 'cpu', 'ram', 'disco', 'rede', 'processos_completos'
            )}
            processos = amostra['processos']
            escolhidos = {}
            for metrica in ('cpu', 'ram'):
                medidos = [p for p in processos if p[metrica] is not None]
                for processo in sorted(medidos, key=lambda p: p[metrica], reverse=True)[:15]:
                    escolhidos[processo['nome']] = processo
            resumo['processos'] = [escolhidos[nome] for nome in sorted(escolhidos)]
            gold_amostras.append(resumo)
        gold = {
            'versao': 2, 'empresa_id': empresa, 'gerado_em': gerado_em,
            'processos_por_amostra': 15,
            'servidores': sorted({a['servidor_id'] for a in amostras}, key=int),
            'amostras': gold_amostras,
        }
        sufixo = sufixo_dashboard(arquivo)
        for servidor in gold['servidores']:
            gold_servidor = gold.copy()
            gold_servidor['servidores'] = [servidor]
            gold_servidor['amostras'] = [
                amostra for amostra in gold['amostras']
                if amostra['servidor_id'] == servidor
            ]

            salvar_adicional(
                bucket,
                f'gold/{empresa_pasta}/{servidor}/dashboard_{sufixo}.json',
                gold_servidor
            )
    print(f'Dashboard Matheus: {len(por_empresa)} empresa(s); {invalidas} amostra(s) inválida(s).')


def gerar_gestao_problemas_ana(bucket, arquivo_origem, empresa_pasta):
    # Mantém o recorte MENSAL do individual da Ana.
    import re
    from decimal import Decimal

    parametro = (os.getenv('GESTAO_PROBLEMAS_PERIODO') or 'MENSAL').upper()
    if parametro not in ('MENSAL', 'ANUAL'):
        raise ValueError('GESTAO_PROBLEMAS_PERIODO deve ser MENSAL ou ANUAL.')
    data_nome = re.search(r'captura_(\d{4}-\d{2}-\d{2})', arquivo_origem.rsplit('/', 1)[-1])
    if not data_nome:
        raise ValueError('Não foi possível identificar a data do CSV para a gestão de problemas.')
    periodo = data_nome.group(1)[:7 if parametro == 'MENSAL' else 4]
    servidores = {}
    for chave in listar_arquivos_adicionais(bucket, f'bronze/hardware/{empresa_pasta}/', ('.csv',)):
        if not chave.rsplit('/', 1)[-1].startswith('captura_' + periodo):
            continue
        for linha in ler_registros_adicionais(bucket, chave):
            servidor = id_adicional(campo_adicional(linha, 'id', 'servidor_id') or chave.split('/')[3])
            # Igual ao pandas do individual: números são números; vazios ficam null.
            registro = {}
            for campo, valor in linha.items():
                if campo is None:
                    continue
                if valor in (None, ''):
                    registro[campo] = None
                elif campo in ('data_hora', 'inicio', 'fim', 'INICIO_AMOSTRA', 'FIM_AMOSTRA', 'id_mac'):
                    registro[campo] = valor
                else:
                    try:
                        registro[campo] = numero_adicional(valor)
                    except (ValueError, TypeError):
                        registro[campo] = valor
            registro['arquivo_origem'] = chave
            servidores.setdefault(servidor, []).append(registro)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    for servidor, registros in servidores.items():
        registros.sort(key=lambda r: str(r.get('data_hora') or r.get('inicio') or ''))
        salvar_adicional(bucket, f'silver/hardwares/{empresa_pasta}/{servidor}/dash-gestao-problemas_{timestamp}.json', registros)

    # A Gold da Ana depende de metadados reais do MySQL, não de valores fictícios.
    obrigatorias = ('DB_HOST', 'DB_USER', 'DB_NAME')
    if not all(os.getenv(campo) for campo in obrigatorias):
        print('Gold da Ana não gerada: configure DB_HOST, DB_USER, DB_PASSWORD, DB_NAME e DB_PORT.')
        return
    import pymysql
    bd = pymysql.connect(
        host=os.environ['DB_HOST'], user=os.environ['DB_USER'],
        password=os.getenv('DB_PASSWORD') or '', database=os.environ['DB_NAME'],
        port=int(os.getenv('DB_PORT') or os.getenv('PORT') or '3306'),
        cursorclass=pymysql.cursors.DictCursor, connect_timeout=10,
    )
    try:
        with bd.cursor() as cursor:
            coluna_empresa = 'e.idEmpresa' if empresa_pasta.isdigit() else 'e.nome'
            cursor.execute(
                'SELECT equipa.idEquipamento AS id, equipa.nome AS nome, '
                'equipa.valorDowntime AS valorDowntime, equipe.nome AS equipe '
                'FROM empresa AS e JOIN equipamento AS equipa ON equipa.fkEmpresa = e.idEmpresa '
                'JOIN usuario_equipamento AS ue ON ue.fkEquipamento = equipa.idEquipamento '
                'JOIN equipe ON equipe.idEquipe = ue.fkEquipe '
                f'WHERE {coluna_empresa} = %s '
                'GROUP BY equipa.idEquipamento, equipa.nome, equipa.valorDowntime, equipe.nome '
                'ORDER BY equipa.idEquipamento', (empresa_pasta,),
            )
            equipes = cursor.fetchall()
        agrupados = {}
        for linha in equipes:
            valor = linha['valorDowntime']
            servidor = agrupados.setdefault(linha['id'], {
                'id': linha['id'], 'nome': linha['nome'],
                'valorDowntime': float(valor) if isinstance(valor, Decimal) else valor,
                'equipes': [],
            })
            if linha['equipe'] not in servidor['equipes']:
                servidor['equipes'].append(linha['equipe'])
        for identificador, servidor in agrupados.items():
            salvar_adicional(bucket, f'gold/hardwares/{empresa_pasta}/{identificador}/dash-gestao-problemas_{timestamp}.json', servidor)
    finally:
        bd.close()


def carregar_leitor_processos():
    import importlib.util
    caminho = os.path.join(os.path.dirname(__file__), 'script-leitura-processos.py')
    spec = importlib.util.spec_from_file_location('leitor_processos_grupo', caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


# ============================================================
# ENTRADA DA AWS LAMBDA (gatilho: novo CSV na Bronze/hardware)
# ============================================================

PREFIXO_BRONZE_HARDWARE = os.getenv(
    'S3_PREFIX_BRONZE_HARDWARE', 'bronze/hardware/'
).strip('/') + '/'


def _arquivos_do_evento_lambda(event):
    """Extrai (bucket, key) de notificações S3 ou de um evento de teste simples."""
    arquivos = []
    for registro in event.get('Records') or []:
        dados_s3 = registro.get('s3', {})
        bucket = dados_s3.get('bucket', {}).get('name')
        chave = dados_s3.get('object', {}).get('key')
        if chave:
            arquivos.append((bucket, urllib.parse.unquote_plus(chave)))

    # Permite invocar manualmente a função com {"bucket": "...", "key": "..."}.
    if not arquivos and event.get('key'):
        arquivos.append((event.get('bucket'), urllib.parse.unquote_plus(event['key'])))

    return arquivos


# Dashboard de identificação de padrões (Gustavo).
# Evento agendado/manual: {"acao":"gerar_dash_padroes","bucket":"...","empresa":"..."}

FUSO_PADROES = ZoneInfo('America/Sao_Paulo')
HISTORICO_PADROES = 430
EVENTOS_PADROES = ('Pagamento de salários', 'Vencimento de boletos', 'Black Friday', '13º salário (2ª parcela)')


def datas_eventos_padroes(ano, mes):
    primeiro = date(ano, mes, 1)
    uteis = [primeiro + timedelta(days=i) for i in range(9)
             if (primeiro + timedelta(days=i)).weekday() < 5]
    datas = {EVENTOS_PADROES[0]: uteis[4], EVENTOS_PADROES[1]: date(ano, mes, 8)}
    if mes == 11:
        quinta = primeiro + timedelta(days=(3 - primeiro.weekday()) % 7 + 21)
        datas[EVENTOS_PADROES[2]] = quinta + timedelta(days=1)
    if mes == 12:
        datas[EVENTOS_PADROES[3]] = date(ano, mes, 20)
    return datas


def calendario_padroes(inicio, hoje):
    calendario = defaultdict(list)
    for ano in range(inicio.year, hoje.year + 2):
        for mes in range(1, 13):
            for nome, dia in datas_eventos_padroes(ano, mes).items():
                if dia >= inicio:
                    calendario[nome].append(dia)
    return calendario


def dia_vazio_padroes():
    return {'horas': [0] * 24, 'minutos_resolvidos': 0, 'resolvidos': 0}


def sintetico_padroes(empresa, inicio, hoje, calendario):
    especiais = {dia for datas in calendario.values() for dia in datas}
    dias = {}
    for i in range((hoje - inicio).days):
        dia = inicio + timedelta(days=i)
        rnd = random.Random(empresa + ':' + dia.isoformat())
        resumo = dia_vazio_padroes()
        qtd = rnd.randint(0, 4 if dia.weekday() < 5 else 2)
        if dia in especiais:
            qtd += rnd.randint(6, 12)
        for _ in range(qtd):
            hora = rnd.randint(16, 19) if dia in especiais and rnd.random() < .7 else rnd.randrange(24)
            resumo['horas'][hora] += 1
            resumo['minutos_resolvidos'] += rnd.randint(8, 45)
        resumo['resolvidos'] = qtd
        dias[dia.isoformat()] = resumo
    return dias


def ler_json_padroes(bucket, chave):
    try:
        obj = s3.get_object(Bucket=bucket, Key=chave)
    except Exception as erro:
        if getattr(erro, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return {}, None
        raise
    return json.loads(obj['Body'].read()), obj['ETag']


def gravar_json_padroes(bucket, chave, dados, etag):
    # Escrita condicional: uma execução atrasada não substitui outra mais recente.
    condicao = {'IfMatch': etag} if etag else {'IfNoneMatch': '*'}
    s3.put_object(Bucket=bucket, Key=chave, ContentType='application/json',
                  Body=json.dumps(dados, ensure_ascii=False, allow_nan=False).encode(), **condicao)


def instante_padroes(texto):
    valor = datetime.fromisoformat(texto.replace('Z', '+00:00'))
    if valor.tzinfo is None:
        raise ValueError('Horário de chamado sem fuso.')
    return valor


def serie_padroes(dias, inicio, quantidade):
    return [dias.get((inicio + timedelta(days=i)).isoformat(), dia_vazio_padroes())
            for i in range(quantidade)]


def mapa_padroes(dias, hoje, janela):
    inicio = hoje - timedelta(days=janela)
    matriz = [[0] * 24 for _ in range(7)]
    for i, resumo in enumerate(serie_padroes(dias, inicio, janela)):
        linha = matriz[(inicio + timedelta(days=i)).weekday()]
        for hora, qtd in enumerate(resumo['horas']):
            linha[hora] += qtd
    total = sum(map(sum, matriz))
    maximo = max(map(max, matriz))
    topo = sum(sorted(map(sum, zip(*matriz)), reverse=True)[:6])
    atipicos = sum(n for linha in matriz for n in linha if n <= maximo / 4)
    janelas = [(sum(linha[h:h+3]), d, h) for d, linha in enumerate(matriz) for h in range(22)]
    def janela_extrema(funcao):
        if not total:
            return None
        n, d, h = funcao(janelas, key=lambda item: item[0])
        return {'dia_semana': d, 'inicio_hora': h, 'fim_hora': h+3, 'alertas': n}
    return {'janela_dias': janela, 'inicio': inicio.isoformat(), 'fim': (hoje-timedelta(days=1)).isoformat(),
            'contagens': matriz, 'media_semanal': [[n / (janela // 7) for n in linha] for linha in matriz],
            'kpis': {'pct_pico': round(100*topo/total) if total else None,
                     'alertas_atipicos': atipicos, 'pct_atipicos': round(100*atipicos/total) if total else None,
                     'janela_risco': janela_extrema(max), 'janela_manutencao': janela_extrema(min)}}


def previsoes_padroes(dias, calendario, hoje, inicio):
    recentes = serie_padroes(dias, hoje-timedelta(days=30), 30)
    normal = [sum(coluna)/30 for coluna in zip(*(dia['horas'] for dia in recentes))]
    media = sum(normal)
    eventos = []
    for nome, datas in calendario.items():
        futura = min(dia for dia in datas if dia >= hoje)
        passadas = [dia for dia in datas if inicio <= dia < hoje]
        ref = max(passadas) if passadas else None
        evento = {'nome': nome, 'data': futura.isoformat(), 'dias_restantes': (futura-hoje).days,
                  'referencia': ref.isoformat() if ref else None, 'ocorrencias_passadas': len(passadas),
                  'confianca': 'alta' if len(passadas)>=3 else 'média' if len(passadas)==2 else 'baixa',
                  'disponivel': False}
        eventos.append(evento)
        if not ref or ref-timedelta(days=30) < inicio:
            continue
        anterior = serie_padroes(dias, ref-timedelta(days=30), 30)
        base = sum(sum(d['horas']) for d in anterior)/30
        fator = min(2, max(.5, media/base)) if base else 1
        resumo = dias.get(ref.isoformat(), dia_vazio_padroes())
        perfil = resumo['horas']
        total = sum(perfil)
        previsto = math.floor(total*fator + .5)
        duracao = resumo['minutos_resolvidos']/resumo['resolvidos'] if resumo['resolvidos'] else None
        h = max(range(22), key=lambda i: sum(perfil[i:i+3]))
        razao = previsto/media if media else None
        evento.update(disponivel=True, incidentes_referencia=total, incidentes_previstos=previsto,
            fator_crescimento=fator, fator_dia_normal=razao,
            risco=None if razao is None else 'alto' if razao>=3 else 'médio' if razao>=1.5 else 'baixo',
            faixa=[math.floor(previsto*n+.5) for n in (.75,1.25)], duracao_media_min=duracao,
            tempo_incidente_previsto_min=round(previsto*duracao) if duracao is not None else None,
            hora_pico=f'{h}h às {h+3}h' if total else None, perfil_normal=normal, perfil_referencia=perfil,
            perfil_previsto=[round(n*fator,2) for n in perfil])
    return sorted(eventos, key=lambda e: e['data'])[:3]


def gerar_dash_padroes(bucket, empresa, arquivo=None):
    if not bucket or not isinstance(empresa, str) or not empresa.strip() or '/' in empresa:
        raise ValueError('Informe bucket e empresa (nome da pasta Bronze).')
    agora = datetime.now(timezone.utc)
    hoje = agora.astimezone(FUSO_PADROES).date()
    inicio = hoje - timedelta(days=HISTORICO_PADROES)
    calendario = calendario_padroes(inicio, hoje)
    janela = int(os.getenv('JANELA_MAPA_DIAS', '28'))
    if janela < 7 or janela % 7 or janela > HISTORICO_PADROES:
        raise ValueError('Use semanas completas dentro do histórico para JANELA_MAPA_DIAS.')
    chave = f'gold/{empresa}/dash-padroes_{sufixo_dashboard(arquivo)}.json'
    anterior, etag = ler_json_padroes(bucket, chave)
    if (anterior.get('versao') == 2 and anterior.get('data_referencia') == hoje.isoformat()
            and anterior['mapa_calor']['janela_dias'] == janela
            and agora - instante_padroes(anterior['gerado_em']) < timedelta(hours=1)):
        return {'chave': chave, 'atualizado': False}
    dias = sintetico_padroes(empresa, inicio, hoje, calendario)
    gold = {
        'versao': 2, 'empresa': empresa, 'gerado_em': agora.isoformat(),
        'data_referencia': hoje.isoformat(), 'fuso': 'America/Sao_Paulo',
        'fonte': {'mapa_calor': 'sintetico', 'previsibilidade': 'sintetico'},
        'mapa_calor': mapa_padroes(dias, hoje, janela),
        'previsibilidade': {'eventos': previsoes_padroes(dias, calendario, hoje, inicio)},
    }
    gravar_json_padroes(bucket, chave, gold, etag)
    return {'chave': chave, 'atualizado': True}


def lambda_handler(event, context):
    """Processa cada CSV de hardware recebido em uma notificação do S3."""
    if not isinstance(event, dict):
        raise ValueError('O evento da Lambda deve ser um objeto JSON.')

    if event.get("acao") == "gerar_dash_padroes":
        return gerar_dash_padroes(event.get("bucket") or os.getenv("S3_BUCKET") or os.getenv("BUCKET_NAME"), event.get("empresa"))

    arquivos = _arquivos_do_evento_lambda(event)
    if not arquivos:
        print('Evento sem objetos S3 para processar.')
        return {'processados': [], 'ignorados': 0}

    bucket_padrao = os.getenv('S3_BUCKET') or os.getenv('BUCKET_NAME')
    processados = []
    ignorados = 0

    for bucket, chave in arquivos:
        if chave.startswith('bronze/processo/') and chave.lower().endswith('.csv'):
            bucket = bucket or bucket_padrao
            if not bucket:
                raise ValueError('Bucket ausente no evento de processos.')
            carregar_leitor_processos().processar_arquivo_processos_s3(bucket, chave)
            gerar_dashboard_matheus(bucket, chave.split('/')[2], chave)
            processados.append({'bucket': bucket, 'key': chave})
            continue

        if not chave.startswith(PREFIXO_BRONZE_HARDWARE) or not chave.lower().endswith('.csv'):
            print(f'Objeto ignorado fora do prefixo esperado: {chave}')
            ignorados += 1
            continue

        bucket = bucket or bucket_padrao
        if not bucket:
            raise ValueError(
                'Bucket ausente no evento. Configure S3_BUCKET/BUCKET_NAME '
                'ou use uma notificação do S3 com o nome do bucket.'
            )

        processar_arquivo_s3(bucket, chave)
        processados.append({'bucket': bucket, 'key': chave})

    return {'processados': processados, 'ignorados': ignorados}

if __name__ == '__main__':
    BUCKET = os.getenv('S3_BUCKET') or os.getenv('BUCKET_NAME') or 'infrawatch-server-s3'
    # Localiza automaticamente o arquivo .csv mais recente enviado na pasta bronze/hardware/
    ARQUIVO_MAIS_RECENTE = buscar_ultimo_arquivo_s3(
        BUCKET, prefixo='bronze/hardware/'
    )
    processar_arquivo_s3(BUCKET, ARQUIVO_MAIS_RECENTE)

