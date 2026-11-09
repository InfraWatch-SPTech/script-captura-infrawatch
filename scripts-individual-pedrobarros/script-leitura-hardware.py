                                                                                                                                                                                                                                                                                                                                                                                                                                           
import boto3
import csv
import io
import json
import urllib.parse
import math
from datetime import datetime, timedelta, timezone

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

def processar_arquivo_s3(bucket, arquivo):
    arquivo = urllib.parse.unquote_plus(arquivo)
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
    
    if categoria == 'processo':
        print(f'Arquivo de processos {nome_arquivo} ignorado.')

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
            if campo == 'id' or campo == 'data_hora':
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

    inicio = dados_tratados[0]['data_hora']
    fim = dados_tratados[-1]['data_hora']
    
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
            instante = _timestamp_previsibilidade(linha.get('data_hora'))
            if instante is None:
                continue
            registro = {'data_hora': instante.isoformat(sep=' '), '_instante': instante}
            for campo, valor in linha.items():
                if campo in ('id', 'data_hora') or campo is None:
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

if __name__ == '__main__':
    BUCKET = 'infrawatch-server-s3'
    # Localiza automaticamente o arquivo .csv mais recente enviado na pasta bronze/hardware/
    ARQUIVO_MAIS_RECENTE = buscar_ultimo_arquivo_s3(
        BUCKET, prefixo='bronze/hardware/'
    )
    processar_arquivo_s3(BUCKET, ARQUIVO_MAIS_RECENTE)

