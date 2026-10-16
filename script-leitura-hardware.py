                                                                                                                                                                                                                                                                                                                                                                                                                                           
import boto3
import csv
import io
import json
import urllib.parse

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

if __name__ == '__main__':
    BUCKET = 'infrawatch-server-s3'
    # Localiza automaticamente o arquivo .csv mais recente enviado na pasta bronze/hardware/
    ARQUIVO_MAIS_RECENTE = buscar_ultimo_arquivo_s3(
        BUCKET, prefixo='bronze/hardware/'
    )
    processar_arquivo_s3(BUCKET, ARQUIVO_MAIS_RECENTE)

