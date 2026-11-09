        
import csv
import io
import json
import urllib.parse
import boto3

s3 = boto3.client('s3')

def buscar_ultimo_arquivo_processos_s3(bucket, prefixo='bronze/processo/'):
    resposta = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo)
    if 'Contents' not in resposta:
        raise FileNotFoundError(
            f'Nenhum arquivo encontrado no bucket {bucket} com o prefixo {prefixo}'
        )
    arquivos_csv = [
        obj for obj in resposta['Contents'] if obj['Key'].endswith('.csv')
    ]
    if not arquivos_csv:
        raise FileNotFoundError(
            f'Nenhum arquivo .csv encontrado em s3://{bucket}/{prefixo}'
        )
    # Ordena pelo campo LastModified para pegar o arquivo mais recente
    arquivo_mais_recente = sorted(
        arquivos_csv, key=lambda x: x['LastModified'], reverse=True
    )
    return arquivo_mais_recente[0]['Key']
                                                                                                                                                                                                                                                                                                                                                                                                                      
def processar_arquivo_processos_s3(bucket, arquivo):
    arquivo = urllib.parse.unquote_plus(arquivo)
    print(f'Processando arquivo de processos: s3://{bucket}/{arquivo}')
    partes = arquivo.split('/')
    # Estrutura: bronze / processo / empresa / equipamento / arquivo.csv
    if len(partes) != 5:
        raise ValueError(
            f'Caminho inválido: {arquivo}. Esperado:'
            ' bronze/processo/empresa/equipamento/arquivo.csv'
        )
    if partes[0] != 'bronze':
        raise ValueError(f'O arquivo não está na camada Bronze: {arquivo}')
    categoria = partes[1]       # 'processo'
    empresa = partes[2]         # 'Bananinha Ltda'
    equipamento = partes[3]     # '12'
    nome_arquivo = partes[4]    # 'captura_processos_....csv'
    
    if categoria != 'processo':
        print(
            f"Arquivo de categoria '{categoria}' ignorado por esta rotina de"
            ' processos.'
        )
        return

    print(f'Categoria: {categoria}')
    print(f'Empresa: {empresa}')
    print(f'Equipamento: {equipamento}')
    print(f'Arquivo: {nome_arquivo}')
    
    # =========================================================
    # LÊ O CSV DA CAMADA BRONZE (PROCESSO)
    # =========================================================
    resposta = s3.get_object(Bucket=bucket, Key=arquivo)
    conteudo = resposta['Body'].read().decode('utf-8')
    csv_buffer = io.StringIO(conteudo)
    leitor = csv.DictReader(csv_buffer, delimiter=';')
    registros = list(leitor)
    if not registros:
        raise ValueError('O arquivo CSV de processos não possui registros.')

    # =========================================================
    # TRATAMENTO DOS DADOS DOS PROCESSOS
    # =========================================================
    dados_tratados = []
    for linha in registros:
        registro = {}
        for campo, valor in linha.items():
            if valor is not None:
                valor = valor.strip()
            else:
                valor = ''
            if campo in ['PID', 'Processo', 'CAMINHO']:
                if campo == 'PID':
                    try:
                        registro[campo] = int(valor)
                    except ValueError:
                        registro[campo] = valor
                else:
                    registro[campo] = valor
            else:
                try:
                    registro[campo] = float(valor)
                except ValueError:
                    registro[campo] = valor
        dados_tratados.append(registro)

    quantidade_processos = len(dados_tratados)
    
    # Identifica colunas numéricas (ex: RAM, TAMANHO)
    metricas = []
    if dados_tratados:
        for campo, valor in dados_tratados[0].items():
            if campo not in ['PID', 'Processo', 'CAMINHO'] and isinstance(
                valor, (int, float)
            ):
                metricas.append(campo)
    print(f'Métricas numéricas encontradas: {metricas}')
    
    # =========================================================
    # CÁLCULO ESTATÍSTICO GERAL
    # =========================================================
    estatisticas = {}
    for metrica in metricas:
        valores = [
            registro[metrica] for registro in dados_tratados if isinstance(registro[metrica], (int, float))
        ]

    if valores:
            estatisticas[metrica] = {
                'media': round(sum(valores) / len(valores), 2),
                'minimo': round(min(valores), 2),
                'maximo': round(max(valores), 2),
                'total': round(sum(valores), 2),
            }

    # =========================================================
    # TOP 5 PROCESSOS QUE MAIS CONSUMIRAM RECURSOS
    # =========================================================
    chave_ordenacao = (
        'RAM' if 'RAM' in metricas else (metricas[0] if metricas else None)
    )
    if chave_ordenacao:
        top_5_processos = sorted(
            [
                p for p in dados_tratados if isinstance(p.get(chave_ordenacao), (int, float))
            ],
            key=lambda x: x[chave_ordenacao],
            reverse=True,
        )[:5]
    else:
        top_5_processos = dados_tratados[:5]

    # =========================================================
    # MONTA E SALVA A CAMADA SILVER
    # =========================================================
    silver = {
        'empresa': empresa,
        'equipamento': equipamento,
        'quantidade_processos': quantidade_processos,
        'metricas_gerais': estatisticas,
        'processos': dados_tratados,
    }
    nome_silver = nome_arquivo.replace('.csv', '.json')
    arquivo_silver = f'silver/{empresa}/{equipamento}/{nome_silver}'
    s3.put_object(
        Bucket=bucket,
        Key=arquivo_silver,
        Body=json.dumps(silver, ensure_ascii=False, indent=4).encode('utf-8'),
        ContentType='application/json',
    )
    print(f'Arquivo Silver salvo em: s3://{bucket}/{arquivo_silver}')
    
    # =========================================================
    # MONTA E SALVA A CAMADA GOLD (COM TOP 5 PROCESSOS)
    # =========================================================
    gold = {
        'empresa': empresa,
        'equipamento': equipamento,
        'quantidade_processos': quantidade_processos,
        'top_5_processos': top_5_processos,
    }
    for metrica, valores in estatisticas.items():
        gold[f'{metrica}_media'] = valores['media']
        gold[f'{metrica}_total'] = valores['total']

    nome_gold = nome_arquivo.replace('.csv', '.json')
    arquivo_gold = f'gold/{empresa}/{equipamento}/{nome_gold}'
    s3.put_object(
        Bucket=bucket,
        Key=arquivo_gold,
        Body=json.dumps(gold, ensure_ascii=False, indent=4).encode('utf-8'),
        ContentType='application/json',
    )
    print(f'Arquivo Gold salvo em: s3://{bucket}/{arquivo_gold}')
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'mensagem': 'Arquivo de processos processado com sucesso',
            'bronze': arquivo,
            'silver': arquivo_silver,
            'gold': arquivo_gold,
        }),
    }

if __name__ == '__main__':
    BUCKET = 'infrawatch-server-s3'
    ARQUIVO_PROCESSO_MAIS_RECENTE = buscar_ultimo_arquivo_processos_s3(
        BUCKET,
        prefixo='bronze/processo/'
    )
    processar_arquivo_processos_s3(BUCKET, ARQUIVO_PROCESSO_MAIS_RECENTE)
