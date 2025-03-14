import requests
import pandas as pd
import pyarrow as pa
import boto3
from io import StringIO, BytesIO
import base64
import datetime
import pyarrow.parquet as pq
from unidecode import unidecode

# Obtendo a data atual e formatando o nome do arquivo
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
file_name = f"carteira_diaria_{current_date}"

# URL para download do arquivo CSV
url = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetDownloadPortfolioDay/eyJpbmRleCI6IklCT1YiLCJsYW5ndWFnZSI6InB0LWJyIn0='

# Fazendo o download do arquivo CSV
response = requests.get(url, headers={'accept': 'application/json, text/plain, */*'})
response.raise_for_status()

# Lendo o conteúdo do CSV
csv_content_base64 = response.content.decode('latin-1')
csv_content = base64.b64decode(csv_content_base64).decode('latin-1')

# Processando o conteúdo do CSV para remover a primeira e a última linha que não são dados em CSV
csv_lines = csv_content.split('\n')
csv_lines = csv_lines[2:-3]  # Remove a primeira e a última 2 linhas
csv_lines = [unidecode(line.rstrip(';\r').replace('.','').replace('/','')) for line in csv_lines]  # Remove o ';' do final de cada linha
csv_content = '\n'.join(csv_lines)

# Escrevendo o conteúdo do CSV para um arquivo local
with open(f"{file_name}.csv", "w") as f:
     f.write(csv_content)

# define o type de cada coluna
data_type = {
    'cod_acao': 'str',
    'desc_acao': 'str',
    'tipo_acao': 'str',
    'qtde_teorica': 'str',
    'perc_part': 'str',
    'data': 'datetime64[ns]'
}

# Lendo o arquivo CSV
df = pd.read_csv(f"{file_name}.csv", encoding='latin-1', sep=';', header=None)

# Renomeando as colunas usando seus índices
df.columns = ['cod_acao', 'desc_acao', 'tipo_acao', 'qtde_teorica', 'perc_part']

# Adiconando coluna data
df['data'] = current_date

# Convert the 'volume' column to int
df['qtde_teorica'] = df['qtde_teorica'].astype(int)

# Convertendo o DataFrame para Parquet
table = pa.Table.from_pandas(df)
parquet_buffer = BytesIO()
pq.write_table(table, parquet_buffer)

# # Escrevendo o conteúdo do buffer Parquet para um arquivo local
with open(f"{file_name}.parquet", "wb") as f:
     f.write(parquet_buffer.getvalue())

# Configurando o cliente S3
s3_client = boto3.client('s3', region_name='us-east-1')

# Nome do bucket e do arquivo
bucket_name = 'tech-challenge2-ibov'

parquet_file_name = f'{file_name}.parquet'

# Salvando o arquivo Parquet no bucket S3
s3_client.put_object(Bucket=bucket_name, Key=f'raw/{parquet_file_name}', Body=parquet_buffer.getvalue())

print(f"Arquivo salvo com sucesso no bucket {bucket_name} com o nome {parquet_file_name}")