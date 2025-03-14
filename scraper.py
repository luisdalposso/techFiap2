import requests
import pandas as pd
import pyarrow as pa
import boto3
from io import BytesIO
import datetime
import pyarrow.parquet as pq
from unidecode import unidecode
import base64

# Configurações iniciais
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
file_name = f"carteira_diaria_{current_date}"
url = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetDownloadPortfolioDay/eyJpbmRleCI6IklCT1YiLCJsYW5ndWFnZSI6InB0LWJyIn0='
bucket_name = 'tech-challenge2-ibov'

# Download do CSV
response = requests.get(url, headers={'accept': 'application/json, text/plain, */*'})
response.raise_for_status()

# Decodificação e limpeza do conteúdo
csv_content = response.content.decode('latin-1')
csv_content = base64.b64decode(csv_content).decode('latin-1')
csv_lines = csv_content.split('\n')[2:-3]
csv_lines = [unidecode(line.rstrip(';\r').replace('.','').replace('/','')) for line in csv_lines]
csv_content = '\n'.join(csv_lines)

# Leitura e transformação do DataFrame
df = pd.read_csv(StringIO(csv_content), encoding='latin-1', sep=';', header=None)
df.columns = ['cod_acao', 'desc_acao', 'tipo_acao', 'qtde_teorica', 'perc_part']
df['data'] = current_date
df['qtde_teorica'] = df['qtde_teorica'].astype(int)

# Conversão para Parquet
table = pa.Table.from_pandas(df)
parquet_buffer = BytesIO()
pq.write_table(table, parquet_buffer)

# Upload para S3
s3_client = boto3.client('s3', region_name='us-east-1')
s3_client.put_object(Bucket=bucket_name, Key=f'raw/{file_name}.parquet', Body=parquet_buffer.getvalue())

print(f"Arquivo salvo com sucesso no bucket {bucket_name} como {file_name}.parquet")