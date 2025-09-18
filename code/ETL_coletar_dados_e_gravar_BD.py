import os
import sys
import pathlib
import re
import time
import gc
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import requests
from bs4 import BeautifulSoup
import zipfile
import shutil
import pandas as pd
import psycopg2
from psycopg2 import sql

# ---------------------------
# Config & util
# ---------------------------
current_path = pathlib.Path().resolve()
# Look for .env inside a utils folder relative to project root
dotenv_path_candidate = current_path.joinpath('../utils', '.env')
if not dotenv_path_candidate.exists():
    dotenv_path_candidate = current_path.joinpath('.env')
    if not dotenv_path_candidate.exists():
        raise FileNotFoundError('Arquivo .env não encontrado em ./utils nem na raiz do projeto. Coloque o .env em utils/.env')

load_dotenv(dotenv_path=str(dotenv_path_candidate))

# Read paths and urls from env (mandatory)
OUTPUT_FILES_PATH = os.getenv('OUTPUT_FILES_PATH')
EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
RF_DATA_URL = os.getenv('RF_DATA_URL')

if not OUTPUT_FILES_PATH or not EXTRACTED_FILES_PATH or not RF_DATA_URL:
    raise EnvironmentError('Variáveis OUTPUT_FILES_PATH, EXTRACTED_FILES_PATH e RF_DATA_URL devem existir no .env')

os.makedirs(OUTPUT_FILES_PATH, exist_ok=True)
os.makedirs(EXTRACTED_FILES_PATH, exist_ok=True)

print(f'OUTPUT_FILES_PATH={OUTPUT_FILES_PATH}\nEXTRACTED_FILES_PATH={EXTRACTED_FILES_PATH}\nRF_DATA_URL={RF_DATA_URL}')

# DB settings from env
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise EnvironmentError('Variáveis de conexão com o BD não encontradas no .env')

# Create sqlalchemy engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
                       pool_size=5, max_overflow=10)

# A small helper to stream-download a file using requests and write it to disk in chunks
def download_file(url: str, dest_path: str):
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        with open(dest_path, 'wb') as f:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 / total
                        sys.stdout.write(f'\r{os.path.basename(dest_path)} {pct:.2f}% ({downloaded}/{total})')
                        sys.stdout.flush()
    print('\n')

# ---------------------------
# Discover files on RF index page
# ---------------------------
print('Buscando lista de arquivos em', RF_DATA_URL)
resp = requests.get(RF_DATA_URL, timeout=30)
resp.raise_for_status()
page = BeautifulSoup(resp.content, 'lxml')
links = [a.get('href') for a in page.find_all('a', href=True) if a.get('href').lower().endswith('.zip')]
# Normalize links that might be relative
Files = []
for l in links:
    if l.startswith('http'):
        Files.append(l)
    else:
        Files.append(os.path.join(RF_DATA_URL, l))

print(f'Foram encontrados {len(Files)} arquivos .zip')
for i, f in enumerate(Files, 1):
    print(f'{i} - {f}')

# ---------------------------
# Download files (only if changed or missing)
# ---------------------------

def needs_download(url: str, local_path: str) -> bool:
    if not os.path.exists(local_path):
        return True
    try:
        head = requests.head(url, timeout=20)
        head.raise_for_status()
        remote_size = int(head.headers.get('content-length', 0))
        local_size = os.path.getsize(local_path)
        return remote_size != local_size
    except Exception:
        # If HEAD fails, fallback to not re-downloading
        return False

for remote_url in Files:
    fname = os.path.basename(remote_url)
    dest = os.path.join(OUTPUT_FILES_PATH, fname)
    if needs_download(remote_url, dest):
        print('Baixando:', remote_url)
        download_file(remote_url, dest)
    else:
        print('Já existe e parece igual, pulando:', fname)

# ---------------------------
# Extract and remove zips to free disk space
# ---------------------------
print('Descompactando arquivos...')
for file in os.listdir(OUTPUT_FILES_PATH):
    if file.lower().endswith('.zip'):
        full = os.path.join(OUTPUT_FILES_PATH, file)
        try:
            with zipfile.ZipFile(full, 'r') as z:
                z.extractall(EXTRACTED_FILES_PATH)
            print('Extraído:', file)
            # Remove zip to free space
            os.remove(full)
            print('Removido zip:', file)
        except Exception as e:
            print('Falha ao extrair', file, e)

# ---------------------------
# Utility: optimized to_sql using chunks and method='multi'
# ---------------------------

def insert_dataframe(df: pd.DataFrame, table_name: str, engine, if_exists='append', chunksize=5000):
    """Insert dataframe into Postgres using pandas.to_sql with method='multi' and explicit chunksize.
       Converts object columns with low cardinality to category to reduce memory while in pandas.
    """
    # Reduce memory: convert object columns with low cardinality to category (helps pandas memory)
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique(dropna=False) / max(1, len(df)) < 0.5:
            df[col] = df[col].astype('category')

    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False, method='multi', chunksize=chunksize)

# ---------------------------
# Map extracted filenames to logical groups (the original script logic)
# ---------------------------
items = [f for f in os.listdir(EXTRACTED_FILES_PATH)]
arquivos = {
    'empresa': [f for f in items if 'EMPRE' in f],
    'estabelecimento': [f for f in items if 'ESTABELE' in f],
    'socios': [f for f in items if 'SOCIO' in f],
    'simples': [f for f in items if 'SIMPLES' in f],
    'cnae': [f for f in items if 'CNAE' in f],
    'moti': [f for f in items if 'MOTI' in f],
    'munic': [f for f in items if 'MUNIC' in f],
    'natju': [f for f in items if 'NATJU' in f],
    'pais': [f for f in items if 'PAIS' in f],
    'quals': [f for f in items if 'QUALS' in f],
}

# ---------------------------
# DB connection helper for COPY (faster loads) - simple fallback using to_sql when needed
# ---------------------------

def get_psycopg_conn():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

# We'll use a combination: for very large files use chunked read_csv + to_sql; for small ones just full read

# Example: empresa (smaller-ish) - keep original columns mapping but use chunking
if arquivos['empresa']:
    print('\n### Processando arquivos EMPRESA ###')
    with get_psycopg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS empresa;')
        conn.commit()

    for f in arquivos['empresa']:
        path = os.path.join(EXTRACTED_FILES_PATH, f)
        dtypes = {0: 'object', 1: 'object', 2: 'Int64', 3: 'Int64', 4: 'object', 5: 'Int64', 6: 'object'}
        # read in chunks
        reader = pd.read_csv(path, sep=';', header=None, dtype=dtypes, encoding='latin-1', chunksize=100000)
        first = True
        for chunk in reader:
            chunk.reset_index(drop=True, inplace=True)
            chunk.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
            # convert capital_social
            chunk['capital_social'] = chunk['capital_social'].astype(str).str.replace(',', '.').replace('nan', None)
            chunk['capital_social'] = pd.to_numeric(chunk['capital_social'], errors='coerce')
            insert_dataframe(chunk, 'empresa', engine, if_exists='append', chunksize=5000)
            del chunk
            gc.collect()
        print('Concluído:', f)

# estabelecimento: muito grande -> chunked processing already present in original; we use chunksize iterator and to_sql with method='multi'
if arquivos['estabelecimento']:
    print('\n### Processando arquivos ESTABELECIMENTO ###')
    with get_psycopg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS estabelecimento;')
        conn.commit()

    for f in arquivos['estabelecimento']:
        path = os.path.join(EXTRACTED_FILES_PATH, f)
        dtypes = {0: 'object', 1: 'object', 2: 'object', 3: 'Int64', 4: 'object', 5: 'Int64', 6: 'Int64',
                  7: 'Int64', 8: 'object', 9: 'object', 10: 'Int64', 11: 'Int64', 12: 'object', 13: 'object',
                  14: 'object', 15: 'object', 16: 'object', 17: 'object', 18: 'object', 19: 'object',
                  20: 'Int64', 21: 'object', 22: 'object', 23: 'object', 24: 'object', 25: 'object',
                  26: 'object', 27: 'object', 28: 'object', 29: 'Int64'}
        reader = pd.read_csv(path, sep=';', header=None, dtype=dtypes, encoding='latin-1', chunksize=200000)
        for i, chunk in enumerate(reader):
            chunk.reset_index(drop=True, inplace=True)
            chunk.columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
                             'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
                             'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro',
                             'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
                             'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
            insert_dataframe(chunk, 'estabelecimento', engine, if_exists='append', chunksize=5000)
            print(f'Inserido chunk {i} do arquivo {f}')
            del chunk
            gc.collect()
        print('Concluído:', f)

# socios
if arquivos['socios']:
    print('\n### Processando arquivos SOCIOS ###')
    with get_psycopg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS socios;')
        conn.commit()

    for f in arquivos['socios']:
        path = os.path.join(EXTRACTED_FILES_PATH, f)
        dtypes = {0: 'object', 1: 'Int64', 2: 'object', 3: 'object', 4: 'Int64', 5: 'Int64', 6: 'Int64',
                  7: 'object', 8: 'object', 9: 'Int64', 10: 'Int64'}
        reader = pd.read_csv(path, sep=';', header=None, dtype=dtypes, encoding='latin-1', chunksize=200000)
        for chunk in reader:
            chunk.reset_index(drop=True, inplace=True)
            chunk.columns = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
                             'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
                             'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']
            insert_dataframe(chunk, 'socios', engine, if_exists='append', chunksize=5000)
            del chunk
            gc.collect()
        print('Concluído:', f)

# simples (split/partitions) - read in chunks
if arquivos['simples']:
    print('\n### Processando arquivos SIMPLES ###')
    with get_psycopg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS simples;')
        conn.commit()

    for f in arquivos['simples']:
        path = os.path.join(EXTRACTED_FILES_PATH, f)
        # We will iterate by chunks instead of counting lines upfront (more memory friendly)
        dtypes = {0: 'object', 1: 'object', 2: 'Int64', 3: 'Int64', 4: 'object', 5: 'Int64', 6: 'Int64'}
        reader = pd.read_csv(path, sep=';', header=None, dtype=dtypes, encoding='latin-1', chunksize=500000)
        for i, chunk in enumerate(reader):
            chunk.reset_index(drop=True, inplace=True)
            chunk.columns = ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples',
                             'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
            insert_dataframe(chunk, 'simples', engine, if_exists='append', chunksize=5000)
            print(f'Inserido chunk {i} do arquivo {f}')
            del chunk
            gc.collect()
        print('Concluído:', f)

# The smaller lookup tables (cnae, moti, munic, natju, pais, quals) can be read fully and inserted
for tbl in ['cnae', 'moti', 'munic', 'natju', 'pais', 'quals']:
    files = arquivos.get(tbl) or []
    if not files:
        continue
    print(f'\n### Processando {tbl.upper()} ###')
    with get_psycopg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier(tbl)))
        conn.commit()

    for f in files:
        path = os.path.join(EXTRACTED_FILES_PATH, f)
        df = pd.read_csv(path, sep=';', header=None, dtype='object', encoding='latin-1')
        df.reset_index(drop=True, inplace=True)
        df.columns = ['codigo', 'descricao']
        insert_dataframe(df, tbl, engine, if_exists='append', chunksize=5000)
        del df
        gc.collect()
        print('Concluído:', f)

# ---------------------------
# Create indexes (if not exists)
# ---------------------------
print('\nCriando índices...')
with engine.begin() as conn:
    conn.execute(text('create index if not exists empresa_cnpj on empresa(cnpj_basico);'))
    conn.execute(text('create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);'))
    conn.execute(text('create index if not exists socios_cnpj on socios(cnpj_basico);'))
    conn.execute(text('create index if not exists simples_cnpj on simples(cnpj_basico);'))

print('Processo finalizado!')