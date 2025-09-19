import os
import sys
import re
import time
import pathlib
import zipfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg2
import requests
import wget
import bs4 as bs
from sqlalchemy import create_engine
from dotenv import load_dotenv


class RfbEtl:
    def __init__(self):
        current_path = pathlib.Path().resolve()
        dotenv_path = os.path.join(current_path, '../utils', '.env')
        if not os.path.isfile(dotenv_path):
            raise FileNotFoundError("Arquivo .env não encontrado em utils/")
        load_dotenv(dotenv_path=dotenv_path)

        # Diretórios
        self.output_files = os.getenv("OUTPUT_FILES_PATH")
        self.extracted_files = os.getenv("EXTRACTED_FILES_PATH")
        self.rf_data_url = os.getenv("RF_DATA_URL")

        self.makedirs(self.output_files)
        self.makedirs(self.extracted_files)

        # Banco
        self.db_user = os.getenv("DB_USER")
        self.db_pass = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_name = os.getenv("DB_NAME")

        try:
            self.engine = create_engine(
                f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
            )
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                host=self.db_host,
                port=self.db_port,
                password=self.db_pass
            )
            self.cur = self.conn.cursor()
            print(f"Conectado com sucesso ao banco: {self.db_name}@{self.db_host}:{self.db_port}")
        except Exception as e:
            print(f"Erro ao conectar no banco de dados: {e}")
            sys.exit(1)

    @staticmethod
    def makedirs(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def check_diff(url, file_name):
        if not os.path.isfile(file_name):
            return True
        response = requests.head(url)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True
        return False

    def list_files(self):
        raw_html = urllib.request.urlopen(self.rf_data_url).read()
        page_items = bs.BeautifulSoup(raw_html, 'lxml')
        html_str = str(page_items)

        files = []
        for m in re.finditer('.zip', html_str):
            i_start = m.start() - 40
            i_end = m.end()
            i_loc = html_str[i_start:i_end].find('href=') + 6
            files.append(html_str[i_start + i_loc:i_end])

        files_clean = [f for f in files if not f.find('.zip">') > -1]
        return files_clean

    def download_file(self, url):
        file_name = os.path.join(self.output_files, os.path.basename(url))
        if self.check_diff(url, file_name):
            print(f"Baixando {file_name}...")
            wget.download(url, out=self.output_files)
        else:
            print(f"Já existe e está atualizado: {file_name}")
        return file_name

    def download_files(self, max_workers=2):
        files = self.list_files()
        urls = [self.rf_data_url + f for f in files]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.download_file, url) for url in urls]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    print(f"\n✔ Finalizado: {result}")
                except Exception as e:
                    print(f"\n❌ Erro no download: {e}")

    def extract_files(self):
        files = [f for f in os.listdir(self.output_files) if f.endswith(".zip")]
        for i, f in enumerate(files, 1):
            print(f"Extraindo {i}/{len(files)}: {f}")
            full_path = os.path.join(self.output_files, f)
            try:
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    zip_ref.extractall(self.extracted_files)
                os.remove(full_path)
            except Exception as e:
                print(f"Erro ao extrair {f}: {e}")

        print("Arquivos presentes em EXTRACTED_FILES_PATH:")
        for f in os.listdir(self.extracted_files):
            print(f" - {f}")

    def to_sql(self, dataframe, **kwargs):
        try:
            dataframe.to_sql(**kwargs, method='multi', chunksize=10000)
        except Exception as e:
            print(f"Erro ao inserir {kwargs.get('name')}: {e}")
            print("Tentando inserir sem method='multi'...")
            try:
                dataframe.to_sql(**kwargs)
            except Exception as e2:
                print(f"Falha completa na inserção: {e2}")
                raise e2

    def _init_state_table(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_processed_files (
            file_name TEXT PRIMARY KEY,
            table_name TEXT,
            processed_at TIMESTAMP,
            status TEXT,
            rows_inserted BIGINT DEFAULT 0,
            error TEXT
        );
        """)
        self.conn.commit()

    def is_processed(self, file_name):
        self.cur.execute("SELECT status FROM etl_processed_files WHERE file_name = %s;", (file_name,))
        row = self.cur.fetchone()
        return bool(row and row[0] == 'done')

    def mark_processed(self, file_name, table_name, rows_inserted=0, status='done', error=None):
        self.cur.execute("""
            INSERT INTO etl_processed_files(file_name, table_name, processed_at, status, rows_inserted, error)
            VALUES (%s, %s, NOW(), %s, %s, %s)
            ON CONFLICT (file_name) DO UPDATE
            SET table_name = EXCLUDED.table_name,
                processed_at = EXCLUDED.processed_at,
                status = EXCLUDED.status,
                rows_inserted = EXCLUDED.rows_inserted,
                error = EXCLUDED.error;
        """, (file_name, table_name, status, rows_inserted, error))
        self.conn.commit()

    def _match_files(self, substr_list):
        files = os.listdir(self.extracted_files)
        matched = []
        for f in files:
            for sub in substr_list:
                if sub.lower() in f.lower():
                    matched.append(f)
                    break
        return matched

    def _insert_df(self, file_path, table_name, columns_expected=None, chunksize=1_000_000):
        total_rows = sum(1 for _ in open(file_path, encoding='latin-1'))
        inserted_rows = 0
        chunk_num = 0
        start_time = time.time()

        for chunk in pd.read_csv(
            file_path,
            sep=';',
            header=None,
            encoding='latin-1',
            dtype=str,
            chunksize=chunksize
        ):
            chunk_num += 1
            if columns_expected and len(chunk.columns) == len(columns_expected):
                chunk.columns = columns_expected

            self.to_sql(chunk, name=table_name, con=self.engine, if_exists='append', index=False)

            inserted_rows += len(chunk)

            elapsed = time.time() - start_time
            rows_per_sec = inserted_rows / elapsed if elapsed > 0 else 0
            eta = (total_rows - inserted_rows) / rows_per_sec if rows_per_sec > 0 else 0

            percent = (inserted_rows / total_rows) * 100 if total_rows else 0
            print(f"[{table_name}] Chunk {chunk_num} -> {inserted_rows:,}/{total_rows:,} "
                  f"linhas ({percent:.2f}%) - ETA {eta/60:.1f} min")

        elapsed_total = time.time() - start_time
        print(f"✅ Finalizado {table_name} - {inserted_rows:,}/{total_rows:,} em {elapsed_total/60:.1f} min")
        return inserted_rows

    def load_data(self, drop_tables=False, resume=True, force=False):
        start = time.time()
        self._init_state_table()

        file_categories = {
            "estabelecimento": ['ESTABELE'],
            "empresa": ['EMPRE'],
            "socios": ['SOCIO'],
            "simples": ['SIMPLES'],
            "cnae": ['CNAE'],
            "moti": ['MOTI'],
            "munic": ['MUNIC'],
            "natju": ['NATJU'],
            "pais": ['PAIS'],
            "quals": ['QUALS']
        }

        expected_columns = {
            "estabelecimento": [
                'cnpj_basico','cnpj_ordem','cnpj_dv','identificador_matriz_filial',
                'nome_fantasia','situacao_cadastral','data_situacao_cadastral','motivo_situacao_cadastral',
                'nome_cidade_exterior','pais','data_inicio_atividade','cnae_fiscal_principal','cnae_fiscal_secundaria',
                'tipo_logradouro','logradouro','numero','complemento','bairro','cep','uf','municipio',
                'ddd1','telefone1','ddd2','telefone2','ddd_fax','fax','correio_eletronico','situacao_especial','data_situacao_especial'
            ],
            "empresa": ['cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel','capital_social','porte','ente_federativo'],
            "socios": ['cnpj_basico','identificador_socio','nome_socio','cnpj_cpf_socio','qualificacao_socio','data_entrada_sociedade',
                       'pais','representante_legal','nome_representante','qualificacao_representante','faixa_etaria'],
            "simples": ['cnpj_basico','opcao_simples','data_opcao_simples','data_exclusao_simples','opcao_mei','data_opcao_mei','data_exclusao_mei'],
            "cnae": ['codigo','descricao'],
            "moti": ['codigo','descricao'],
            "munic": ['codigo','descricao'],
            "natju": ['codigo','descricao'],
            "pais": ['codigo','descricao'],
            "quals": ['codigo','descricao']
        }

        for table, substr_list in file_categories.items():
            if drop_tables:
                print(f"Dropping table {table} se existir (opção --drop ativada)...")
                try:
                    self.cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
                    self.conn.commit()
                except Exception as e:
                    print(f"Erro ao dropar {table}: {e}")

            matched_files = self._match_files(substr_list)
            if not matched_files:
                print(f"Nenhum arquivo encontrado para {table}, pulando...")
                continue

            print(f"Carregando {table} ({len(matched_files)} arquivo(s))...")

            for file in matched_files:
                if resume and not force and self.is_processed(file):
                    print(f"Pulando {file} (já processado). Use --force para reprocessar.")
                    continue

                path = os.path.join(self.extracted_files, file)
                print(f"Iniciando arquivo {file} -> tabela {table}")
                try:
                    rows_inserted = self._insert_df(path, table_name=table, columns_expected=expected_columns.get(table))
                    self.mark_processed(file_name=file, table_name=table, rows_inserted=rows_inserted, status='done', error=None)
                except Exception as e:
                    print(f"❌ Erro processando {file}: {e}")
                    try:
                        self.mark_processed(file_name=file, table_name=table, rows_inserted=0, status='failed', error=str(e))
                    except Exception:
                        pass
                    continue

        end = time.time()
        print(f"Carga concluída em {round((end-start)/60, 2)} minutos")

    def create_indexes(self):
        self.cur.execute("""
        create index if not exists empresa_cnpj on empresa(cnpj_basico);
        create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
        create index if not exists socios_cnpj on socios(cnpj_basico);
        create index if not exists simples_cnpj on simples(cnpj_basico);
        """)
        self.conn.commit()
        print("Índices criados!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL RFB")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--extract", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--index", action="store_true")
    parser.add_argument("--drop", action="store_true", help="Dropar tabelas antes de carregar (use com cuidado)")
    parser.add_argument("--force", action="store_true", help="Forçar reprocessamento de arquivos já processados")
    args = parser.parse_args()

    etl = RfbEtl()

    if args.download:
        etl.download_files()
    if args.extract:
        etl.extract_files()
    if args.load:
        etl.load_data(drop_tables=args.drop, resume=True, force=args.force)
    if args.index:
        etl.create_indexes()