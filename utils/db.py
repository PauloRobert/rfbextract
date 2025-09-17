import os
import time
from contextlib import contextmanager
from typing import Dict, List, Any, Generator, Optional, Tuple, Union

import polars as pl
import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool, sql
from psycopg2.extras import execute_values

from utils.logging import logger

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "Dados_RFB")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 10))

class DatabaseManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.connection_pool = None
        self.schema_definitions = self._get_schema_definitions()

    def create_pool(self, min_conn=1, max_conn=10, database=None):
        try:
            if self.connection_pool is not None:
                self.connection_pool.closeall()

            db_name = database or DB_NAME

            self.connection_pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=db_name
            )
            logger.info(f"Conexão estabelecida com {db_name}", host=DB_HOST, pool_size=max_conn)
        except Exception as e:
            logger.critical(f"Falha ao conectar com {database or DB_NAME}", exception=e)
            raise

    @contextmanager
    def get_connection(self):
        if self.connection_pool is None:
            self.create_pool()

        conn = None
        for attempt in range(MAX_RETRIES):
            try:
                conn = self.connection_pool.getconn()
                conn.autocommit = False
                yield conn
                break
            except (psycopg2.OperationalError, psycopg2.pool.PoolError) as e:
                if conn:
                    self.connection_pool.putconn(conn, close=True)

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Falha na conexão. Tentativa {attempt+1}/{MAX_RETRIES}. Aguardando {wait_time}s", exception=e)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha após {MAX_RETRIES} tentativas", exception=e)
                    raise

        if conn:
            try:
                self.connection_pool.putconn(conn)
            except Exception as e:
                logger.warning("Erro ao devolver conexão ao pool", exception=e)

    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error("Erro na operação. Rollback executado.", exception=e)
                raise
            finally:
                cursor.close()

    @contextmanager
    def connect_to_postgres(self):
        conn = None
        for attempt in range(MAX_RETRIES):
            try:
                conn = psycopg2.connect(
                    user=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT,
                    database="postgres"
                )
                conn.autocommit = True
                yield conn
                break
            except psycopg2.OperationalError as e:
                if conn:
                    conn.close()

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Falha na conexão. Tentativa {attempt+1}/{MAX_RETRIES}. Aguardando {wait_time}s", exception=e)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha após {MAX_RETRIES} tentativas", exception=e)
                    raise

        if conn:
            conn.close()

    def execute_query(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)

    def execute_and_fetch(self, query, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def bulk_insert(self, table, columns, values, batch_size=5000):
        total_inserted = 0

        with self.get_cursor() as cursor:
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES %s
                """
                execute_values(cursor, query, batch)
                total_inserted += len(batch)
                logger.progress(total_inserted, len(values), f"Inserindo em {table}")

        return total_inserted

    def copy_from_polars(self, df, table, columns=None):
        if columns is None:
            columns = df.columns

        csv_data = df.select(columns).write_csv(separator='\t', include_header=False)

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.copy_from(
                        file=csv_data.splitlines(),
                        table=table,
                        sep='\t',
                        columns=columns
                    )
                    conn.commit()
                    return len(df)
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Erro ao copiar para {table}", exception=e)
                    raise

    def drop_table(self, table, if_exists=True):
        exists_clause = "IF EXISTS" if if_exists else ""
        query = f"DROP TABLE {exists_clause} {table}"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            logger.info(f"Tabela {table} removida")

    def create_index(self, table, columns, index_name=None, unique=False, if_not_exists=True):
        if not index_name:
            index_name = f"idx_{table}_{'_'.join(columns)}"

        unique_clause = "UNIQUE" if unique else ""
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        query = f"""
        CREATE {unique_clause} INDEX {exists_clause} {index_name}
        ON {table} ({', '.join(columns)})
        """

        with self.get_cursor() as cursor:
            cursor.execute(query)
            logger.info(f"Índice {index_name} criado na tabela {table}")

    def database_exists(self):
        try:
            with self.connect_to_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Erro ao verificar existência do banco {DB_NAME}", exception=e)
            return False

    def create_database(self):
        if self.database_exists():
            logger.info(f"Banco {DB_NAME} já existe")
            return

        try:
            with self.connect_to_postgres() as conn:
                with conn.cursor() as cursor:
                    db_name = sql.Identifier(DB_NAME)
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {} WITH ENCODING 'UTF8'").format(db_name)
                    )
                    logger.info(f"Banco {DB_NAME} criado com sucesso")
        except Exception as e:
            logger.critical(f"Erro ao criar banco {DB_NAME}", exception=e)
            raise

    def table_exists(self, table):
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        )
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (table,))
            return cursor.fetchone()[0]

    def truncate_table(self, table):
        if not self.table_exists(table):
            logger.warning(f"Tabela {table} não existe para ser truncada")
            return

        query = f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            logger.info(f"Tabela {table} truncada")

    def get_table_row_count(self, table):
        if not self.table_exists(table):
            return 0

        query = f"SELECT COUNT(*) FROM {table}"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]

    def _get_schema_definitions(self):
        return {
            "empresa": """
                CREATE TABLE IF NOT EXISTS empresa (
                    cnpj_basico VARCHAR(8) PRIMARY KEY,
                    razao_social VARCHAR(255),
                    natureza_juridica INTEGER,
                    qualificacao_responsavel INTEGER,
                    capital_social NUMERIC(15, 2),
                    porte_empresa INTEGER,
                    ente_federativo_responsavel VARCHAR(255)
                )
            """,

            "estabelecimento": """
                CREATE TABLE IF NOT EXISTS estabelecimento (
                    cnpj_basico VARCHAR(8),
                    cnpj_ordem VARCHAR(4),
                    cnpj_dv VARCHAR(2),
                    identificador_matriz_filial INTEGER,
                    nome_fantasia VARCHAR(255),
                    situacao_cadastral INTEGER,
                    data_situacao_cadastral INTEGER,
                    motivo_situacao_cadastral INTEGER,
                    nome_cidade_exterior VARCHAR(255),
                    pais INTEGER,
                    data_inicio_atividade INTEGER,
                    cnae_fiscal_principal INTEGER,
                    cnae_fiscal_secundaria TEXT,
                    tipo_logradouro VARCHAR(255),
                    logradouro VARCHAR(255),
                    numero VARCHAR(255),
                    complemento VARCHAR(255),
                    bairro VARCHAR(255),
                    cep VARCHAR(8),
                    uf VARCHAR(2),
                    municipio INTEGER,
                    ddd_1 VARCHAR(4),
                    telefone_1 VARCHAR(9),
                    ddd_2 VARCHAR(4),
                    telefone_2 VARCHAR(9),
                    ddd_fax VARCHAR(4),
                    fax VARCHAR(9),
                    correio_eletronico VARCHAR(255),
                    situacao_especial VARCHAR(255),
                    data_situacao_especial INTEGER,
                    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
                )
            """,

            "socios": """
                CREATE TABLE IF NOT EXISTS socios (
                    cnpj_basico VARCHAR(8),
                    identificador_socio INTEGER,
                    nome_socio_razao_social VARCHAR(255),
                    cpf_cnpj_socio VARCHAR(14),
                    qualificacao_socio INTEGER,
                    data_entrada_sociedade INTEGER,
                    pais INTEGER,
                    representante_legal VARCHAR(14),
                    nome_do_representante VARCHAR(255),
                    qualificacao_representante_legal INTEGER,
                    faixa_etaria INTEGER,
                    PRIMARY KEY (cnpj_basico, cpf_cnpj_socio)
                )
            """,

            "simples": """
                CREATE TABLE IF NOT EXISTS simples (
                    cnpj_basico VARCHAR(8) PRIMARY KEY,
                    opcao_pelo_simples VARCHAR(1),
                    data_opcao_simples INTEGER,
                    data_exclusao_simples INTEGER,
                    opcao_mei VARCHAR(1),
                    data_opcao_mei INTEGER,
                    data_exclusao_mei INTEGER
                )
            """,

            "cnae": """
                CREATE TABLE IF NOT EXISTS cnae (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """,

            "moti": """
                CREATE TABLE IF NOT EXISTS moti (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """,

            "munic": """
                CREATE TABLE IF NOT EXISTS munic (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """,

            "natju": """
                CREATE TABLE IF NOT EXISTS natju (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """,

            "pais": """
                CREATE TABLE IF NOT EXISTS pais (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """,

            "quals": """
                CREATE TABLE IF NOT EXISTS quals (
                    codigo INTEGER PRIMARY KEY,
                    descricao VARCHAR(255)
                )
            """
        }

    def create_tables(self, tables=None):
        if tables is None:
            tables = list(self.schema_definitions.keys())

        with self.get_cursor() as cursor:
            for table in tables:
                if table not in self.schema_definitions:
                    logger.warning(f"Definição para tabela {table} não encontrada")
                    continue

                cursor.execute(self.schema_definitions[table])
                logger.info(f"Tabela {table} criada ou já existente")

    def create_standard_indexes(self):
        indexes = [
            ("empresa", ["cnpj_basico"]),
            ("estabelecimento", ["cnpj_basico"]),
            ("socios", ["cnpj_basico"]),
            ("simples", ["cnpj_basico"])
        ]

        for table, columns in indexes:
            self.create_index(table, columns)

    def recreate_table(self, table):
        if table not in self.schema_definitions:
            logger.warning(f"Definição para tabela {table} não encontrada")
            return

        self.drop_table(table)
        with self.get_cursor() as cursor:
            cursor.execute(self.schema_definitions[table])
            logger.info(f"Tabela {table} recriada")

    def recreate_all_tables(self):
        for table in self.schema_definitions.keys():
            self.recreate_table(table)

    def initialize_database(self, recreate=False):
        if not self.database_exists():
            self.create_database()
            self.create_pool()

        if recreate:
            self.recreate_all_tables()
        else:
            self.create_tables()

        self.create_standard_indexes()
        logger.info(f"Banco {DB_NAME} inicializado com sucesso")

    def cleanup_before_etl(self, recreate_tables=True):
        if not self.database_exists():
            self.create_database()
            self.create_pool()
            self.create_tables()
            self.create_standard_indexes()
            return

        self.create_pool()

        if recreate_tables:
            self.recreate_all_tables()
            self.create_standard_indexes()
        else:
            for table in self.schema_definitions.keys():
                if self.table_exists(table):
                    self.truncate_table(table)

        logger.info("Banco limpo e pronto para novo ETL")

db_manager = DatabaseManager()