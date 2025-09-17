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

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "Dados_RFB")

# Configurações de retry
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 10))

class DatabaseManager:
    """
    Gerenciador de conexões e operações no banco de dados PostgreSQL.
    Implementa um pool de conexões, retentativas em caso de falha e operações otimizadas.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Inicializa o pool de conexões."""
        self.connection_pool = None
        self.schema_definitions = self._get_schema_definitions()

    def create_pool(self, min_conn: int = 1, max_conn: int = 10, database: Optional[str] = None):
        """
        Cria um pool de conexões com o banco de dados.

        Args:
            min_conn: Número mínimo de conexões no pool
            max_conn: Número máximo de conexões no pool
            database: Nome do banco (se None, usa o padrão do .env)
        """
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
            logger.info("Conexão com o banco de dados estabelecida",
                        host=DB_HOST,
                        db=db_name,
                        pool_size=max_conn)
        except Exception as e:
            logger.critical("Falha ao criar pool de conexões com o banco de dados",
                            exception=e,
                            host=DB_HOST,
                            db=database or DB_NAME)
            raise

    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Obtém uma conexão do pool com suporte a retentativas.

        Yields:
            Uma conexão com o banco de dados
        """
        if self.connection_pool is None:
            self.create_pool()

        conn = None
        for attempt in range(MAX_RETRIES):
            try:
                conn = self.connection_pool.getconn()
                conn.autocommit = False  # Transações explícitas
                yield conn
                break
            except (psycopg2.OperationalError, psycopg2.pool.PoolError) as e:
                if conn:
                    self.connection_pool.putconn(conn, close=True)

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Falha na conexão com o banco. Tentativa {attempt+1}/{MAX_RETRIES}. "
                                   f"Aguardando {wait_time}s para nova tentativa.",
                                   exception=e)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha ao obter conexão após {MAX_RETRIES} tentativas",
                                 exception=e)
                    raise

        if conn:
            try:
                self.connection_pool.putconn(conn)
            except Exception as e:
                logger.warning("Erro ao devolver conexão ao pool", exception=e)

    @contextmanager
    def get_cursor(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        """
        Obtém um cursor para executar operações no banco de dados.

        Yields:
            Um cursor para executar operações no banco
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error("Erro durante operação no banco de dados. Realizando rollback.",
                             exception=e)
                raise
            finally:
                cursor.close()

    @contextmanager
    def connect_to_postgres(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Conecta diretamente ao servidor PostgreSQL sem especificar um banco de dados.
        Usado para criar o banco de dados se não existir.

        Yields:
            Uma conexão com o servidor PostgreSQL
        """
        conn = None
        for attempt in range(MAX_RETRIES):
            try:
                # Conectar ao 'postgres' padrão para poder criar nosso banco
                conn = psycopg2.connect(
                    user=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT,
                    database="postgres"
                )
                conn.autocommit = True  # Precisamos de autocommit para CREATE DATABASE
                yield conn
                break
            except psycopg2.OperationalError as e:
                if conn:
                    conn.close()

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Falha na conexão com o servidor PostgreSQL. Tentativa {attempt+1}/{MAX_RETRIES}. "
                                   f"Aguardando {wait_time}s para nova tentativa.",
                                   exception=e)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha ao conectar ao servidor PostgreSQL após {MAX_RETRIES} tentativas",
                                 exception=e)
                    raise

        if conn:
            conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Executa uma query SQL sem retorno.

        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)

    def execute_and_fetch(self, query: str, params: Optional[tuple] = None) -> List[Tuple]:
        """
        Executa uma query SQL e retorna todos os resultados.

        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query

        Returns:
            Lista de tuplas com os resultados
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def bulk_insert(self,
                    table: str,
                    columns: List[str],
                    values: List[Tuple],
                    batch_size: int = 5000) -> int:
        """
        Insere múltiplos registros no banco de dados de forma otimizada.

        Args:
            table: Nome da tabela
            columns: Lista de colunas
            values: Lista de tuplas com os valores
            batch_size: Tamanho do lote para inserção

        Returns:
            Número de registros inseridos
        """
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

                # Log do progresso
                logger.progress(total_inserted, len(values), f"Inserindo em {table}")

        return total_inserted

    def copy_from_polars(self,
                         df: pl.DataFrame,
                         table: str,
                         columns: Optional[List[str]] = None) -> int:
        """
        Insere dados de um DataFrame Polars usando COPY para máxima performance.

        Args:
            df: DataFrame Polars com os dados
            table: Nome da tabela
            columns: Lista de colunas (opcional, usa todas as colunas do DataFrame se None)

        Returns:
            Número de registros inseridos
        """
        if columns is None:
            columns = df.columns

        # Converter para CSV em memória
        csv_data = df.select(columns).write_csv(separator='\t', include_header=False)

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # Usar o COPY para carregar os dados diretamente
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
                    logger.error(f"Erro ao copiar dados para a tabela {table}", exception=e)
                    raise

    def drop_table(self, table: str, if_exists: bool = True) -> None:
        """
        Remove uma tabela do banco de dados.

        Args:
            table: Nome da tabela
            if_exists: Adiciona IF EXISTS à query
        """
        exists_clause = "IF EXISTS" if if_exists else ""
        query = f"DROP TABLE {exists_clause} {table}"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            logger.info(f"Tabela {table} removida")

    def create_index(self,
                     table: str,
                     columns: List[str],
                     index_name: Optional[str] = None,
                     unique: bool = False,
                     if_not_exists: bool = True) -> None:
        """
        Cria um índice na tabela.

        Args:
            table: Nome da tabela
            columns: Lista de colunas para o índice
            index_name: Nome do índice (opcional)
            unique: Se o índice deve ser único
            if_not_exists: Adiciona IF NOT EXISTS à query
        """
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

    def database_exists(self) -> bool:
        """
        Verifica se o banco de dados configurado existe.

        Returns:
            True se o banco existir, False caso contrário
        """
        try:
            with self.connect_to_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Erro ao verificar existência do banco de dados {DB_NAME}", exception=e)
            return False

    def create_database(self) -> None:
        """
        Cria o banco de dados se não existir.
        """
        if self.database_exists():
            logger.info(f"Banco de dados {DB_NAME} já existe")
            return

        try:
            with self.connect_to_postgres() as conn:
                with conn.cursor() as cursor:
                    # Escapar o nome do banco para evitar SQL injection
                    db_name = sql.Identifier(DB_NAME)
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {} WITH ENCODING 'UTF8'").format(db_name)
                    )
                    logger.info(f"Banco de dados {DB_NAME} criado com sucesso")
        except Exception as e:
            logger.critical(f"Erro ao criar banco de dados {DB_NAME}", exception=e)
            raise

    def table_exists(self, table: str) -> bool:
        """
        Verifica se uma tabela existe no banco de dados.

        Args:
            table: Nome da tabela

        Returns:
            True se a tabela existir, False caso contrário
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        )
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (table,))
            return cursor.fetchone()[0]

    def truncate_table(self, table: str) -> None:
        """
        Limpa todos os dados de uma tabela.

        Args:
            table: Nome da tabela
        """
        if not self.table_exists(table):
            logger.warning(f"Tabela {table} não existe para ser truncada")
            return

        query = f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            logger.info(f"Tabela {table} truncada")

    def get_table_row_count(self, table: str) -> int:
        """
        Obtém o número de linhas de uma tabela.

        Args:
            table: Nome da tabela

        Returns:
            Número de linhas na tabela
        """
        if not self.table_exists(table):
            return 0

        query = f"SELECT COUNT(*) FROM {table}"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]

    def _get_schema_definitions(self) -> Dict[str, str]:
        """
        Retorna as definições de schema para todas as tabelas do CNPJ.

        Returns:
            Dicionário com nome da tabela e comando SQL para criá-la
        """
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

    def create_tables(self, tables: Optional[List[str]] = None) -> None:
        """
        Cria as tabelas especificadas (ou todas se não especificadas).

        Args:
            tables: Lista de nomes de tabelas para criar (opcional)
        """
        if tables is None:
            tables = list(self.schema_definitions.keys())

        with self.get_cursor() as cursor:
            for table in tables:
                if table not in self.schema_definitions:
                    logger.warning(f"Definição para tabela {table} não encontrada")
                    continue

                cursor.execute(self.schema_definitions[table])
                logger.info(f"Tabela {table} criada ou já existente")

    def create_standard_indexes(self) -> None:
        """
        Cria os índices padrão para otimizar as consultas.
        """
        indexes = [
            ("empresa", ["cnpj_basico"]),
            ("estabelecimento", ["cnpj_basico"]),
            ("socios", ["cnpj_basico"]),
            ("simples", ["cnpj_basico"])
        ]

        for table, columns in indexes:
            self.create_index(table, columns)

    def recreate_table(self, table: str) -> None:
        """
        Recria uma tabela do zero (drop + create).

        Args:
            table: Nome da tabela
        """
        if table not in self.schema_definitions:
            logger.warning(f"Definição para tabela {table} não encontrada")
            return

        self.drop_table(table)
        with self.get_cursor() as cursor:
            cursor.execute(self.schema_definitions[table])
            logger.info(f"Tabela {table} recriada")

    def recreate_all_tables(self) -> None:
        """
        Recria todas as tabelas do banco de dados.
        """
        for table in self.schema_definitions.keys():
            self.recreate_table(table)

    def initialize_database(self, recreate: bool = False) -> None:
        """
        Inicializa o banco de dados completo (criar BD, tabelas e índices).

        Args:
            recreate: Se True, recria todas as tabelas mesmo se já existirem
        """
        # Verificar e criar o banco se necessário
        if not self.database_exists():
            self.create_database()

            # Conectar ao novo banco para as próximas operações
            self.create_pool()

        # Criar ou recriar as tabelas
        if recreate:
            self.recreate_all_tables()
        else:
            self.create_tables()

        # Criar índices padrão
        self.create_standard_indexes()

        logger.info(f"Banco de dados {DB_NAME} inicializado com sucesso")

    def cleanup_before_etl(self, recreate_tables: bool = True) -> None:
        """
        Prepara o banco para um novo processo de ETL.

        Args:
            recreate_tables: Se True, recria todas as tabelas; se False, apenas trunca
        """
        # Verificar se o banco existe e criar se necessário
        if not self.database_exists():
            self.create_database()
            self.create_pool()
            self.create_tables()
            self.create_standard_indexes()
            return

        # Conectar ao banco existente
        self.create_pool()

        # Limpar ou recriar tabelas
        if recreate_tables:
            self.recreate_all_tables()
            self.create_standard_indexes()
        else:
            # Truncar todas as tabelas
            for table in self.schema_definitions.keys():
                if self.table_exists(table):
                    self.truncate_table(table)

        logger.info("Banco de dados limpo e pronto para novo processo de ETL")

# Instância global do gerenciador de banco de dados
db_manager = DatabaseManager()