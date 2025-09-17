import os
import io
import re
import glob
import polars as pl
import concurrent.futures
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

from dotenv import load_dotenv

from utils.logging import logger
from utils.db import db_manager

load_dotenv()

EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000000))
COMPRESSION_LEVEL = int(os.getenv('COMPRESSION_LEVEL', 9))

class DataLoader:
    def __init__(self, input_path: str = None):
        self.input_path = input_path or EXTRACTED_FILES_PATH
        self.temp_path = TEMP_PATH
        self.file_patterns = {
            'empresa': r'(EMPR|empresa).*\.(csv|CSV)$',
            'estabelecimento': r'(ESTABELE|estabelecimento).*\.(csv|CSV)$',
            'socios': r'(SOCIO|socio).*\.(csv|CSV)$',
            'simples': r'(SIMPLES|simples).*\.(csv|CSV)$',
            'cnae': r'(CNAE|cnae).*\.(csv|CSV)$',
            'moti': r'(MOTI|motivo).*\.(csv|CSV)$',
            'munic': r'(MUNIC|municipio).*\.(csv|CSV)$',
            'natju': r'(NATJU|natureza).*\.(csv|CSV)$',
            'pais': r'(PAIS|pais).*\.(csv|CSV)$',
            'quals': r'(QUALS|qualificacoes).*\.(csv|CSV)$'
        }

        # Mapeamento de colunas para cada tipo de arquivo
        self.column_maps = {
            'empresa': {
                0: 'cnpj_basico',
                1: 'razao_social',
                2: 'natureza_juridica',
                3: 'qualificacao_responsavel',
                4: 'capital_social',
                5: 'porte_empresa',
                6: 'ente_federativo_responsavel'
            },
            'estabelecimento': {
                0: 'cnpj_basico', 1: 'cnpj_ordem', 2: 'cnpj_dv', 3: 'identificador_matriz_filial',
                4: 'nome_fantasia', 5: 'situacao_cadastral', 6: 'data_situacao_cadastral',
                7: 'motivo_situacao_cadastral', 8: 'nome_cidade_exterior', 9: 'pais',
                10: 'data_inicio_atividade', 11: 'cnae_fiscal_principal', 12: 'cnae_fiscal_secundaria',
                13: 'tipo_logradouro', 14: 'logradouro', 15: 'numero', 16: 'complemento',
                17: 'bairro', 18: 'cep', 19: 'uf', 20: 'municipio', 21: 'ddd_1',
                22: 'telefone_1', 23: 'ddd_2', 24: 'telefone_2', 25: 'ddd_fax',
                26: 'fax', 27: 'correio_eletronico', 28: 'situacao_especial', 29: 'data_situacao_especial'
            },
            'socios': {
                0: 'cnpj_basico', 1: 'identificador_socio', 2: 'nome_socio_razao_social',
                3: 'cpf_cnpj_socio', 4: 'qualificacao_socio', 5: 'data_entrada_sociedade',
                6: 'pais', 7: 'representante_legal', 8: 'nome_do_representante',
                9: 'qualificacao_representante_legal', 10: 'faixa_etaria'
            },
            'simples': {
                0: 'cnpj_basico', 1: 'opcao_pelo_simples', 2: 'data_opcao_simples',
                3: 'data_exclusao_simples', 4: 'opcao_mei', 5: 'data_opcao_mei',
                6: 'data_exclusao_mei'
            },
            'cnae': {0: 'codigo', 1: 'descricao'},
            'moti': {0: 'codigo', 1: 'descricao'},
            'munic': {0: 'codigo', 1: 'descricao'},
            'natju': {0: 'codigo', 1: 'descricao'},
            'pais': {0: 'codigo', 1: 'descricao'},
            'quals': {0: 'codigo', 1: 'descricao'}
        }

        # Schema para cada tipo de arquivo
        self.schemas = {
            'empresa': {
                'cnpj_basico': pl.latin-1,
                'razao_social': pl.latin-1,
                'natureza_juridica': pl.Int32,
                'qualificacao_responsavel': pl.Int32,
                'capital_social': pl.Float64,
                'porte_empresa': pl.Int32,
                'ente_federativo_responsavel': pl.latin-1
            },
            'estabelecimento': {
                'cnpj_basico': pl.latin-1, 'cnpj_ordem': pl.latin-1, 'cnpj_dv': pl.latin-1,
                'identificador_matriz_filial': pl.Int32, 'nome_fantasia': pl.latin-1,
                'situacao_cadastral': pl.Int32, 'data_situacao_cadastral': pl.Int32,
                'motivo_situacao_cadastral': pl.Int32, 'nome_cidade_exterior': pl.latin-1,
                'pais': pl.Int32, 'data_inicio_atividade': pl.Int32, 'cnae_fiscal_principal': pl.Int32,
                'cnae_fiscal_secundaria': pl.latin-1, 'tipo_logradouro': pl.latin-1, 'logradouro': pl.latin-1,
                'numero': pl.latin-1, 'complemento': pl.latin-1, 'bairro': pl.latin-1, 'cep': pl.latin-1,
                'uf': pl.latin-1, 'municipio': pl.Int32, 'ddd_1': pl.latin-1, 'telefone_1': pl.latin-1,
                'ddd_2': pl.latin-1, 'telefone_2': pl.latin-1, 'ddd_fax': pl.latin-1, 'fax': pl.latin-1,
                'correio_eletronico': pl.latin-1, 'situacao_especial': pl.latin-1, 'data_situacao_especial': pl.Int32
            },
            'socios': {
                'cnpj_basico': pl.latin-1, 'identificador_socio': pl.Int32, 'nome_socio_razao_social': pl.latin-1,
                'cpf_cnpj_socio': pl.latin-1, 'qualificacao_socio': pl.Int32, 'data_entrada_sociedade': pl.Int32,
                'pais': pl.Int32, 'representante_legal': pl.latin-1, 'nome_do_representante': pl.latin-1,
                'qualificacao_representante_legal': pl.Int32, 'faixa_etaria': pl.Int32
            },
            'simples': {
                'cnpj_basico': pl.latin-1, 'opcao_pelo_simples': pl.latin-1, 'data_opcao_simples': pl.Int32,
                'data_exclusao_simples': pl.Int32, 'opcao_mei': pl.latin-1, 'data_opcao_mei': pl.Int32,
                'data_exclusao_mei': pl.Int32
            },
            'cnae': {'codigo': pl.Int32, 'descricao': pl.latin-1},
            'moti': {'codigo': pl.Int32, 'descricao': pl.latin-1},
            'munic': {'codigo': pl.Int32, 'descricao': pl.latin-1},
            'natju': {'codigo': pl.Int32, 'descricao': pl.latin-1},
            'pais': {'codigo': pl.Int32, 'descricao': pl.latin-1},
            'quals': {'codigo': pl.Int32, 'descricao': pl.latin-1}
        }

    def find_files_by_type(self, file_type: str) -> List[str]:
        pattern = self.file_patterns.get(file_type)
        if not pattern:
            logger.warning(f"Padrão não definido para o tipo de arquivo: {file_type}")
            return []

        # Procurar nos diretórios de input e temp
        input_files = [f for f in glob.glob(os.path.join(self.input_path, '*'))
                       if re.search(pattern, os.path.basename(f))]

        temp_files = [f for f in glob.glob(os.path.join(self.temp_path, '*'))
                      if re.search(pattern, os.path.basename(f))]

        all_files = input_files + temp_files
        logger.info(f"Encontrados {len(all_files)} arquivos do tipo {file_type}")
        return all_files

    def process_file(self, file_path: str, file_type: str) -> bool:
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"Processando arquivo: {file_name}")

            column_map = self.column_maps.get(file_type, {})
            schema = self.schemas.get(file_type, {})

            start_time = time.time()

            # Ler o CSV sem cabeçalhos e definir os nomes das colunas usando column_map
            df_lazy = pl.scan_csv(
                file_path,
                separator=';',
                encoding='latin-1',
                has_header=False,  # Arquivos não têm cabeçalhos
                new_columns=list(column_map.values()),  # Usar os nomes das colunas do column_map
                dtypes=schema,
                low_memory=True
            )

            total_rows = 0
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    for i, chunk_df in enumerate(df_lazy.collect(streaming=True)):
                        if isinstance(chunk_df, pl.Series):
                            chunk_df = chunk_df.to_frame()

                        if file_type == 'empresa' and 'capital_social' in chunk_df.columns:
                            chunk_df = chunk_df.with_columns(
                                pl.col('capital_social').str.replace(',', '.').cast(pl.Float64)
                            )

                        chunk_rows = len(chunk_df)
                        total_rows += chunk_rows
                        logger.info(f"Processando chunk {i+1} de {file_name}: {chunk_rows:,} linhas")

                        csv_buffer = io.StringIO()
                        chunk_df.write_csv(csv_buffer, separator='\t', include_header=False)
                        csv_buffer.seek(0)

                        cursor.copy_from(
                            file=csv_buffer,
                            table=file_type,
                            sep='\t',
                            columns=chunk_df.columns
                        )

                        elapsed = time.time() - start_time
                        speed = total_rows / elapsed if elapsed > 0 else 0
                        logger.info(f"Progresso {file_name}: {total_rows:,} linhas - {speed:.0f} linhas/s")

                    conn.commit()

            elapsed = time.time() - start_time
            logger.info(f"Arquivo {file_name} processado: {total_rows:,} linhas em {elapsed:.2f}s ({total_rows/elapsed:.0f} linhas/s)")

            return True

        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}", exception=e)
            return False


    def load_file_type(self, file_type: str) -> Tuple[int, int]:
        files = self.find_files_by_type(file_type)
        if not files:
            logger.warning(f"Nenhum arquivo encontrado para o tipo: {file_type}")
            return 0, 0

        successful = 0
        failed = 0

        # Preparar o banco de dados
        db_manager.recreate_table(file_type)

        # Usar ProcessPoolExecutor para processar arquivos em paralelo
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(self.process_file, file_path, file_type): file_path
                for file_path in files
            }

            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Erro no processamento de {file_path}", exception=e)
                    failed += 1

        # Criar índices após carga
        if successful > 0:
            if file_type in ['empresa', 'estabelecimento', 'socios', 'simples']:
                db_manager.create_index(file_type, ['cnpj_basico'])

            if file_type == 'estabelecimento':
                # Índices adicionais para estabelecimento
                db_manager.create_index(file_type, ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'])

        logger.info(f"Carga de {file_type} concluída: {successful} sucessos, {failed} falhas")
        return successful, failed

    def load_all_data(self) -> Dict[str, Tuple[int, int]]:
        result = {}

        # Inicializar o banco de dados
        db_manager.initialize_database(recreate=True)

        # Ordem de processamento otimizada (menores primeiro para liberar espaço)
        processing_order = [
            'cnae', 'moti', 'munic', 'natju', 'pais', 'quals',  # Tabelas de referência (pequenas)
            'empresa', 'simples', 'socios',  # Tabelas médias
            'estabelecimento'  # Tabela grande
        ]

        for file_type in processing_order:
            start_time = logger.start_timer(f"load_{file_type}")
            result[file_type] = self.load_file_type(file_type)
            logger.end_timer(f"load_{file_type}", start_time)

        return result

def run_loader():
    try:
        timer_start = logger.start_timer("load_all_data")
        loader = DataLoader()
        results = loader.load_all_data()
        logger.end_timer("load_all_data", timer_start)

        total_success = sum(r[0] for r in results.values())
        total_fail = sum(r[1] for r in results.values())

        logger.info(f"Carga de dados concluída: {total_success} sucessos, {total_fail} falhas")
        return results
    except Exception as e:
        logger.critical("Falha crítica no processo de carga de dados", exception=e)
        return {}

if __name__ == "__main__":
    run_loader()