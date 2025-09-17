import os
import io
import re
import glob
import pandas as pd
import concurrent.futures
import time
from pathlib import Path
from typing import Dict, List, Tuple
from dotenv import load_dotenv
import chardet

from utils.logging import logger
from utils.db import db_manager

# Carregar variáveis de ambiente
load_dotenv()

# Definir variáveis globais
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
            'empresa': r'EMPRE',
            'estabelecimento': r'ESTABELE',
            'socios': r'SOCIO',
            'simples': r'SIMPLES',
            'cnae': r'CNAE',
            'moti': r'MOTI',
            'munic': r'MUNIC',
            'natju': r'NATJU',
            'pais': r'PAIS',
            'quals': r'QUALS'
        }

        self.column_maps = {
            'empresa': {0: 'cnpj_basico', 1: 'razao_social', 2: 'natureza_juridica',
                        3: 'qualificacao_responsavel', 4: 'capital_social', 5: 'porte_empresa',
                        6: 'ente_federativo_responsavel'},
            'estabelecimento': {0: 'cnpj_basico', 1: 'cnpj_ordem', 2: 'cnpj_dv',
                                3: 'identificador_matriz_filial', 4: 'nome_fantasia',
                                5: 'situacao_cadastral', 6: 'data_situacao_cadastral',
                                7: 'motivo_situacao_cadastral', 8: 'nome_cidade_exterior',
                                9: 'pais', 10: 'data_inicio_atividade', 11: 'cnae_fiscal_principal',
                                12: 'cnae_fiscal_secundaria', 13: 'tipo_logradouro', 14: 'logradouro',
                                15: 'numero', 16: 'complemento', 17: 'bairro', 18: 'cep',
                                19: 'uf', 20: 'municipio', 21: 'ddd_1', 22: 'telefone_1',
                                23: 'ddd_2', 24: 'telefone_2', 25: 'ddd_fax', 26: 'fax',
                                27: 'correio_eletronico', 28: 'situacao_especial',
                                29: 'data_situacao_especial'},
            'socios': {0: 'cnpj_basico', 1: 'identificador_socio', 2: 'nome_socio_razao_social',
                       3: 'cpf_cnpj_socio', 4: 'qualificacao_socio', 5: 'data_entrada_sociedade',
                       6: 'pais', 7: 'representante_legal', 8: 'nome_do_representante',
                       9: 'qualificacao_representante_legal', 10: 'faixa_etaria'},
            'simples': {0: 'cnpj_basico', 1: 'opcao_pelo_simples', 2: 'data_opcao_simples',
                        3: 'data_exclusao_simples', 4: 'opcao_mei', 5: 'data_opcao_mei',
                        6: 'data_exclusao_mei'},
            'cnae': {0: 'codigo', 1: 'descricao'},
            'moti': {0: 'codigo', 1: 'descricao'},
            'munic': {0: 'codigo', 1: 'descricao'},
            'natju': {0: 'codigo', 1: 'descricao'},
            'pais': {0: 'codigo', 1: 'descricao'},
            'quals': {0: 'codigo', 1: 'descricao'}
        }

    def find_files_by_type(self, file_type: str) -> List[str]:
        pattern = self.file_patterns.get(file_type)
        if not pattern:
            logger.warning(f"Padrão não definido para o tipo de arquivo: {file_type}")
            return []

        input_files = [f for f in glob.glob(os.path.join(self.input_path, '*'))
                       if re.search(pattern, os.path.basename(f), re.IGNORECASE)]
        temp_files = [f for f in glob.glob(os.path.join(self.temp_path, '*'))
                      if re.search(pattern, os.path.basename(f), re.IGNORECASE)]
        all_files = input_files + temp_files
        logger.info(f"Encontrados {len(all_files)} arquivos do tipo {file_type}")
        return all_files

    def detect_encoding(self, file_path: str, n_lines: int = 5000) -> str:
        with open(file_path, 'rb') as f:
            raw = b''.join([f.readline() for _ in range(n_lines)])
        result = chardet.detect(raw)
        encoding = result['encoding'] if result['encoding'] else 'utf-8'
        logger.info(f"Encoding detectado para {os.path.basename(file_path)}: {encoding}")
        return encoding

    def process_file(self, file_path: str, file_type: str) -> bool:
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"Processando arquivo: {file_name}")

            column_map = self.column_maps.get(file_type, {})
            total_rows = 0
            encoding = self.detect_encoding(file_path)

            for chunk in pd.read_csv(
                    file_path,
                    sep=';',
                    names=list(column_map.values()),
                    dtype=str,
                    chunksize=CHUNK_SIZE,
                    encoding=encoding,
                    on_bad_lines='skip'
            ):
                # Verificar e remover linhas onde a primeira coluna ('codigo') é nula
                initial_count = len(chunk)

                # A chave aqui é o `dropna`
                # Ele remove as linhas onde a coluna 'codigo' é nula (NaN)
                chunk.dropna(subset=['codigo'], inplace=True)

                rows_dropped = initial_count - len(chunk)
                if rows_dropped > 0:
                    logger.warning(
                        f"Removidas {rows_dropped} linhas com valores nulos na coluna 'codigo' "
                        f"do arquivo {file_name}."
                    )

                if chunk.empty:
                    logger.warning(f"O chunk ficou vazio após a limpeza. Continuando...")
                    continue

                # Corrigir capital_social para empresa
                if file_type == 'empresa' and 'capital_social' in chunk.columns:
                    chunk['capital_social'] = chunk['capital_social'].str.replace(',', '.').astype(float)

                total_rows += len(chunk)

                # Inserir no banco
                with db_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        buffer = io.StringIO()
                        chunk.to_csv(buffer, sep='\t', index=False, header=False)
                        buffer.seek(0)
                        cursor.copy_from(buffer, file_type, sep='\t', columns=chunk.columns)
                    conn.commit()

            logger.info(f"Arquivo {file_name} processado: {total_rows:,} linhas")
            return True

        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}", exception=e)
            return False

    def load_file_type(self, file_type: str) -> Tuple[int, int]:
        files = self.find_files_by_type(file_type)
        if not files:
            logger.warning(f"Nenhum arquivo encontrado para o tipo: {file_type}")
            return 0, 0

        successful, failed = 0, 0
        db_manager.recreate_table(file_type)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {executor.submit(self.process_file, f, file_type): f for f in files}
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

        # Índices
        if successful > 0:
            if file_type in ['empresa', 'estabelecimento', 'socios', 'simples']:
                db_manager.create_index(file_type, ['cnpj_basico'])
            if file_type == 'estabelecimento':
                db_manager.create_index(file_type, ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'])

        logger.info(f"Carga de {file_type} concluída: {successful} sucessos, {failed} falhas")
        return successful, failed

    def load_all_data(self) -> Dict[str, Tuple[int, int]]:
        result = {}
        db_manager.initialize_database(recreate=True)
        processing_order = [
            'cnae', 'moti', 'munic', 'natju', 'pais', 'quals',
            'empresa', 'simples', 'socios', 'estabelecimento'
        ]
        for file_type in processing_order:
            start_time = logger.start_timer(f"load_{file_type}")
            try:
                result[file_type] = self.load_file_type(file_type)
            except Exception as e:
                logger.error(f"Falha ao carregar dados do tipo {file_type}", exception=e)
                result[file_type] = (0, 0)
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

        # Só gerar warning se houver falhas reais
        if total_fail > 0 and total_success == 0:
            logger.warning("Carregamento concluído, mas sem arquivos processados")
        elif total_fail > 0:
            logger.warning(f"Carregamento concluído com {total_fail} falhas")
        else:
            logger.info("Carregamento concluído sem falhas")

        return results

    except Exception as e:
        logger.critical("Falha crítica no processo de carga de dados", exception=e)
        return {}


if __name__ == "__main__":
    run_loader()