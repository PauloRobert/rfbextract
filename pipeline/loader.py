import os
import io
import re
import glob
import pandas as pd
import concurrent.futures
from typing import Dict, List, Tuple

from utils.logging import logger
from utils.db import db_manager

# Variáveis de ambiente (sem dependência de dotenv)
EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000000))
COMPRESSION_LEVEL = int(os.getenv('COMPRESSION_LEVEL', 9))  # mantido se usado por db_manager/schema


class DataLoader:
    def __init__(self, input_path: str = None):
        self.input_path = input_path or EXTRACTED_FILES_PATH

        if not self.input_path:
            logger.critical("EXTRACTED_FILES_PATH não está definido no ambiente.")
            raise ValueError("EXTRACTED_FILES_PATH não definido")

        if not os.path.isdir(self.input_path):
            logger.critical(f"Diretório EXTRACTED_FILES_PATH inválido ou inexistente: {self.input_path}")
            raise ValueError(f"Diretório inválido: {self.input_path}")

        # Padrões para mapeamento dos arquivos
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

        # Mapeamento de colunas (ordem exata usada no COPY)
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
        """
        Procura arquivos SOMENTE em EXTRACTED_FILES_PATH, conforme solicitado.
        """
        pattern = self.file_patterns.get(file_type)
        if not pattern:
            logger.warning(f"Padrão não definido para o tipo de arquivo: {file_type}")
            return []

        files = [f for f in glob.glob(os.path.join(self.input_path, '*'))
                 if re.search(pattern, os.path.basename(f), re.IGNORECASE)]
        logger.info(f"Encontrados {len(files)} arquivos do tipo {file_type} em {self.input_path}")
        return files

    def detect_encoding(self, file_path: str) -> str:
        """
        Estratégia simples e robusta:
        - Prioriza 'latin-1' (padrão dos arquivos da RFB).
        - Se falhar ao validar alguns bytes, usa 'utf-8' como fallback.
        """
        try:
            with open(file_path, 'rb') as f:
                _ = f.read(4096).decode('latin-1', errors='strict')
            logger.info(f"Assumindo encoding 'latin-1' para {os.path.basename(file_path)}")
            return 'latin-1'
        except Exception:
            logger.warning(f"Falha ao validar 'latin-1' em {os.path.basename(file_path)}; usando 'utf-8' como fallback.")
            return 'utf-8'

    def process_file(self, file_path: str, file_type: str) -> bool:
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"Processando arquivo: {file_name}")

            column_map = self.column_maps.get(file_type, {})
            expected_cols = list(column_map.values())
            total_rows = 0
            encoding = self.detect_encoding(file_path)

            # Leitura em chunks com dtype=str (mais permissivo, semelhante ao to_sql do script simples)
            for chunk in pd.read_csv(
                    file_path,
                    sep=';',
                    names=expected_cols if expected_cols else None,
                    dtype=str,
                    chunksize=CHUNK_SIZE,
                    encoding=encoding,
                    on_bad_lines='skip',
                    header=None,            # evita inferência de cabeçalho
                    keep_default_na=False,  # não converte "NA"/"N/A" em NaN
                    skip_blank_lines=True   # ignora linhas completamente vazias
            ):
                # Determinar a coluna-chave (primeira coluna do mapeamento)
                key_col = column_map.get(0) if 0 in column_map else (expected_cols[0] if expected_cols else None)

                # Garantir que todas as colunas esperadas existam no chunk e na ordem do COPY
                if expected_cols:
                    for col in expected_cols:
                        if col not in chunk.columns:
                            chunk[col] = ""
                    chunk = chunk[expected_cols]

                # Normalizar espaços
                for col in expected_cols:
                    chunk[col] = chunk[col].astype(str).str.strip()

                # Remover possíveis cabeçalhos residuais e linhas sem chave
                if key_col and key_col in chunk.columns:
                    header_like = chunk[key_col].str.lower().isin({key_col.lower(), 'codigo', 'cnpj_basico'})
                    if header_like.any():
                        chunk = chunk[~header_like]

                    before_clean = len(chunk)
                    chunk = chunk[chunk[key_col].notna() & (chunk[key_col] != "")]
                    dropped_key = before_clean - len(chunk)
                    if dropped_key:
                        logger.warning(f"Removidas {dropped_key} linhas sem chave em {file_name}")

                if chunk.empty:
                    logger.warning(f"Chunk vazio após limpeza em {file_name}. Continuando...")
                    continue

                # Ajuste de capital_social (como no script simples)
                if file_type == 'empresa' and 'capital_social' in chunk.columns:
                    s = (
                        chunk['capital_social']
                        .astype(str)
                        .str.strip()
                        .str.replace('.', '', regex=False)   # remove separador de milhar
                        .str.replace(',', '.', regex=False)  # converte decimal
                    )
                    chunk['capital_social'] = pd.to_numeric(s, errors='coerce').fillna(0.0)

                total_rows += len(chunk)

                # Inserção com COPY (rápido e consistente)
                with db_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        buffer = io.StringIO()
                        cols_for_copy = expected_cols if expected_cols else list(chunk.columns)
                        chunk[cols_for_copy].to_csv(buffer, sep='\t', index=False, header=False)
                        buffer.seek(0)
                        cursor.copy_from(buffer, file_type, sep='\t', columns=cols_for_copy)
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

        # Processamento paralelo por arquivo
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