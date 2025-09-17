import os
import shutil
import zipfile
import concurrent.futures
import time
from pathlib import Path
from typing import List, Tuple, Set

from dotenv import load_dotenv

from utils.logging import logger

load_dotenv()

OUTPUT_FILES_PATH = os.getenv('OUTPUT_FILES_PATH')
EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', 10))
# Controle opcional para remover o arquivo ZIP original após extração (padrão: True)
DELETE_ARCHIVE_AFTER_EXTRACT = os.getenv('DELETE_ARCHIVE_AFTER_EXTRACT', 'true').lower() in ('1', 'true', 'yes')


class Extractor:
    """Classe responsável por extrair arquivos ZIP encontrados em OUTPUT_FILES_PATH

    Alterações importantes nesta versão:
    - Remove o arquivo .zip original logo após a extração bem-sucedida (controle via DELETE_ARCHIVE_AFTER_EXTRACT)
    - Mantém compatibilidade com a API existente (mesmos métodos públicos)
    """

    def __init__(self, input_path: str = None, output_path: str = None, delete_archive: bool = None):
        self.input_path = input_path or OUTPUT_FILES_PATH
        self.output_path = output_path or EXTRACTED_FILES_PATH
        # Se delete_archive for fornecido, prevalece; caso contrário usa a env var
        self.delete_archive = DELETE_ARCHIVE_AFTER_EXTRACT if delete_archive is None else bool(delete_archive)

        Path(self.output_path).mkdir(parents=True, exist_ok=True)

    def get_zip_files(self) -> List[str]:
        zip_files = []
        if not self.input_path or not os.path.isdir(self.input_path):
            logger.warning(f"Input path inválido ou inexistente: {self.input_path}")
            return zip_files

        for item in os.listdir(self.input_path):
            if item.lower().endswith('.zip'):
                zip_files.append(item)

        if not zip_files:
            logger.warning(f"Nenhum arquivo .zip encontrado em {self.input_path}")

        return zip_files

    def _safe_remove(self, file_path: str) -> bool:
        """Tenta remover o arquivo e loga adequadamente. Retorna True se removido ou inexistente."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Arquivo removido após extração: {file_path}")
            else:
                logger.debug(f"Arquivo já inexistente ao tentar remover: {file_path}")
            return True
        except Exception as e:
            logger.warning(f"Falha ao remover arquivo {file_path}: {e}",)
            return False

    def extract_file(self, zip_file: str) -> bool:
        file_path = os.path.join(self.input_path, zip_file)

        # Verificar se o arquivo existe
        if not os.path.isfile(file_path):
            logger.error(f"Arquivo {file_path} não existe")
            return False

        # Verificar se o arquivo é realmente um ZIP
        if not zipfile.is_zipfile(file_path):
            logger.error(f"Arquivo {file_path} não é um arquivo ZIP válido")
            return False

        # Verificar espaço em disco disponível (estimativa com base no tamanho dos membros)
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                total_size = sum(info.file_size for info in zip_ref.infolist())

                # Verificar espaço disponível usando shutil (compatível com Windows, Mac e Linux)
                try:
                    free_space = shutil.disk_usage(self.output_path).free
                    if total_size > free_space:
                        logger.error(
                            f"Espaço insuficiente para extrair {zip_file}. Necessário: {total_size/1024**2:.2f} MB, Disponível: {free_space/1024**2:.2f} MB"
                        )
                        return False
                except Exception as e:
                    # Se não conseguir verificar o espaço, apenas registra aviso e continua
                    logger.warning(f"Não foi possível verificar espaço em disco: {str(e)}")
        except Exception as e:
            logger.error(f"Erro ao verificar tamanho do arquivo ZIP {zip_file}",)
            return False

        # Extrair o arquivo com retentativas
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Extraindo {zip_file} - Tentativa {attempt+1}/{MAX_RETRIES}")

                start_time = time.time()

                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Obter a lista de arquivos para monitorar o progresso
                    file_list = zip_ref.infolist()
                    total_files = len(file_list)
                    total_size = sum(info.file_size for info in file_list)

                    # Extrair arquivos individualmente para monitorar o progresso
                    extracted_size = 0
                    for i, file_info in enumerate(file_list, 1):
                        # Extrair o arquivo
                        zip_ref.extract(file_info, self.output_path)

                        # Atualizar o progresso
                        extracted_size += file_info.file_size
                        if i % 10 == 0 or i == total_files:  # Log a cada 10 arquivos
                            progress = (extracted_size / total_size) * 100 if total_size > 0 else 0
                            elapsed = time.time() - start_time
                            speed = extracted_size / (elapsed * 1024**2) if elapsed > 0 else 0  # MB/s
                            logger.info(f"Extração {zip_file}: {i}/{total_files} arquivos ({progress:.1f}%) - {speed:.2f} MB/s")

                logger.info(f"Extração completa: {zip_file}")

                # Remover o arquivo ZIP original para economizar espaço, se configurado
                if self.delete_archive:
                    removed = self._safe_remove(file_path)
                    if not removed:
                        logger.warning(f"Não foi possível remover o arquivo de origem {file_path} após extração")
                else:
                    logger.debug(f"Remoção do arquivo ZIP desabilitada por configuração: {file_path}")

                return True

            except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Erro na extração de {zip_file}. Aguardando {wait_time}s para nova tentativa.",)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha na extração de {zip_file} após {MAX_RETRIES} tentativas",)
                    return False

        return False

    def extract_all_files(self) -> Tuple[Set[str], Set[str]]:
        zip_files = self.get_zip_files()
        logger.info(f"Encontrados {len(zip_files)} arquivos ZIP para extração")

        successful = set()
        failed = set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {executor.submit(self.extract_file, file): file for file in zip_files}

            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    if future.result():
                        successful.add(file)
                    else:
                        failed.add(file)
                except Exception as e:
                    logger.error(f"Erro no processamento de {file}",)
                    failed.add(file)

        if failed:
            logger.warning(f"Falha na extração de {len(failed)} arquivos: {', '.join(failed)}")

        logger.info(f"Extrações concluídas: {len(successful)} sucesso, {len(failed)} falhas")
        return successful, failed


def run_extractor():
    try:
        timer_start = logger.start_timer("extract_all_files")
        extractor = Extractor()
        successful, failed = extractor.extract_all_files()
        logger.end_timer("extract_all_files", timer_start)

        # Verificar se todos os arquivos falharam e se havia arquivos para extrair
        if len(successful) == 0 and len(failed) > 0:
            logger.error("Todas as extrações falharam")
            return 0, len(failed)

        return len(successful), len(failed)
    except Exception as e:
        logger.critical("Falha crítica no processo de extração",)
        return 0, 0


if __name__ == "__main__":
    run_extractor()
