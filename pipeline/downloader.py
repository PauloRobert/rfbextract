import os
import concurrent.futures
import time
from pathlib import Path
from typing import List, Tuple, Set

import requests
import bs4 as bs
from dotenv import load_dotenv

from utils.logging import logger

load_dotenv()

OUTPUT_FILES_PATH = os.getenv('OUTPUT_FILES_PATH')
RF_DATA_URL = os.getenv('RF_DATA_URL')
DOWNLOAD_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', 300))
CONNECT_TIMEOUT = int(os.getenv('CONNECT_TIMEOUT', 60))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', 10))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))

class Downloader:
    def __init__(self, output_path: str = None):
        self.output_path = output_path or OUTPUT_FILES_PATH
        self.url = RF_DATA_URL
        Path(self.output_path).mkdir(parents=True, exist_ok=True)

    def get_file_list(self) -> List[str]:
        try:
            response = requests.get(self.url, timeout=CONNECT_TIMEOUT)
            response.raise_for_status()

            soup = bs.BeautifulSoup(response.content, 'lxml')

            files = []
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and href.endswith('.zip'):
                    files.append(href)

            if not files:
                logger.warning(f"Nenhum arquivo .zip encontrado em {self.url}")

            return files

        except Exception as e:
            logger.error(f"Erro ao obter lista de arquivos de {self.url}", exception=e)
            raise

    def file_needs_download(self, url: str, file_path: str) -> bool:
        if not os.path.exists(file_path):
            return True

        try:
            response = requests.head(url, timeout=CONNECT_TIMEOUT)
            remote_size = int(response.headers.get('content-length', 0))
            local_size = os.path.getsize(file_path)

            if remote_size != local_size:
                logger.info(f"Arquivo {file_path} tem tamanho diferente do remoto. Será baixado novamente.")
                if os.path.exists(file_path):
                    os.remove(file_path)
                return True

            return False

        except Exception as e:
            logger.warning(f"Erro ao verificar tamanho do arquivo {url}", exception=e)
            return True

    def download_file(self, file_name: str) -> bool:
        url = f"{self.url}{file_name}"
        file_path = os.path.join(self.output_path, file_name)

        if not self.file_needs_download(url, file_path):
            logger.info(f"Arquivo {file_name} já existe e está atualizado")
            return True

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Baixando {file_name} - Tentativa {attempt+1}/{MAX_RETRIES}")

                with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get('content-length', 0))

                    with open(file_path, 'wb') as f:
                        downloaded = 0
                        start_time = time.time()
                        last_log_time = start_time

                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                                current_time = time.time()
                                if current_time - last_log_time >= 5:
                                    progress = (downloaded / total_size) * 100 if total_size > 0 else 0
                                    speed = downloaded / (current_time - start_time) / 1024
                                    logger.info(f"Download {file_name}: {progress:.1f}% - {speed:.1f} KB/s")
                                    last_log_time = current_time

                logger.info(f"Download completo: {file_name}")
                return True

            except requests.exceptions.RequestException as e:
                if os.path.exists(file_path):
                    os.remove(file_path)

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Erro no download de {file_name}. Aguardando {wait_time}s para nova tentativa.", exception=e)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha no download de {file_name} após {MAX_RETRIES} tentativas", exception=e)
                    return False

        return False

    def download_all_files(self) -> Tuple[Set[str], Set[str]]:
        files = self.get_file_list()
        logger.info(f"Encontrados {len(files)} arquivos para download")

        successful = set()
        failed = set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {executor.submit(self.download_file, file): file for file in files}

            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    if future.result():
                        successful.add(file)
                    else:
                        failed.add(file)
                except Exception as e:
                    logger.error(f"Erro no processamento de {file}", exception=e)
                    failed.add(file)

        if failed:
            logger.warning(f"Falha no download de {len(failed)} arquivos: {', '.join(failed)}")

        logger.info(f"Downloads concluídos: {len(successful)} sucesso, {len(failed)} falhas")
        return successful, failed

def run_downloader():
    try:
        timer_start = logger.start_timer("download_all_files")
        downloader = Downloader()
        successful, failed = downloader.download_all_files()
        logger.end_timer("download_all_files", timer_start)

        return len(successful), len(failed)
    except Exception as e:
        logger.critical("Falha crítica no processo de download", exception=e)
        return 0, 0

if __name__ == "__main__":
    run_downloader()