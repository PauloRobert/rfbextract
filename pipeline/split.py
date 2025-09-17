import os
import mmap
import concurrent.futures
import time
from pathlib import Path
from typing import List, Dict, Set, Tuple

from dotenv import load_dotenv

from utils.logging import logger

load_dotenv()

EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000000))  # Linhas por chunk

class FileSplitter:
    def __init__(self, input_path: str = None, output_path: str = None):
        self.input_path = input_path or EXTRACTED_FILES_PATH
        self.output_path = output_path or TEMP_PATH
        Path(self.output_path).mkdir(parents=True, exist_ok=True)

    def count_lines(self, file_path: str) -> int:
        with open(file_path, 'rb') as f:
            # Usar mmap para contagem eficiente de linhas em arquivos grandes
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            line_count = 0

            # Contar bytes de nova linha
            line_count = mm.read().count(b'\n')
            mm.close()

            # Ajustar para o caso de o arquivo não terminar com nova linha
            if os.path.getsize(file_path) > 0 and line_count == 0:
                line_count = 1

            return line_count

    def find_large_files(self, min_lines: int = CHUNK_SIZE) -> List[str]:
        large_files = []

        for file in os.listdir(self.input_path):
            if file.endswith('.csv'):
                file_path = os.path.join(self.input_path, file)
                try:
                    line_count = self.count_lines(file_path)
                    if line_count > min_lines:
                        large_files.append((file, line_count))
                        logger.info(f"Arquivo grande encontrado: {file} ({line_count:,} linhas)")
                except Exception as e:
                    logger.error(f"Erro ao verificar arquivo {file}", exception=e)

        return large_files

    def get_file_header(self, file_path: str) -> str:
        with open(file_path, 'r', encoding='latin-1') as f:
            return f.readline().strip()

    def split_file(self, file_info: Tuple[str, int]) -> bool:
        file_name, total_lines = file_info
        file_path = os.path.join(self.input_path, file_name)
        base_name = os.path.splitext(file_name)[0]

        # Verificar se o arquivo existe
        if not os.path.exists(file_path):
            logger.error(f"Arquivo {file_path} não existe")
            return False

        try:
            start_time = time.time()
            logger.info(f"Iniciando divisão de {file_name} ({total_lines:,} linhas)")

            # Obter o cabeçalho do arquivo
            header = self.get_file_header(file_path)

            # Calcular número de chunks
            num_chunks = (total_lines + CHUNK_SIZE - 1) // CHUNK_SIZE

            # Abrir o arquivo de entrada
            with open(file_path, 'r', encoding='latin-1') as infile:
                # Pular o cabeçalho no arquivo de entrada (já foi lido)
                next(infile)

                for chunk_num in range(num_chunks):
                    chunk_file = os.path.join(self.output_path, f"{base_name}_chunk_{chunk_num+1:03d}.csv")

                    with open(chunk_file, 'w', encoding='latin-1') as outfile:
                        # Escrever o cabeçalho em cada chunk
                        outfile.write(f"{header}\n")

                        # Escrever as linhas do chunk
                        lines_written = 0
                        for _ in range(CHUNK_SIZE):
                            line = infile.readline()
                            if not line:
                                break

                            outfile.write(line)
                            lines_written += 1

                    # Calcular progresso
                    progress = min(100, ((chunk_num + 1) * CHUNK_SIZE / total_lines) * 100)
                    elapsed = time.time() - start_time
                    speed = ((chunk_num + 1) * CHUNK_SIZE) / elapsed if elapsed > 0 else 0

                    logger.info(f"Divisão {file_name}: chunk {chunk_num+1}/{num_chunks} ({progress:.1f}%) - {speed:.0f} linhas/s")

            logger.info(f"Divisão completa: {file_name} em {num_chunks} chunks")
            return True

        except Exception as e:
            logger.error(f"Erro ao dividir arquivo {file_name}", exception=e)
            return False

    def split_all_large_files(self) -> Tuple[Set[str], Set[str]]:
        large_files = self.find_large_files()

        if not large_files:
            logger.info("Nenhum arquivo grande encontrado para divisão")
            return set(), set()

        logger.info(f"Encontrados {len(large_files)} arquivos grandes para divisão")

        successful = set()
        failed = set()

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(self.split_file, file_info): file_info[0]
                for file_info in large_files
            }

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
            logger.warning(f"Falha na divisão de {len(failed)} arquivos: {', '.join(failed)}")

        logger.info(f"Divisões concluídas: {len(successful)} sucesso, {len(failed)} falhas")
        return successful, failed

def run_splitter():
    try:
        timer_start = logger.start_timer("split_all_large_files")
        splitter = FileSplitter()
        successful, failed = splitter.split_all_large_files()
        logger.end_timer("split_all_large_files", timer_start)

        return len(successful), len(failed)
    except Exception as e:
        logger.critical("Falha crítica no processo de divisão de arquivos", exception=e)
        return 0, 0

if __name__ == "__main__":
    run_splitter()