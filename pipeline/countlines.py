import os
import mmap
import glob
import concurrent.futures
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from utils.logging import logger

load_dotenv()

EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))

class LineCounter:
    def __init__(self, input_paths=None):
        if input_paths is None:
            input_paths = [EXTRACTED_FILES_PATH, TEMP_PATH]
        self.input_paths = input_paths

    def count_file_lines(self, file_path: str) -> Tuple[str, int]:
        """Conta linhas de um arquivo usando mmap para máxima performance"""
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        if file_size == 0:
            return file_name, 0

        try:
            start_time = time.time()

            with open(file_path, 'rb') as f:
                # Usar mmap para contagem rápida
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                line_count = 0

                # Para arquivos muito grandes, contar em blocos
                if file_size > 1024 * 1024 * 100:  # 100 MB
                    chunk_size = 1024 * 1024 * 10  # 10 MB chunks
                    for i in range(0, file_size, chunk_size):
                        end = min(i + chunk_size, file_size)
                        mm.seek(i)
                        chunk = mm.read(end - i)
                        line_count += chunk.count(b'\n')

                        # Log de progresso para arquivos grandes
                        if (i // chunk_size) % 10 == 0:  # Log a cada 10 chunks
                            progress = (i / file_size) * 100
                            elapsed = time.time() - start_time
                            speed = i / (elapsed * 1024 * 1024) if elapsed > 0 else 0  # MB/s
                            logger.debug(f"Contagem {file_name}: {progress:.1f}% - {speed:.2f} MB/s")
                else:
                    # Para arquivos menores, contar tudo de uma vez
                    line_count = mm.read().count(b'\n')

                mm.close()

            # Ajustar para arquivos que não terminam com nova linha
            if file_size > 0 and line_count == 0:
                line_count = 1

            elapsed = time.time() - start_time
            speed = file_size / (elapsed * 1024 * 1024) if elapsed > 0 else 0  # MB/s
            logger.info(f"Contagem completa: {file_name} - {line_count:,} linhas - {speed:.2f} MB/s")

            return file_name, line_count

        except Exception as e:
            logger.error(f"Erro ao contar linhas de {file_path}", exception=e)
            return file_name, -1

    def find_all_files(self) -> List[str]:
        """Encontra todos os arquivos CSV nos diretórios de input"""
        all_files = []

        for path in self.input_paths:
            if os.path.exists(path):
                # Encontrar arquivos .csv e .CSV
                files = glob.glob(os.path.join(path, "*.csv"))
                files.extend(glob.glob(os.path.join(path, "*.CSV")))
                all_files.extend(files)

        return all_files

    def count_all_files(self) -> Dict[str, int]:
        """Conta linhas de todos os arquivos em paralelo"""
        all_files = self.find_all_files()

        if not all_files:
            logger.warning("Nenhum arquivo encontrado para contagem")
            return {}

        logger.info(f"Iniciando contagem de linhas em {len(all_files)} arquivos")
        results = {}

        # Usar ProcessPoolExecutor para contar arquivos em paralelo
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(self.count_file_lines, file_path): file_path
                for file_path in all_files
            }

            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_name, line_count = future.result()
                    if line_count >= 0:  # Ignorar arquivos com erro
                        results[file_name] = line_count
                except Exception as e:
                    logger.error(f"Erro ao processar {file_path}", exception=e)

        # Categorizar por tipo de arquivo
        categorized = defaultdict(int)
        for file_name, count in results.items():
            file_type = self._categorize_file(file_name)
            categorized[file_type] += count

        total_lines = sum(results.values())
        logger.info(f"Contagem total: {total_lines:,} linhas em {len(results)} arquivos")

        for file_type, count in sorted(categorized.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"Tipo {file_type}: {count:,} linhas ({(count/total_lines)*100:.1f}%)")

        return results

    def _categorize_file(self, file_name: str) -> str:
        """Categoriza arquivos por tipo baseado no nome"""
        file_name = file_name.lower()

        if 'empr' in file_name:
            return 'empresa'
        elif 'estab' in file_name:
            return 'estabelecimento'
        elif 'socio' in file_name:
            return 'socios'
        elif 'simples' in file_name:
            return 'simples'
        elif 'cnae' in file_name:
            return 'cnae'
        elif 'moti' in file_name:
            return 'motivos'
        elif 'munic' in file_name:
            return 'municipios'
        elif 'natju' in file_name:
            return 'naturezas'
        elif 'pais' in file_name:
            return 'paises'
        elif 'qual' in file_name:
            return 'qualificacoes'
        else:
            return 'outros'

def run_counter():
    try:
        timer_start = logger.start_timer("count_all_files")
        counter = LineCounter()
        results = counter.count_all_files()
        logger.end_timer("count_all_files", timer_start)

        return results
    except Exception as e:
        logger.critical("Falha crítica no processo de contagem de linhas", exception=e)
        return {}

if __name__ == "__main__":
    run_counter()