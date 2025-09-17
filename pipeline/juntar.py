import os
import re
import shutil
import concurrent.futures
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv

from utils.logging import logger

load_dotenv()

EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))
BUFFER_SIZE = 10 * 1024 * 1024  # 10MB buffer para cópia

class FileMerger:
    def __init__(self, input_path: str = None, output_path: str = None):
        self.input_path = input_path or EXTRACTED_FILES_PATH
        self.output_path = output_path or EXTRACTED_FILES_PATH
        self.temp_path = TEMP_PATH

        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        Path(self.temp_path).mkdir(parents=True, exist_ok=True)

    def find_multipart_files(self) -> Dict[str, List[str]]:
        # Padrão para arquivos multipartes, ex: ESTABELE1.PARTE00, ESTABELE1.PARTE01
        pattern = re.compile(r'(.+)\.PARTE(\d+)')

        files_by_prefix = defaultdict(list)

        for file in os.listdir(self.input_path):
            match = pattern.match(file)
            if match:
                prefix, part_num = match.groups()
                files_by_prefix[prefix].append((int(part_num), file))

        # Ordenar as partes por número para cada prefixo
        result = {}
        for prefix, parts in files_by_prefix.items():
            if len(parts) > 1:  # Considerar apenas arquivos com múltiplas partes
                sorted_parts = [p[1] for p in sorted(parts, key=lambda x: x[0])]
                result[prefix] = sorted_parts

        return result

    def merge_file_parts(self, prefix: str, parts: List[str]) -> bool:
        output_file = os.path.join(self.output_path, f"{prefix}.csv")
        temp_output = os.path.join(self.temp_path, f"{prefix}_temp.csv")

        # Verificar se o arquivo já existe e tem o tamanho correto
        if os.path.exists(output_file):
            expected_size = sum(os.path.getsize(os.path.join(self.input_path, part)) for part in parts)
            if os.path.getsize(output_file) == expected_size:
                logger.info(f"Arquivo {output_file} já existe com o tamanho correto")
                return True

        try:
            start_time = time.time()
            total_size = sum(os.path.getsize(os.path.join(self.input_path, part)) for part in parts)
            processed_size = 0

            logger.info(f"Iniciando junção de {len(parts)} partes para {prefix}")

            with open(temp_output, 'wb') as outfile:
                for i, part in enumerate(parts):
                    part_path = os.path.join(self.input_path, part)
                    part_size = os.path.getsize(part_path)

                    with open(part_path, 'rb') as infile:
                        # Copiar em blocos para eficiência
                        while True:
                            buffer = infile.read(BUFFER_SIZE)
                            if not buffer:
                                break
                            outfile.write(buffer)

                    processed_size += part_size
                    progress = (processed_size / total_size) * 100
                    elapsed = time.time() - start_time
                    speed = processed_size / (elapsed * 1024 * 1024) if elapsed > 0 else 0  # MB/s

                    logger.info(f"Junção {prefix}: {i+1}/{len(parts)} partes ({progress:.1f}%) - {speed:.2f} MB/s")

            # Mover o arquivo temporário para o destino final
            shutil.move(temp_output, output_file)

            # Remover os arquivos de partes após a junção bem-sucedida
            for part in parts:
                os.remove(os.path.join(self.input_path, part))

            logger.info(f"Junção completa: {prefix} ({len(parts)} partes, {total_size/1024**2:.2f} MB)")
            return True

        except Exception as e:
            logger.error(f"Erro ao juntar partes de {prefix}", exception=e)

            # Limpar arquivo temporário em caso de erro
            if os.path.exists(temp_output):
                os.remove(temp_output)

            return False

    def merge_all_multipart_files(self) -> Tuple[Set[str], Set[str]]:
        multipart_files = self.find_multipart_files()

        if not multipart_files:
            logger.info("Nenhum arquivo multipartes encontrado")
            return set(), set()

        logger.info(f"Encontrados {len(multipart_files)} arquivos multipartes para junção")

        successful = set()
        failed = set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_prefix = {
                executor.submit(self.merge_file_parts, prefix, parts): prefix
                for prefix, parts in multipart_files.items()
            }

            for future in concurrent.futures.as_completed(future_to_prefix):
                prefix = future_to_prefix[future]
                try:
                    if future.result():
                        successful.add(prefix)
                    else:
                        failed.add(prefix)
                except Exception as e:
                    logger.error(f"Erro no processamento de {prefix}", exception=e)
                    failed.add(prefix)

        if failed:
            logger.warning(f"Falha na junção de {len(failed)} arquivos: {', '.join(failed)}")

        logger.info(f"Junções concluídas: {len(successful)} sucesso, {len(failed)} falhas")
        return successful, failed

def run_merger():
    try:
        timer_start = logger.start_timer("merge_all_multipart_files")
        merger = FileMerger()
        successful, failed = merger.merge_all_multipart_files()
        logger.end_timer("merge_all_multipart_files", timer_start)

        return len(successful), len(failed)
    except Exception as e:
        logger.critical("Falha crítica no processo de junção de arquivos", exception=e)
        return 0, 0

if __name__ == "__main__":
    run_merger()