import os
import concurrent.futures
import time
from pathlib import Path
from typing import Set, Tuple

from dotenv import load_dotenv
from utils.logging import logger

load_dotenv()

EXTRACTED_FILES_PATH = os.getenv('EXTRACTED_FILES_PATH')
TEMP_PATH = os.getenv('TEMP_PATH')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))
MAX_CHUNK_SIZE_MB = int(os.getenv('MAX_CHUNK_SIZE_MB', 100))
MAX_CHUNK_SIZE = MAX_CHUNK_SIZE_MB * 1024 * 1024  # bytes


def split_file_global(file_path: str, output_path: str, max_chunk_size: int) -> bool:
    """
    Função global para dividir um arquivo em chunks.
    Cada chunk terá no máximo max_chunk_size bytes.
    O arquivo original será deletado ao final.
    """
    try:
        import os
        import time
        from utils.logging import logger

        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]
        start_time = time.time()
        logger.info(f"Iniciando divisão de {file_name} (max {max_chunk_size / 1024 ** 2:.2f} MB)")

        with open(file_path, 'r', encoding='latin-1') as infile:
            header = infile.readline().strip()
            chunk_num = 1
            current_chunk_size = 0
            chunk_file = os.path.join(output_path, f"{base_name}_chunk_{chunk_num:03d}.csv")
            outfile = open(chunk_file, 'w', encoding='latin-1')
            outfile.write(header + "\n")
            lines_written = 0

            for line in infile:
                outfile.write(line)
                current_chunk_size += len(line.encode('latin-1'))
                lines_written += 1

                if current_chunk_size >= max_chunk_size:
                    outfile.close()
                    elapsed = time.time() - start_time
                    logger.info(
                        f"{file_name}: chunk {chunk_num} concluído "
                        f"({current_chunk_size / 1024 ** 2:.2f} MB, {lines_written} linhas) - "
                        f"{lines_written / elapsed:.0f} linhas/s"
                    )
                    chunk_num += 1
                    chunk_file = os.path.join(output_path, f"{base_name}_chunk_{chunk_num:03d}.csv")
                    outfile = open(chunk_file, 'w', encoding='latin-1')
                    outfile.write(header + "\n")
                    current_chunk_size = 0
                    lines_written = 0

            if not outfile.closed:
                outfile.close()
                elapsed = time.time() - start_time
                logger.info(
                    f"{file_name}: chunk {chunk_num} concluído "
                    f"({current_chunk_size / 1024 ** 2:.2f} MB, {lines_written} linhas)"
                )

        # Deletar arquivo original
        os.remove(file_path)
        logger.info(f"Arquivo original {file_name} removido após divisão")

        return True

    except Exception as e:
        logger.error(f"Erro ao dividir arquivo {file_path}", exception=e)
        return False


class FileSplitter:
    def __init__(self, input_path: str = None, output_path: str = None):
        self.input_path = Path(input_path or EXTRACTED_FILES_PATH).absolute()
        self.output_path = Path(output_path or TEMP_PATH).absolute()
        self.output_path.mkdir(parents=True, exist_ok=True)

    def split_all_files(self) -> Tuple[Set[str], Set[str]]:
        files_to_split = [
            f for f in self.input_path.iterdir()
            if f.is_file() and f.stat().st_size > 0
        ]

        if not files_to_split:
            logger.info("Nenhum arquivo encontrado para divisão")
            return set(), set()

        successful = set()
        failed = set()

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(split_file_global, str(f), str(self.output_path), MAX_CHUNK_SIZE): f.name
                for f in files_to_split if f.stat().st_size > MAX_CHUNK_SIZE
            }

            for f in files_to_split:
                if f.stat().st_size <= MAX_CHUNK_SIZE:
                    logger.info(f"Arquivo {f.name} menor que {MAX_CHUNK_SIZE_MB} MB, mantendo original")
                    successful.add(f.name)

            for future in concurrent.futures.as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    if future.result():
                        successful.add(file_name)
                    else:
                        failed.add(file_name)
                except Exception as e:
                    logger.error(f"Erro no processamento de {file_name}", exception=e)
                    failed.add(file_name)

        logger.info(f"Divisões concluídas: {len(successful)} sucesso, {len(failed)} falhas")
        return successful, failed


def run_splitter():
    try:
        timer_start = logger.start_timer("split_all_files")
        splitter = FileSplitter()
        successful, failed = splitter.split_all_files()
        logger.end_timer("split_all_files", timer_start)
        return len(successful), len(failed)
    except Exception as e:
        logger.critical("Falha crítica no processo de divisão de arquivos", exception=e)
        return 0, 0


if __name__ == "__main__":
    run_splitter()