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
MAX_CHUNK_SIZE_MB = int(os.getenv('MAX_CHUNK_SIZE_MB', 100))  # Novo parâmetro
MAX_CHUNK_SIZE = MAX_CHUNK_SIZE_MB * 1024 * 1024  # Convertendo para bytes

class FileSplitter:
    def __init__(self, input_path: str = None, output_path: str = None):
        self.input_path = input_path or EXTRACTED_FILES_PATH
        self.output_path = output_path or TEMP_PATH
        Path(self.output_path).mkdir(parents=True, exist_ok=True)

    def get_file_header(self, file_path: str) -> str:
        """Retorna a primeira linha do arquivo como cabeçalho"""
        with open(file_path, 'r', encoding='utf8') as f:
            return f.readline().strip()

    def split_file_by_size(self, file_name: str) -> bool:
        """Divide o arquivo em chunks de no máximo MAX_CHUNK_SIZE bytes"""
        file_path = os.path.join(self.input_path, file_name)
        base_name = os.path.splitext(file_name)[0]

        if not os.path.exists(file_path):
            logger.error(f"Arquivo {file_path} não existe")
            return False

        if os.path.getsize(file_path) == 0:
            logger.warning(f"Arquivo {file_name} está vazio, ignorando")
            return False

        try:
            start_time = time.time()
            logger.info(f"Iniciando divisão de {file_name} por tamanho (max {MAX_CHUNK_SIZE_MB} MB)")

            header = self.get_file_header(file_path)

            chunk_num = 1
            current_chunk_size = 0
            chunk_file = os.path.join(self.output_path, f"{base_name}_chunk_{chunk_num:03d}.csv")
            outfile = open(chunk_file, 'w', encoding='utf8')
            outfile.write(f"{header}\n")
            lines_written = 0

            with open(file_path, 'r', encoding='utf8') as infile:
                next(infile)  # pular cabeçalho

                for line in infile:
                    outfile.write(line)
                    current_chunk_size += len(line.encode('utf8'))
                    lines_written += 1

                    if current_chunk_size >= MAX_CHUNK_SIZE:
                        outfile.close()
                        elapsed = time.time() - start_time
                        logger.info(
                            f"{file_name}: chunk {chunk_num} concluído "
                            f"({current_chunk_size / 1024 ** 2:.2f} MB, {lines_written} linhas) - "
                            f"{lines_written / elapsed:.0f} linhas/s"
                        )

                        # Próximo chunk
                        chunk_num += 1
                        chunk_file = os.path.join(self.output_path, f"{base_name}_chunk_{chunk_num:03d}.csv")
                        outfile = open(chunk_file, 'w', encoding='utf8')
                        outfile.write(f"{header}\n")
                        current_chunk_size = 0
                        lines_written = 0

            # Fechar último chunk se estiver aberto
            if not outfile.closed:
                outfile.close()
                elapsed = time.time() - start_time
                logger.info(
                    f"{file_name}: chunk {chunk_num} concluído "
                    f"({current_chunk_size / 1024 ** 2:.2f} MB, {lines_written} linhas)"
                )

            logger.info(f"Divisão completa de {file_name} em {chunk_num} chunks")
            return True

        except Exception as e:
            logger.error(f"Erro ao dividir arquivo {file_name}", exception=e)
            return False

    def split_all_files(self) -> Tuple[Set[str], Set[str]]:
        """Divide todos os arquivos do diretório de input, respeitando MAX_CHUNK_SIZE"""
        files_to_split = [
            f for f in os.listdir(self.input_path)
            if os.path.isfile(os.path.join(self.input_path, f)) and os.path.getsize(os.path.join(self.input_path, f)) > 0
        ]

        if not files_to_split:
            logger.info("Nenhum arquivo encontrado para divisão")
            return set(), set()

        successful = set()
        failed = set()

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {}
            for f in files_to_split:
                file_path = os.path.join(self.input_path, f)
                if os.path.getsize(file_path) <= MAX_CHUNK_SIZE:
                    logger.info(f"Arquivo {f} menor que {MAX_CHUNK_SIZE_MB} MB, passando para próxima etapa")
                    successful.add(f)
                    continue
                future_to_file[executor.submit(self.split_file_by_size, f)] = f

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