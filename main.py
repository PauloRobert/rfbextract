import os
import sys
import time
import argparse
from typing import Dict, List, Tuple, Optional, Any

from dotenv import load_dotenv

from utils.logging import logger
from utils.db import db_manager
from pipeline.downloader import run_downloader
from pipeline.extract import run_extractor
from pipeline.juntar import run_merger
from pipeline.split import run_splitter
from pipeline.countlines import run_counter
from pipeline.loader import run_loader

load_dotenv()

class ETLPipeline:
    def __init__(self, skip_steps: List[str] = None, force_recreate_db: bool = False):
        self.skip_steps = skip_steps or []
        self.force_recreate_db = force_recreate_db
        self.results = {}
        self.start_time = time.time()

    def should_skip(self, step: str) -> bool:
        return step in self.skip_steps

    def prepare_database(self) -> bool:
        try:
            if self.force_recreate_db:
                logger.info("Recriando banco de dados")
                db_manager.initialize_database(recreate=True)
            else:
                logger.info("Preparando banco de dados")
                db_manager.cleanup_before_etl(recreate_tables=True)
            return True
        except Exception as e:
            logger.critical("Falha ao preparar banco de dados", exception=e)
            return False

    def download_files(self) -> bool:
        if self.should_skip('download'):
            logger.info("Etapa de download ignorada")
            return True

        try:
            logger.info("Iniciando download de arquivos")
            success, failed = run_downloader()
            self.results['download'] = {'success': success, 'failed': failed}

            if failed > 0 or success == 0:  # Falha se houver erros ou nenhum arquivo baixado
                logger.error(f"Download falhou: {success} arquivos baixados, {failed} falhas")
                return False
            else:
                logger.info("Download concluído com sucesso")
                return True
        except Exception as e:
            logger.critical("Falha crítica na etapa de download", exception=e)
            self.results['download'] = {'success': 0, 'failed': 1, 'error': str(e)}
            return False

    def extract_files(self) -> bool:
        if self.should_skip('extract'):
            logger.info("Etapa de extração ignorada")
            return True

        try:
            logger.info("Iniciando extração de arquivos")
            success, failed = run_extractor()
            self.results['extract'] = {'success': success, 'failed': failed}

            if failed > 0 and success == 0:
                logger.error(f"Extração falhou completamente: {failed} falhas")
                return False
            elif failed > 0:
                logger.warning(f"Extração concluída com {failed} falhas")
            else:
                logger.info("Extração concluída com sucesso")

            return success > 0  # Sucesso se pelo menos um arquivo foi extraído
        except Exception as e:
            logger.critical("Falha crítica na etapa de extração", exception=e)
            self.results['extract'] = {'success': 0, 'failed': 1, 'error': str(e)}
            return False

    def merge_files(self) -> bool:
        if self.should_skip('merge'):
            logger.info("Etapa de junção ignorada")
            return True

        try:
            logger.info("Iniciando junção de arquivos multipartes")
            success, failed = run_merger()
            self.results['merge'] = {'success': success, 'failed': failed}

            logger.info("Junção concluída")
            return True  # Continuar mesmo sem arquivos para juntar
        except Exception as e:
            logger.critical("Falha crítica na etapa de junção", exception=e)
            self.results['merge'] = {'success': 0, 'failed': 1, 'error': str(e)}
            return False

    def split_large_files(self) -> bool:
        if self.should_skip('split'):
            logger.info("Etapa de divisão ignorada")
            return True

        try:
            logger.info("Iniciando divisão de arquivos grandes")
            success, failed = run_splitter()
            self.results['split'] = {'success': success, 'failed': failed}

            logger.info("Divisão concluída")
            return True  # Continuar mesmo sem arquivos para dividir
        except Exception as e:
            logger.critical("Falha crítica na etapa de divisão", exception=e)
            self.results['split'] = {'success': 0, 'failed': 1, 'error': str(e)}
            return False

    def count_lines(self) -> bool:
        if self.should_skip('count'):
            logger.info("Etapa de contagem ignorada")
            return True

        try:
            logger.info("Iniciando contagem de linhas")
            results = run_counter()

            total_lines = sum(results.values())
            total_files = len(results)

            self.results['count'] = {'total_lines': total_lines, 'total_files': total_files}

            logger.info(f"Contagem concluída: {total_lines:,} linhas em {total_files} arquivos")
            return True
        except Exception as e:
            logger.critical("Falha crítica na etapa de contagem", exception=e)
            self.results['count'] = {'total_lines': 0, 'total_files': 0, 'error': str(e)}
            return False

    def load_data(self) -> bool:
        if self.should_skip('load'):
            logger.info("Etapa de carregamento ignorada")
            return True

        try:
            logger.info("Iniciando carregamento de dados")
            results = run_loader()

            total_success = sum(r[0] for r in results.values())
            total_fail = sum(r[1] for r in results.values())

            self.results['load'] = {
                'success': total_success,
                'failed': total_fail,
                'details': results
            }

            if total_fail > 0:
                logger.warning(f"Carregamento concluído com {total_fail} falhas")
            else:
                logger.info("Carregamento concluído com sucesso")

            return total_fail == 0  # Sucesso se não houver falhas
        except Exception as e:
            logger.critical("Falha crítica na etapa de carregamento", exception=e)
            self.results['load'] = {'success': 0, 'failed': 1, 'error': str(e)}
            return False

    def create_indexes(self) -> bool:
        if self.should_skip('index'):
            logger.info("Etapa de criação de índices ignorada")
            return True

        try:
            logger.info("Criando índices")
            db_manager.create_standard_indexes()

            self.results['index'] = {'success': True}
            logger.info("Índices criados com sucesso")
            return True
        except Exception as e:
            logger.critical("Falha crítica na criação de índices", exception=e)
            self.results['index'] = {'success': False, 'error': str(e)}
            return False

    def run_pipeline(self) -> Dict[str, Any]:
        logger.info("Iniciando pipeline ETL de dados da Receita Federal")

        # Preparar banco de dados
        if not self.prepare_database():
            return self.generate_report(success=False)

        # Download é uma etapa crítica - se falhar, interrompe todo o pipeline
        if not self.download_files():
            logger.critical("Interrompendo pipeline devido à falha crítica na etapa de download")
            return self.generate_report(success=False)

        # As demais etapas só executam se o download for bem-sucedido
        steps = [
            self.extract_files,
            self.merge_files,
            self.split_large_files,
            self.count_lines,
            self.load_data,
            self.create_indexes
        ]

        pipeline_success = True
        for step in steps:
            step_success = step()
            if not step_success:
                pipeline_success = False
                if step.__name__ == 'load_data':
                    # Falha na carga também é crítica e interrompe o pipeline
                    logger.critical(f"Interrompendo pipeline devido a falha crítica em {step.__name__}")
                    break

        return self.generate_report(success=pipeline_success)

    def generate_report(self, success: bool) -> Dict[str, Any]:
        total_time = time.time() - self.start_time

        report = {
            'success': success,
            'total_time': total_time,
            'total_time_formatted': self.format_time(total_time),
            'steps': self.results
        }

        # Exibir relatório
        logger.info(f"Pipeline concluído em {self.format_time(total_time)}")
        logger.info(f"Status: {'Sucesso' if success else 'Falha'}")

        for step, result in self.results.items():
            if 'error' in result:
                logger.error(f"Etapa {step} falhou: {result['error']}")
            elif step == 'count':
                logger.info(f"Etapa {step}: {result.get('total_lines', 0):,} linhas em {result.get('total_files', 0)} arquivos")
            elif step in ['download', 'extract', 'load']:
                logger.info(f"Etapa {step}: {result.get('success', 0)} sucessos, {result.get('failed', 0)} falhas")
            else:
                logger.info(f"Etapa {step}: {result}")

        return report

    @staticmethod
    def format_time(seconds: float) -> str:
        """Formata tempo em segundos para formato legível"""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

def parse_args():
    parser = argparse.ArgumentParser(description='ETL de dados públicos da Receita Federal')

    parser.add_argument('--skip', type=str, nargs='+', choices=[
        'download', 'extract', 'merge', 'split', 'count', 'load', 'index'
    ], help='Etapas a serem puladas')

    parser.add_argument('--recreate-db', action='store_true',
                        help='Recriar o banco de dados do zero')

    parser.add_argument('--only', type=str, choices=[
        'download', 'extract', 'merge', 'split', 'count', 'load', 'index'
    ], help='Executar apenas a etapa especificada')

    return parser.parse_args()

def main():
    args = parse_args()

    # Se --only for especificado, pular todas as outras etapas
    skip_steps = []
    if args.only:
        all_steps = ['download', 'extract', 'merge', 'split', 'count', 'load', 'index']
        skip_steps = [step for step in all_steps if step != args.only]
    elif args.skip:
        skip_steps = args.skip

    try:
        pipeline = ETLPipeline(
            skip_steps=skip_steps,
            force_recreate_db=args.recreate_db
        )

        report = pipeline.run_pipeline()

        if not report['success']:
            logger.error("Pipeline concluído com falhas")
            sys.exit(1)
        else:
            logger.info("Pipeline concluído com sucesso")
            sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        logger.critical("Erro fatal no pipeline", exception=e)
        sys.exit(1)

if __name__ == "__main__":
    main()