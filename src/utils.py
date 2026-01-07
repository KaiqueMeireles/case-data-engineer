import logging
import os


def garantir_diretorio(caminho: str) -> None:
    """Garante que o diretório especificado exista.

    Verifica a existência do caminho informado e, caso não exista,
    cria a estrutura de pastas necessária.

    Args:
        caminho (str): Caminho da pasta a ser verificada/criada.
    """
    logger = logging.getLogger(__name__)
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        logger.info(f"Diretório preparado: {caminho}")


def _limpar_log_anterior(caminho_log: str) -> None:
    """Remove o arquivo de log anterior para iniciar uma nova sessão limpa.

    Verifica se o arquivo de log existe e o remove, evitando logs
    acumulados de execuções anteriores.

    Args:
        caminho_log (str): Caminho completo do arquivo de log a limpar.
    """
    logger = logging.getLogger(__name__)
    if os.path.exists(caminho_log):
        try:
            os.remove(caminho_log)
            logger.info(f"Log anterior removido: {caminho_log}")
        except OSError as e:
            logger.warning(f"Não foi possível remover log anterior: {e}")


def configurar_logging() -> None:
    """Centraliza a configuração de log do sistema.

    Configura dois handlers:
    - Console: exibe apenas WARNING e ERROR (para evitar poluição visual)
    - Arquivo: registra todos os eventos com detalhes em data/output/pipeline_diagnosis.log
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Captura tudo internamente

    # Remove handlers anteriores (se houver)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    caminho_log = "data/output/pipeline_diagnosis.log"
    _limpar_log_anterior(caminho_log)

    # Formato detalhado para arquivo
    formato_arquivo = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Formato conciso para console
    formato_console = logging.Formatter(
        '%(levelname)s: %(message)s'
    )

    # Handler para geração de ARQUIVO (todos os níveis de logs)
    garantir_diretorio("data/output/")
    arquivo_handler = logging.FileHandler(caminho_log)
    arquivo_handler.setLevel(logging.DEBUG)
    arquivo_handler.setFormatter(formato_arquivo)
    logger.addHandler(arquivo_handler)

    # Handler para o CONSOLE (apenas logs de nível WARNING+)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formato_console)
    logger.addHandler(console_handler)

    # Silencia logs verbose das bibliotecas externas
    # urllib3 gera muitas mensagens de retry que poluem o console
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    logging.getLogger('requests').setLevel(logging.WARNING)