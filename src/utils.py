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
    arquivo_handler = logging.FileHandler("data/output/pipeline_diagnosis.log")
    arquivo_handler.setLevel(logging.DEBUG)
    arquivo_handler.setFormatter(formato_arquivo)
    logger.addHandler(arquivo_handler)

    # Handler para o CONSOLE (apenas logs de nível WARNING+)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formato_console)
    logger.addHandler(console_handler)