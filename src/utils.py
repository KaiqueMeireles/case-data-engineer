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

    Define o nível de logging como INFO com formato padrão incluindo
    timestamp, nome do logger, nível e mensagem.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
    )