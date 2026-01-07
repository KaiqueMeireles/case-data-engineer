import logging
import os
from datetime import datetime

import pandas as pd

from src.utils import garantir_diretorio

logger = logging.getLogger(__name__)


def limpar_arquivo(caminho_arquivo: str) -> None:
    """Remove um arquivo individual se existir.
    
    Args:
        caminho_arquivo (str): Caminho completo do arquivo a remover.
    """
    if os.path.exists(caminho_arquivo):
        try:
            os.remove(caminho_arquivo)
            logger.info(f"Arquivo anterior removido: {caminho_arquivo}")
        except OSError as e:
            logger.warning(f"Não foi possível remover {caminho_arquivo}: {e}")


def preparar_csv_erros(
    df_erro: pd.DataFrame,
) -> pd.DataFrame:
    """Prepara DataFrame de erros com colunas relevantes e contexto.

    Seleciona apenas colunas importantes (cep, status, mensagem),
    adiciona timestamp do erro e categoriza o tipo de erro.

    Args:
        df_erro (pd.DataFrame): DataFrame com erros
            [cep, status, dados, mensagem].

    Returns:
        pd.DataFrame: DataFrame preparado com colunas
            [cep, mensagem, tipo_erro, data_erro].
    """
    if df_erro.empty:
        return pd.DataFrame()

    df_limpo = df_erro[['cep', 'mensagem']].copy()

    df_limpo['data_erro'] = datetime.now().isoformat()

    df_limpo['tipo_erro'] = df_limpo['mensagem'].apply(
        _categorizar_erro
    )

    df_limpo = df_limpo[['cep', 'mensagem', 'tipo_erro', 'data_erro']]

    return df_limpo


def _categorizar_erro(
    mensagem: str,
) -> str:
    """Categoriza o tipo de erro baseado na mensagem.

    Args:
        mensagem (str): Mensagem de erro do resultado.

    Returns:
        str: Categoria do erro (invalido, inexistente, conexao, outro).
    """
    if "inválido" in mensagem.lower():
        return "CEP_INVALIDO"
    elif "inexistente" in mensagem.lower():
        return "CEP_INEXISTENTE"
    elif "conexão" in mensagem.lower() or "http" in mensagem.lower():
        return "ERRO_CONEXAO"
    else:
        return "OUTRO"


def exportar_json(
    df: pd.DataFrame,
    output_folder: str = "data/output/",
) -> None:
    """Exporta DataFrame em formato JSON.

    Args:
        df (pd.DataFrame): DataFrame com dados normalizados.
        output_folder (str): Caminho da pasta de saída.
            Padrão: "data/output/".

    Raises:
        Exception: Se houver erro na exportação.
    """
    if df.empty:
        logger.info("DataFrame vazio. Nada será exportado para JSON.")
        return

    garantir_diretorio(output_folder)
    caminho_json = os.path.join(output_folder, "enderecos.json")
    
    logger.info("Exportando dados para JSON...")
    
    # Limpa arquivo JSON anterior
    limpar_arquivo(caminho_json)

    try:
        df.to_json(
            caminho_json,
            orient='records',
            indent=4,
            force_ascii=False
        )
        logger.info(f"JSON: {len(df)} registro(s) exportado(s).")
    except Exception as e:
        logger.error(f"Erro ao exportar JSON: {e}")
        raise


def exportar_xml(
    df: pd.DataFrame,
    output_folder: str = "data/output/",
) -> None:
    """Exporta DataFrame em formato XML.

    Args:
        df (pd.DataFrame): DataFrame com dados normalizados.
        output_folder (str): Caminho da pasta de saída.
            Padrão: "data/output/".

    Raises:
        Exception: Se houver erro na exportação ou biblioteca ausente.
    """
    if df.empty:
        logger.info("DataFrame vazio. Nada será exportado para XML.")
        return

    garantir_diretorio(output_folder)
    caminho_xml = os.path.join(output_folder, "enderecos.xml")
    
    logger.info("Exportando dados para XML...")
    
    limpar_arquivo(caminho_xml)

    try:
        df.to_xml(
            caminho_xml,
            index=False,
            root_name='enderecos',
            row_name='endereco',
            parser='lxml'
        )
        logger.info(f"XML: {len(df)} registro(s) exportado(s).")
    except ImportError:
        logger.error(
            "Erro: biblioteca 'lxml' não instalada. "
            "Execute: pip install lxml"
        )
        raise
    except Exception as e:
        logger.error(f"Erro ao exportar XML: {e}")
        raise
