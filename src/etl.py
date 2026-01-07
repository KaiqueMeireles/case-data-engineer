import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from src.data_transformation import (
    normalizar_resultados,
    validar_dados_transformados,
)
from src.database import criar_banco, inserir_dados
from src.export_data import (
    exportar_json,
    exportar_xml,
    limpar_arquivos_saida,
    preparar_csv_erros,
)
from src.get_cep_info import consultar_cep
from src.get_cep_list import carregar_lista_cep
from src.utils import garantir_diretorio

logger = logging.getLogger(__name__)


def _validar_entrada(
    tamanho_amostra: int,
    caminho_arquivo: str,
) -> None:
    """Valida os parâmetros de entrada do pipeline ETL.

    Verifica se tamanho_amostra é válido e se o arquivo de entrada existe.

    Args:
        tamanho_amostra (int): Quantidade de CEPs a processar.
        caminho_arquivo (str): Caminho do arquivo com lista de CEPs.

    Raises:
        ValueError: Se tamanho_amostra <= 0 ou > 1000000.
        FileNotFoundError: Se arquivo não existe.
    """
    if tamanho_amostra <= 0:
        raise ValueError(
            f"tamanho_amostra deve ser > 0, recebido: {tamanho_amostra}"
        )

    if tamanho_amostra > 1000000:
        raise ValueError(
            f"tamanho_amostra muito grande (máx: 1000000), "
            f"recebido: {tamanho_amostra}"
        )

    caminho_normalizado = str(Path(caminho_arquivo))
    if not os.path.exists(caminho_normalizado):
        raise FileNotFoundError(
            f"Arquivo não encontrado: {caminho_normalizado}"
        )


def executar_pipeline(
    tamanho_amostra: int,
    caminho_arquivo: str = 'data/input/cep.tsv.zip',
    is_local: bool = False,
) -> None:
    """Orquestra o fluxo completo de ETL (Extração, Transformação, Carga).

    Executa o pipeline de processamento de CEPs: carrega lista, consulta
    API (ou mock), transforma dados, persiste em banco e exporta em
    múltiplos formatos (JSON, XML, CSV).

    Args:
        tamanho_amostra (int): Quantidade de CEPs a processar.
        caminho_arquivo (str): Caminho do arquivo com lista de CEPs.
            Padrão: 'data/input/cep.tsv.zip'.
        is_local (bool): Se True, usa mock ao invés de API real.
            Define workers automaticamente: 500 em modo local, 4 em produção.
            Padrão: False (usa API ViaCEP com 4 workers).
    """
    _validar_entrada(tamanho_amostra, caminho_arquivo)

    logger.info("=" * 50)
    logger.info("Iniciando ETL...")
    logger.info("=" * 50)

    workers = 500 if is_local else 3

    # Limpa/cria a pasta de saída e o banco de dados.
    # O padrão é apagar tudo para garantir que o banco
    # e os arquivos sejam recriados a cada execução.
    # Isso foi feito com o objetivo de garantir que todas 
    # as funcionalidades do módulo de banco de dados e 
    # de export de arquivos sejam testadas (criação e inserção).
    garantir_diretorio("data/output/")
    limpar_arquivos_saida("data/output/")
    criar_banco(reset=True)

    modo = 'LOCAL' if is_local else 'API'
    logger.info(
        f"Modo {modo}. Carregando lista com {workers} worker(s)..."
    )
    df_lista = carregar_lista_cep(
        caminho_arquivo=caminho_arquivo,
        tamanho_amostra=tamanho_amostra,
    )
    ceps = df_lista['cep'].tolist()

    logger.info(f"Iniciando consultas para {len(ceps)} CEPs...")

    # Seleciona função de consulta (API real ou mock).
    if is_local:
        from tests.test_get_cep_info import consultar_cep_mock
        consulta_fn = consultar_cep_mock
    else:
        consulta_fn = consultar_cep

    # Execução paralela das consultas
    logger.info(f"[1/5] Consultando {len(ceps)} CEPs na API...")
    
    resultados_brutos = []
    with ThreadPoolExecutor(max_workers=workers) as executor:

        list_tqdm = tqdm(
            executor.map(consulta_fn, ceps),
            total=len(ceps),
            unit="cep",
            desc="Progresso",
            ncols=100  # Largura fixa para não quebrar a linha no terminal
        )
        resultados_brutos = list(list_tqdm)
    
    df_bruto = pd.DataFrame(resultados_brutos)
    logger.info(f"[2/5] {len(df_bruto)} CEPs consultados com sucesso")

    logger.info("[3/5] Filtrando e transformando dados...")
    df_sucesso = df_bruto[df_bruto['status'] == 'sucesso'].copy()
    df_erro = df_bruto[df_bruto['status'] == 'erro'].copy()
    logger.info(f"[3/5] Sucesso: {len(df_sucesso)} | Erros: {len(df_erro)}")

    if not df_sucesso.empty:
        logger.info("[4/5] Normalizando e validando dados...")
        df_final = normalizar_resultados(df_sucesso)
        df_final = validar_dados_transformados(df_final)

        logger.info("[5/5] Salvando em banco de dados e exportando arquivos...")
        inserir_dados(df_final)
        exportar_json(df_final)
        exportar_xml(df_final)

    if not df_erro.empty:
        logger.info(f"Gerando CSV de erros com {len(df_erro)} registro(s)...")
        caminho_csv = "data/output/enderecos_erros.csv"
        df_erros_formatados = preparar_csv_erros(df_erro)
        df_erros_formatados.to_csv(caminho_csv, index=False)

    logger.info("Pipeline finalizado com sucesso!")