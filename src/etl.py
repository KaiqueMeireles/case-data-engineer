from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from src.data_transformation import (
    normalizar_resultados,
    validar_dados_transformados,
)
from src.database import criar_banco, inserir_dados
from src.export_data import exportar_json, exportar_xml, preparar_csv_erros
from src.get_cep_info import consultar_cep
from src.get_cep_list import carregar_lista_cep
from src.utils import garantir_diretorio

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
    # Define workers baseado no modo: 500 local, 4 produção
    workers = 500 if is_local else 4

    caminho_arquivo = str(Path(caminho_arquivo))
    # 1. SETUP E INFRAESTRUTURA
    garantir_diretorio("data/output/")

    # O padrão é True para garantir que o banco seja recriado a cada execução
    # isso foi feito com o objetivo de garantir que todas as funcionalidades
    # do módulo de banco de dados sejam testadas (criação e inserção).
    criar_banco(reset=True)

    # 2. EXTRAÇÃO
    print(
        f"[ETL] Modo {'LOCAL' if is_local else 'API'}. "
        f"Carregando lista com {workers} worker(s)..."
    )
    df_lista = carregar_lista_cep(
        caminho_arquivo=caminho_arquivo,
        tamanho_amostra=tamanho_amostra,
    )
    ceps = df_lista['cep'].tolist()

    print(f"[ETL] Iniciando consultas para {len(ceps)} CEPs...")

    # Seleciona função de consulta (API real ou mock)
    if is_local:
        from tests.test_get_cep_info import consultar_cep_mock
        consulta_fn = consultar_cep_mock
    else:
        consulta_fn = consultar_cep

    # Execução Paralela (I/O Bound)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        resultados_brutos = list(executor.map(consulta_fn, ceps))
    
    df_bruto = pd.DataFrame(resultados_brutos)

    # 3. TRANSFORMAÇÃO
    print("[ETL] Filtrando e transformando dados...")
    df_sucesso = df_bruto[df_bruto['status'] == 'sucesso'].copy()
    df_erro = df_bruto[df_bruto['status'] == 'erro'].copy()

    # Fluxo de Sucesso
    if not df_sucesso.empty:
        df_final = normalizar_resultados(df_sucesso)
        df_final = validar_dados_transformados(df_final)

        # 4. CARGA (Load)
        print("[ETL] Salvando dados no banco e exportando arquivos...")
        inserir_dados(df_final)
        exportar_json(df_final)
        exportar_xml(df_final)

    # Fluxo de Erro (Logs)
    if not df_erro.empty:
        print(f"[ETL] Gerando logs para {len(df_erro)} erro(s)...")
        df_erros_formatados = preparar_csv_erros(df_erro)
        df_erros_formatados.to_csv("data/output/cep_erro.csv", index=False)

    print("[ETL] Pipeline finalizado com sucesso!")