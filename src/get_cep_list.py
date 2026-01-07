import pandas as pd
import os
import warnings

# A escolha do 25 foi por causa da soma do ano de fundação do banco (1969, 1 + 9 + 6 + 9 = 25).
def carregar_lista_cep(
    caminho_arquivo: str = 'data/input/cep.tsv.zip',
    tamanho_amostra: int = 10000,
    semente: int = 25
) -> pd.DataFrame:
    """Carrega e retorna uma amostra aleatória de CEPs em um DataFrame.

    Args:
        caminho_arquivo (str): Caminho para o arquivo com a lista de CEPs.
            Padrão: 'data/input/cep.tsv.zip'
        tamanho_amostra (int): Número de CEPs a serem retornados na amostra.
            Padrão: 10000
        semente (int): Semente para calcular amostra aleatória reproduzível.
            Padrão: 25

    Returns:
        pd.DataFrame: DataFrame com coluna 'cep' contendo os CEPs da amostra.

    Raises:
        FileNotFoundError: Se o arquivo não existir no caminho especificado.
        RuntimeError: Se houver erro ao carregar o arquivo.
    """
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    try:
        # O arquivo é um TSV (table separated values), por isso o separador é '\t'.
        # Puxei apenas a coluna 'cep' para economizar memória.
        # Mantive o tipo str para evitar problemas com zeros à esquerda.
        cep_df = pd.read_csv(caminho_arquivo, sep='\t', usecols=['cep'], dtype=str)
        
        if len(cep_df) < tamanho_amostra:
            warn_message = (
                f"Amostra solicitada ({tamanho_amostra}) "
                f"maior que dados disponíveis ({len(cep_df)}). "
                "Retornando todos os dados."
            )
            warnings.warn(warn_message, UserWarning)
            return cep_df.reset_index(drop=True)
        
        # Limitando os resultados à amostra desejada (10.000 casos).
        # O random_state garante que a amostra seja a mesma em execuções diferentes.
        return cep_df.sample(
            n=tamanho_amostra,
            random_state=semente
        ).reset_index(drop=True)
    
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar CEPs: {e}")