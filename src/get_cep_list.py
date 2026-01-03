import pandas as pd
import os
import warnings

# Pega uma amostra aleatória de 10.000 casos a partir do DataFrame completo
# O parâmetro random_state = 25 garante que a amostra aleatória seja sempre a mesma
# A escolha do 25 foi por causa da soma do ano de fundação do banco (1969, 1 + 9 + 6 + 9 = 25)
def carregar_lista_ceps(caminho_arquivo='data/input/cep.tsv.zip', tamanho_amostra=10000, semente=25):
    """
    Carrega a lista de CEPs do arquivo e retorna uma amostra aleatória.
    """
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
    
    try:
        cep_df = pd.read_csv(caminho_arquivo, sep='\t', usecols=['cep'], dtype=str)
        
        if len(cep_df) < tamanho_amostra:
            warnings.warn(f"Amostra solicitada ({tamanho_amostra}) maior que dados disponíveis ({len(cep_df)}), retornando todos os dados.", UserWarning)
            return cep_df.reset_index(drop=True)
        
        return cep_df.sample(n=tamanho_amostra, random_state=semente).reset_index(drop=True)
    
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar CEPs: {e}")
