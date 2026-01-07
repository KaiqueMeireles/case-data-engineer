from pathlib import Path

from src.etl import executar_pipeline

if __name__ == "__main__":
    tamanho_amostra = 30
    caminho_arquivo = 'data/input/cep.tsv.zip'
    is_local = False  # True para usar mock, False para usar a API real

    executar_pipeline(
        tamanho_amostra=tamanho_amostra,
        caminho_arquivo=caminho_arquivo,
        is_local=is_local,
    )