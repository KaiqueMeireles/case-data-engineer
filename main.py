from src.etl import executar_pipeline
from src.utils import configurar_logging

if __name__ == "__main__":
    configurar_logging()

    tamanho_amostra = 10_000
    caminho_arquivo = 'data/input/cep.tsv.zip'
    is_local = False  # True para usar mock, False para usar a API real

    executar_pipeline(
        tamanho_amostra=tamanho_amostra,
        caminho_arquivo=caminho_arquivo,
        is_local=is_local,
    )