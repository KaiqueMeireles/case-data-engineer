import os

def garantir_diretorio(caminho: str) -> None:
    """Garante que o diretório especificado exista.

    Verifica a existência do caminho informado e, caso não exista,
    cria a estrutura de pastas necessária.

    Args:
        caminho (str): Caminho da pasta a ser verificada/criada.
    """
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        print(f"[I/O] Diretório preparado: {caminho}")