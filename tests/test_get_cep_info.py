import random
import time
from typing import Any


def consultar_cep_mock(cep: str) -> dict[str, Any]:
    """Mock da função consultar_cep para testes.

    Simula o comportamento da API ViaCEP com delay aleatório.
    80% de sucesso, 20% de erro para testar tratamento de falhas.

    Args:
        cep (str): CEP a ser consultado.

    Returns:
        dict[str, Any]: Resultado simulado com status e dados.
    """
    # Simula o tempo de rede (delay aleatório)
    time.sleep(random.uniform(0.2, 1.5))

    # 80% de chance de sucesso e
    # 20% de chance de erro 
    # (para testar tratamento de erros).
    if random.random() <= 0.8:
        return {
            "cep": cep,
            "status": "sucesso",
            "dados": {
                "logradouro": "Rua Fictícia",
                "bairro": "Bairro Fictício",
                "localidade": "Cidade Fictícia",
                "uf": "SP",
            },
            "mensagem": "",
        }

    return {
        "cep": cep,
        "status": "erro",
        "dados": None,
        "mensagem": "CEP inválido ou inexistente.",
    }