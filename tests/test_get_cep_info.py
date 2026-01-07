import time
import random
from typing import Any


def consultar_cep_mock(cep: str) -> dict[str, Any]:
    # Simula o tempo de rede (delay aleatório)
    time.sleep(random.uniform(0.2, 1.5))
    
    # 80% de chance de sucesso, 20% de erro (para testar tratamento de erros)
    if random.random() <= 0.8:
        return {
            "cep": cep,
            "status": "sucesso",
            "dados": {
                "logradouro": "Rua Fictícia",
                "bairro": "Bairro Fictício",
                "localidade": "Cidade Fictícia",
                "uf": "SP"
            },
            "mensagem": ""
        }
        
    else:
        return {
            "cep": cep,
            "status": "erro",
            "dados": None,
            "mensagem": "CEP inválido ou inexistente."
        }