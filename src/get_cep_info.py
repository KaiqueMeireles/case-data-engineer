import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

def criar_sessao() -> requests.Session:
    """Cria uma sessão HTTP com política de Retry automática.
    
    Configura uma sessão reutilizável com estratégia de retry para lidar com
    falhas temporárias da API (rate limiting, timeouts, erros 5xx).
    
    Returns:
        requests.Session: Sessão HTTP configurada com retry automático.
    """
    session = requests.Session()
    
    # Configuração do Retry.
    retry_strategy = Retry(
        total=5,  # Tenta no máximo 5 vezes extras.
        backoff_factor=2,  # Espera: 2s, 4s, 8s, 16s até 32s.
        status_forcelist=[429, 500, 502, 503, 504],  # Códigos de status para retry.
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def consultar_cep(cep: str) -> dict:
    """Consulta informações de endereço para um CEP via API ViaCEP.
    
    Realiza uma requisição GET à API ViaCEP com tratamento de erros de conexão,
    timeout e CEPs inválidos. Inclui delay para respeitar rate limiting.
    
    Args:
        cep (str): Código de Endereçamento Postal (CEP) a ser consultado.
    
    Returns:
        dict: Dicionário contendo:
            - 'cep': CEP consultado
            - 'status': 'sucesso' ou 'erro'
            - 'dados': Dados do endereço (dict) ou None se erro
            - 'mensagem': Mensagem de erro ou string vazia se sucesso
    """
    
    resultado = {
        "cep": cep,
        "status": "erro",  # Assume erro até provar o contrário.
        "dados": None,
        "mensagem": ""
    }

    try:
        response = session.get(
            f"https://viacep.com.br/ws/{cep}/json/",
            timeout=15
        )
        
        # Verificando o Status Code da resposta da API.
        if response.status_code == 200:
            dic_retorno = response.json()
            
            # Tratando o caso de CEP com problema (vazio, errado etc.).
            if dic_retorno.get("erro"):
                resultado['mensagem'] = "CEP inválido ou inexistente."
                
            else:
                resultado['status'] = "sucesso"
                resultado['dados'] = dic_retorno
            
        else:
            # Tratando outros códigos de erro HTTP.
            resultado['mensagem'] = f"Erro HTTP {response.status_code}"

    except Exception as e:
        # Tratando exceções de conexão ou timeout.
        resultado['mensagem'] = f"Erro de conexão: {str(e)}"

    return resultado

# Instanciando a sessão fora da função para que todos os
# workers usem essa mesma configuração.
session = criar_sessao()