import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from typing import Optional

def _criar_sessao() -> requests.Session:
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


def _validar_input_cep(cep_raw: str) -> Optional[str]:
    """Higieniza e valida o formato do CEP.

    Remove caracteres de formatação (pontos e traços) e verifica se o
    resultado consiste em exatamente 8 dígitos numéricos.

    Args:
        cep_raw (str): O CEP bruto conforme lido da fonte de dados.

    Returns:
        Optional[str]: O CEP limpo (apenas números) se for válido,
        ou None se o formato estiver incorreto.
    """
    if not isinstance(cep_raw, str):
        return None

    # Removendo formatações comuns.
    cep_limpo = cep_raw.replace("-", "").replace(".", "").strip()

    # Verificando se tem 8 dígitos e se é numérico.
    if len(cep_limpo) != 8 or not cep_limpo.isdigit():
        return None
    
    return cep_limpo


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
    
    # Delay para respeitar rate limiting da API.
    time.sleep(0.2)
    
    resultado = {
        "cep": cep,
        "status": "erro",  # Assume erro até provar o contrário.
        "dados": None,
        "mensagem": ""
    }

    # Valida o CEP antes de fazer a requisição.
    cep_valido = _validar_input_cep(cep)
    
    if cep_valido is None:
        resultado['mensagem'] = (
            "Formato inválido: CEP deve conter exatamente "
            "8 dígitos numéricos."
        )
        return resultado

    try:
        response = session.get(
            f"https://viacep.com.br/ws/{cep_valido}/json/",
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
session = _criar_sessao()