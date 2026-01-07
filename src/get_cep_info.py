import logging
import random
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _criar_sessao() -> requests.Session:
    """Cria uma sessão HTTP com política de Retry automática.

    Configura uma sessão reutilizável com estratégia de retry para lidar com
    falhas temporárias da API (rate limiting, timeouts, erros 5xx).

    Returns:
        requests.Session: Sessão HTTP configurada com retry automático.
    """
    session = requests.Session()

    # User-Agent para não parecer um bot.
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })

    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
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

    cep_limpo = cep_raw.replace("-", "").replace(".", "").strip()

    if len(cep_limpo) != 8 or not cep_limpo.isdigit():
        return None

    return cep_limpo


def consultar_cep(cep: str) -> dict:
    """Consulta informações de endereço para um CEP via API ViaCEP.

    Realiza uma requisição GET à API ViaCEP com tratamento de erros de
    conexão, timeout e CEPs inválidos. Implementa delay com jitter para
    respeitar rate limiting e evitar bloqueios.

    Args:
        cep (str): Código de Endereçamento Postal (CEP) a ser consultado.

    Returns:
        dict: Dicionário contendo:
            - 'cep': CEP consultado
            - 'status': 'sucesso' ou 'erro'
            - 'dados': Dados do endereço (dict) ou None se erro
            - 'mensagem': Mensagem de erro ou string vazia se sucesso
    """
    # Jitter evita padrões previsíveis que parecem bot
    delay = random.uniform(0.5, 1.0)
    time.sleep(delay)

    resultado = {
        "cep": cep,
        "status": "erro",
        "dados": None,
        "mensagem": "",
    }

    cep_valido = _validar_input_cep(cep)

    if cep_valido is None:
        resultado["mensagem"] = (
            "Formato inválido: CEP deve conter exatamente "
            "8 dígitos numéricos."
        )
        return resultado

    try:
        response = session.get(
            f"https://viacep.com.br/ws/{cep_valido}/json/",
            timeout=15,
        )

        if response.status_code == 200:
            dic_retorno = response.json()

            if dic_retorno.get("erro"):
                resultado["mensagem"] = "CEP inválido ou inexistente."
            else:
                resultado["status"] = "sucesso"
                resultado["dados"] = dic_retorno
        else:
            resultado["mensagem"] = f"Erro HTTP {response.status_code}"

    except Exception as e:
        resultado["mensagem"] = f"Erro de conexão: {str(e)}"

    return resultado


# Sessão global reutilizável com retry automático
session = _criar_sessao()