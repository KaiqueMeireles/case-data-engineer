import logging
from typing import Optional, Dict, Any, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.rate_limit import aguardar_permissao_api

logger = logging.getLogger(__name__)


def _criar_sessao() -> requests.Session:
    """Cria e configura uma sessão HTTP com política de Retry e Headers.

    Returns:
        requests.Session: Objeto de sessão configurado.
    """
    session = requests.Session()

    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Compatible; PipelineETL/1.0)"
    })

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def _validar_input_cep(cep_raw: str) -> Optional[str]:
    """Higieniza e valida o formato do CEP recebido.

    Args:
        cep_raw (str): O CEP bruto.

    Returns:
        Optional[str]: CEP limpo (8 dígitos) ou None.
    """
    if not isinstance(cep_raw, str):
        return None
    cep_limpo = cep_raw.replace("-", "").replace(".", "").strip()

    if len(cep_limpo) == 8 and cep_limpo.isdigit():
        return cep_limpo
    return None


# Sessão global reutilizável
session = _criar_sessao()


def consultar_cep(cep: str) -> Dict[str, Union[str, Optional[Dict[str, Any]]]]:
    """Consulta informações de endereço para um CEP na API ViaCEP.

    Args:
        cep (str): O CEP a ser consultado.

    Returns:
        Dict[str, Any]: Dicionário com status e dados do endereço.
    """
    # Controle de Rate Limit (bloqueia se necessário)
    aguardar_permissao_api()

    resultado = {
        "cep": cep,
        "status": "erro",
        "dados": None,
        "mensagem": "",
    }

    cep_valido = _validar_input_cep(cep)

    if cep_valido is None:
        resultado["mensagem"] = "Formato inválido."
        return resultado

    try:
        response = session.get(
            f"https://viacep.com.br/ws/{cep_valido}/json/",
            timeout=10,
        )

        if response.status_code == 200:
            dados = response.json()
            if "erro" in dados:
                resultado["mensagem"] = "CEP inexistente."
            else:
                resultado["status"] = "sucesso"
                resultado["dados"] = dados

        elif response.status_code == 429:
            resultado["mensagem"] = "Erro 429: Rate Limit da API."
            logger.error(f"Bloqueio 429 detectado no CEP {cep}")

        else:
            resultado["mensagem"] = f"Erro HTTP {response.status_code}"

    except Exception as e:
        resultado["mensagem"] = f"Erro de conexão: {str(e)}"
        logger.error(f"[{cep}] Exception: {e}")

    return resultado