import time
from threading import Lock

# Intervalo de 1.05 segundos garante matematicamente que
# NUNCA passaremos de ~57 requisições por minuto (margem de segurança).
_INTERVALO_MINIMO = 1.05

_ultimo_acesso = 0.0
_lock = Lock()


def aguardar_permissao_api() -> None:
    """Bloqueia a execução atual até que seja seguro fazer uma nova requisição.

    Implementa uma lógica de espaçamento temporal mínimo. Se uma requisição
    tentar ocorrer muito perto da anterior, esta função coloca a thread atual
    para dormir pelo tempo restante necessário.
    """
    global _ultimo_acesso

    with _lock:
        agora = time.monotonic()
        tempo_desde_ultimo = agora - _ultimo_acesso

        if tempo_desde_ultimo < _INTERVALO_MINIMO:
            tempo_espera = _INTERVALO_MINIMO - tempo_desde_ultimo
            time.sleep(tempo_espera)

        _ultimo_acesso = time.monotonic()