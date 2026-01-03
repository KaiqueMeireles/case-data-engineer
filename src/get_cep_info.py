import requests

def consultar_cep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    
    resultado = {
        "cep": cep,
        "status": "erro",  # Assume erro até provar o contrário
        "dados": None,
        "mensagem": ""
    }

    try:
        response = requests.get(url, timeout=5)
        
        # Verificando o Status Code da resposta da API
        if response.status_code == 200:
            dic_retorno = response.json()
            
            # Tratando o caso de CEP com problema (vazio, errado etc.)
            if dic_retorno.get("erro"):
                resultado['mensagem'] = "CEP inválido ou inexistente."
                
            else:
                resultado['status'] = "sucesso"
                resultado['dados'] = dic_retorno
            
        else:
            # Tratando outros códigos de erro HTTP
            resultado['mensagem'] = f"Erro HTTP {response.status_code}"

    except Exception as e:
        # Tratando exceções de conexão ou timeout
        resultado['mensagem'] = f"Erro de conexão: {str(e)}"

    return resultado

# # Área de Testes
# if __name__ == "__main__":
#     # Teste 1: Sucesso
#     print(consultar_cep("01001000")) 
    
#     # Teste 2: CEP inexistente
#     print(consultar_cep("99999999"))