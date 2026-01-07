from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from src.get_cep_list import carregar_lista_cep

# Teste com mock em caso de bloqueio da API por excesso de requisições
from tests.test_get_cep_info import consultar_cep_mock as consultar_cep
# from src.get_cep_info import consultar_cep


workers = 500  # Valor alto temporário por estar utilizando mock

with ThreadPoolExecutor(max_workers=workers) as executor:
    print("Carregando lista de CEPs...")
    lista_cep = carregar_lista_cep(tamanho_amostra=10000)
    
    print("Consultando CEPs...")
    # Convertendo os resultados diretamente para DataFrame
    resultados_df = pd.DataFrame(executor.map(consultar_cep, lista_cep['cep']))
    print("Consultas concluídas.")
    print("Salvando resultados...")
    
    # Separando resultados por status
    resultados_ok = resultados_df[resultados_df['status'] == 'sucesso']
    resultados_erro = resultados_df[resultados_df['status'] == 'erro']

    # Salvando os resultados em arquivos CSV
    resultados_ok.to_csv('data/output/cep_sucesso.csv', index=False)
    resultados_erro.to_csv('data/output/cep_erro.csv', index=False)
    print("Resultados salvos em 'data/output/'.")