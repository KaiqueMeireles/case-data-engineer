import sqlite3
import pandas as pd
import os
from src.utils import garantir_diretorio

def criar_banco(
    output_folder: str = "data/output/",
    reset: bool = True,
) -> str:
    """Cria o banco de dados SQLite com schema de endereços.

    Cria o arquivo base_enderecos.db e a tabela 'enderecos' com
    as colunas esperadas. Se reset=True, deleta dados existentes.
    Caso contrário, apenas cria se não existir.

    Args:
        output_folder (str): Caminho da pasta para salvar o banco.
            Padrão: "data/output/".
        reset (bool): Se True, deleta tabela existente para recriar.
            Se False, mantém dados existentes.
            Padrão: True (para verem o funcionamento da criação do banco).

    Returns:
        str: Caminho completo do arquivo do banco criado.
    """
    garantir_diretorio(output_folder)

    caminho_db = os.path.join(output_folder, "base_enderecos.db")

    try:
        with sqlite3.connect(caminho_db) as conn:
            cursor = conn.cursor()

            # Recria o banco se solicitado
            if reset:
                cursor.execute("DROP TABLE IF EXISTS enderecos;")
                print("[SQL] Tabela 'enderecos' removida (reset).")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS enderecos (
                    id_endereco INTEGER PRIMARY KEY AUTOINCREMENT,
                    cep TEXT NOT NULL UNIQUE,
                    logradouro TEXT,
                    complemento TEXT,
                    unidade TEXT,
                    bairro TEXT,
                    localidade TEXT,
                    uf TEXT,
                    estado TEXT,
                    regiao TEXT,
                    ibge TEXT,
                    gia TEXT,
                    ddd TEXT,
                    siafi TEXT,
                    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            conn.commit()

        print(f"[SQL] Banco criado/validado: {caminho_db}")
        return caminho_db

    except Exception as e:
        print(f"[SQL] Erro ao criar banco: {e}")
        raise


def inserir_dados(
    df: pd.DataFrame,
    output_folder: str = "data/output/",
) -> None:
    """Insere dados normalizados no banco SQLite.

    Insere os registros do DataFrame na tabela 'enderecos'.
    Verifica CEPs existentes e ignora registros duplicados,
    mantendo apenas registros novos.

    Args:
        df (pd.DataFrame): DataFrame com colunas [cep, logradouro, uf, ...].
        output_folder (str): Caminho da pasta do banco.
            Padrão: "data/output/".

    Raises:
        FileNotFoundError: Se o banco não foi criado.
        Exception: Se houver erro na inserção.
    """
    # Verifica se o banco existe
    caminho_db = os.path.join(output_folder, "base_enderecos.db")
    if not os.path.exists(caminho_db):
        raise FileNotFoundError(
            f"Banco de dados não encontrado em {caminho_db}."
        )

    try:
        with sqlite3.connect(caminho_db) as conn:
            # Consulta CEPs existentes e filtra os novos
            ceps_existentes = pd.read_sql(
                "SELECT cep FROM enderecos",
                conn
            )
            
            df_novos = df[~df['cep'].isin(ceps_existentes['cep'])].copy()
            num_duplicados = len(df) - len(df_novos)

            # Alerta sobre CEPs duplicados e insere os novos.
            if num_duplicados > 0:
                print(
                    f"[SQL] Aviso: {num_duplicados} CEP(s) já existem "
                    f"no banco. Serão ignorados."
                )
                
            if len(df_novos) > 0:
                df_novos.to_sql(
                    name="enderecos",
                    con=conn,
                    if_exists="append",
                    index=False
                )

                conn.commit()

                print(
                    f"[SQL] Sucesso: {len(df_novos)} endereço(s) "
                    "inserido(s) no banco."
                )
                
            else:
                print("[SQL] Nenhum CEP novo para inserir.")

    except Exception as e:
        print(f"[SQL] Erro ao inserir dados: {e}")
        raise