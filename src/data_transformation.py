import pandas as pd
import warnings


def _validar_ceps_duplicados(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Valida e remove CEPs duplicados com verificação de inconsistências.

    Identifica CEPs duplicados e verifica se os dados associados são
    idênticos. Se encontrar CEPs duplicados com dados inconsistentes,
    emite um alerta. Remove duplicatas, mantendo a primeira ocorrência.

    Args:
        df (pd.DataFrame): DataFrame contendo coluna 'cep'.

    Returns:
        pd.DataFrame: DataFrame sem duplicatas de CEP.
    """
    df_processado = df.copy()

    # Identifica CEPs duplicados.
    ceps_duplicados = df_processado[
        df_processado.duplicated(subset=['cep'], keep=False)
    ]

    if len(ceps_duplicados) > 0:
        
        # Agrupa por CEP para comparar dados.
        for cep, grupo in ceps_duplicados.groupby('cep'):
            
            # Compara se todos os registros do mesmo CEP são idênticos.
            colunas_comparacao = [
                col for col in grupo.columns if col != 'cep'
            ]
            dados_unicos = grupo[colunas_comparacao].drop_duplicates()

            if len(dados_unicos) > 1:
                warnings.warn(
                    f"[ALERTA] CEP {cep} possui dados inconsistentes em "
                    f"múltiplos registros. Mantendo primeiro.",
                    UserWarning
                )

    # Remove duplicatas de CEP (mantém primeira ocorrência).
    num_antes = len(df_processado)
    df_processado = df_processado.drop_duplicates(
        subset=['cep'],
        keep='first'
    )
    
    num_duplicatas = num_antes - len(df_processado)

    if num_duplicatas > 0:
        warnings.warn(
            f"[ALERTA] Removidas {num_duplicatas} duplicata(s) de CEP.",
            UserWarning
        )

    return df_processado


def validar_dados_transformados(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Orquestra validações dos dados transformados.

    Realiza validações pós-transformação em ordem:
    1. Valida CEPs duplicados com inconsistências
    2. Converte strings vazias para NULL
    3. Verifica se logradouro está preenchido
    4. Valida se UF tem exatamente 2 caracteres

    Args:
        df (pd.DataFrame): DataFrame com colunas [cep, logradouro, uf, ...].

    Returns:
        pd.DataFrame: DataFrame validado e limpo.
    """
    df_validado = df.copy()

    # Valida CEPs duplicados.
    df_validado = _validar_ceps_duplicados(df_validado)

    # Substitui '' e espaços em branco por NaN (valor nulo).
    df_validado = df_validado.replace(r'^\s*$', pd.NA, regex=True)

    # Verifica logradouro nulo.
    logradouro_nulo = df_validado['logradouro'].isna().sum()

    if logradouro_nulo > 0:
        warnings.warn(
            f"[ALERTA] {logradouro_nulo} registro(s) com logradouro nulo.",
            UserWarning
        )

    # Verifica se UF tem 2 caracteres.
    uf_invalido = df_validado[
        (df_validado['uf'].notna()) &
        (df_validado['uf'].str.len() != 2)
    ]

    if len(uf_invalido) > 0:
        warnings.warn(
            f"[ALERTA] {len(uf_invalido)} registro(s) com UF inválido.",
            UserWarning
        )

    return df_validado


def normalizar_resultados(
    df_sucesso: pd.DataFrame,
) -> pd.DataFrame:
    """Normaliza DataFrame de sucessos expandindo coluna 'dados'.

    Expande a coluna 'dados' (que contém dicts) em múltiplas colunas
    individuais (logradouro, bairro, localidade, uf, etc).

    Args:
        df_sucesso (pd.DataFrame): DataFrame contendo apenas registros
            com status='sucesso', com colunas [cep, status, dados, mensagem].

    Returns:
        pd.DataFrame: DataFrame normalizado com colunas [cep, logradouro,
            bairro, localidade, uf, ...].
    """
    # Transformando coluna 'dados' em múltiplas colunas.
    dados_expandidos = pd.json_normalize(df_sucesso['dados'])

    # Removendo coluna 'cep' dos dados expandidos.
    dados_expandidos = dados_expandidos.drop(columns=['cep'], errors='ignore')

    # Mantendo apenas CEP (limpo) e dados expandidos.
    resultado = pd.concat(
        [df_sucesso[['cep']].reset_index(drop=True), dados_expandidos],
        axis=1
    )

    return resultado