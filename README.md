# Pipeline ETL de endereços com CEP (ViaCEP API)

Pipeline de ETL (Extraction, Transformation and Load) robusto e resiliente para processamento de dados de endereços brasileiros. O sistema consome uma lista local de CEPs, consulta a API ViaCEP de forma paralela com retry automático, aplica validações rigorosas de qualidade e consistência e guarda os resultados em múltiplos formatos (SQLite, JSON, XML e CSV).

## Pré-requisitos

- **Python 3.12+**.
- **Pip** (gerenciador de pacotes Python).
- **Git** (para clonar o repositório).
- **Arquivo de entrada:** `data/input/cep.tsv.zip` (lista de CEPs).

## Destaques do projeto

- **Processamento paralelo:** `ThreadPoolExecutor` otimiza requisições de I/O, reduzindo drasticamente o tempo de execução.
- **Resiliência e retry:** sessões HTTP com política de *Backoff Exponencial* (até 5 tentativas) para lidar com instabilidades de rede e Rate Limiting.
- **Validação de dados:** verificação defensiva de entrada, higienização de strings, detecção de duplicatas com inconsistências.
- **Logging centralizado:** sistema de logs ao invés de `prints`, permitindo rastreabilidade completa.
- **Testes offline:** mock integrado para testes de funcionamento da pipeline sem dependência da API externa.

## Tecnologias e ferramentas utilizadas

- **Pandas:** manipulação e transformação de dados.
- **SQLite3:** persistência relacional.
- **Requests:** cliente HTTP com retry automático.
- **lxml:** exportação em XML.
- **Pathlib/OS:** gerenciamento cross-platform.

## Estrutura

```
├── data/
│   ├── input/               # cep.tsv.zip (fonte de dados do Kaggle)
│   └── output/              # base_enderecos.db, JSON, XML, CSV
├── src/
│   ├── etl.py               # Orquestrador principal
│   ├── get_cep_info.py      # Cliente API com retry policy
│   ├── get_cep_list.py      # Carregamento e amostragem
│   ├── data_transformation.py # Validação e normalização
│   ├── database.py          # Persistência SQLite
│   ├── export_data.py       # Exportação múltiplos formatos
│   ├── utils.py             # Logging e utilitários
│   └── __init__.py
├── tests/
│   ├── test_get_cep_info.py # Mock para testes
│   └── __init__.py
├── main.py                  # Ponto de entrada
├── requirements.txt
└── README.md
```

## Instalação e execução

### 1. Dependências

```bash
pip install -r requirements.txt
```

### 2. Dataset

O arquivo `cep.tsv.zip` foi obtido do dataset público no Kaggle:

**Fonte:** [CEP Brasil - Kaggle](https://www.kaggle.com/datasets/diegomariano/cep-brasil?select=cep.tsv).

O arquivo já está disponível no projeto em `data/input/cep.tsv.zip`.

### 3. Configuração

Edite `main.py` para ajustar parâmetros caso necessário:

```python
tamanho_amostra = 10000              # Número de CEPs a processar
caminho_arquivo = 'data/input/cep.tsv.zip'  # Arquivo de entrada
is_local = False                  # False: API real | True: Mock
```

### 4. Execução

```bash
python main.py
```

## Saídas geradas após o processamento

Arquivos criados em `data/output/`:

| Arquivo | Descrição |
|---------|-----------|
| `base_enderecos.db` | Banco SQLite com endereços validados. |
| `enderecos.json` | Dados normalizados em JSON. |
| `enderecos.xml` | Dados normalizados em XML. |
| `enderecos_erros.csv` | Log de CEPs com erro (categorizados e com _timestamp_). |
| `pipeline_diagnosis.log` | Arquivo de log com todos os eventos da execução. |

## Fluxo da pipeline

```
1. Validação de entrada (tamanho_amostra, arquivo)
2. Carregamento e amostragem de CEPs
3. Consultas simultâneas à API ViaCEP (com retry automático)
4. Separação de resultados: sucesso vs. erro
5. Normalização e validação de dados
6. Gravação em SQLite
7. Exportação em JSON e XML
8. Categorização e export de erros em CSV

*Observabilidade: geração contínua de logs de diagnóstico em todas as etapas
```

## Schema do banco de dados

**Tabela `enderecos`:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_endereco` | INTEGER (PK) | Identificador único (auto-incremento). |
| `cep` | TEXT (UNIQUE) | CEP sem formatação (8 dígitos). |
| `logradouro` | TEXT | Rua/Avenida/etc. |
| `complemento` | TEXT | Informação complementar. |
| `unidade` | TEXT | Número da unidade. |
| `bairro` | TEXT | Bairro. |
| `localidade` | TEXT | Cidade. |
| `uf` | TEXT | Estado (sigla 2 caracteres). |
| `estado` | TEXT | Nome do estado. |
| `regiao` | TEXT | Região geográfica. |
| `ibge` | TEXT | Código IBGE. |
| `gia` | TEXT | Código GIA (SP). |
| `ddd` | TEXT | Código de área telefônico. |
| `siafi` | TEXT | Código SIAFI. |
| `data_registro` | TIMESTAMP | Data/hora do registro (auto). |

## Decisões de design

### Persistência segura
- CEP com constraint `UNIQUE` previne duplicidade física.
- Validação lógica elimina duplicatas antes da inserção.
- Tratamento robusto de CEPs inconsistentes.

### Processamento paralelo com ThreadPoolExecutor
- **Motivo:** requisições HTTP são operações que aguardam resposta da rede (não consomem CPU enquanto esperam).
- **Implementação:** `ThreadPoolExecutor` executa múltiplas requisições simultaneamente.
- **Benefício:** redução drástica do tempo total → enquanto uma requisição aguarda a rede, outra está sendo feita.

**Exemplo de impacto (para os 10.000 CEPs do case):**
```
Assumindo um tempo de resposta médio de 1.0s por requisição (delay 0.5-1.0s com jitter + latência):

Sequencial (1 thread):
  10.000 CEPs × 1.0s = 10.000 segundos ≈ 166 minutos

Com 3 workers (modo API):
  10.000 CEPs ÷ 3 × 1.0s ≈ 3.333 lotes × 1.0s ≈ 55 minutos
```
**Resultado: 3x mais rápido** (166 minutos → 55 minutos)

### Encapsulamento
- Funções privadas (`_funcao`) protegem lógica interna.
- Módulos especializados com responsabilidades claras.

### Padrões de projeto
- **Separação de responsabilidades:** cada módulo têm propósito e escopo bem definidos.
  - `etl.py` → orquestração do pipeline.
  - `get_cep_info.py` → comunicação com API.
  - `get_cep_list.py` → carregamento dos dados iniciais.
  - `data_transformation.py` → transformação e validação de dados.
  - `database.py` → criação, inserção e configuração do banco de dados.
  - `export_data.py` → exportação e formatação dos resultados.
  - `utils.py` → funções utilitárias gerais.
- **Conventional Commits:** histórico de commits com padrão semântico (`refactor:`, `style:`, `feat:`).
- **Type Hints:** anotações de tipo em todas as funções para melhor IDE support e type checking.
- **Docstrings <b><span style="color: #4285F4;">G</span><span style="color: #EA4335;">o</span><span style="color: #FBBC04;">o</span><span style="color: #4285F4;">g</span><span style="color: #34A853;">l</span><span style="color: #EA4335;">e</span></b> Style:** documentação consistente e legível.

### Logging
- Função de configuração centralizada em `src/utils.py` (`configurar_logging()`).
- Chamada centralizada em `main.py`, no ponto de entrada da pipeline.
- Eventos importantes registrados com `logger.warning()` e `logger.info()`.
- Exceções críticas disparam `raise` apropriadamente.

### Tratamento de erros
- Validação de entrada com `ValueError` e `FileNotFoundError`.
- Erros de rede tratados com retry automático.
- Erros no processamento do CEP registrados em CSV para análise posterior.

### Estratégia de Mock e Testes
- Implementação de um módulo _mock_ (`tests/test_get_cep_info.py`) para simular a API.
- Permite validação de fluxo, testes de carga e desenvolvimento offline sem risco de bloqueio.

## Performance

- **Modo local (mock):** 500 workers → testes + rápidos.
- **Modo API:** 3 workers + delay de 0.5-1.0s (com jitter) → respeito ao rate limiting para evitar bloqueios.
- **Retry policy:** backoff exponencial (2s, 4s, 8s, 16s até 32s) para lidar com possíveis instabilidades.

## Qualidade do código

- **PEP 8:** formatação padronizada.
- **Logging:** rastreabilidade completa em `data/output/pipeline_diagnosis.log`.
- **Reprodutibilidade:** utilização de semente (_seed_) para fixar a amostra aleatória e permitir a reprodutibilidade de cada execução.

## Exemplos de uso

Execução utilizando a base local (mock) para validar os mecanismos da pipeline:
```python
from src.etl import executar_pipeline

# Processar 1969 CEPs em modo local
executar_pipeline(
    tamanho_amostra=1969,
    caminho_arquivo='data/input/cep.tsv.zip',
    is_local=True
)
```

Execução utilizando a API do ViaCEP:
```python
from src.etl import executar_pipeline

# Processar 1969 CEPs com a API ViaCEP
executar_pipeline(
    tamanho_amostra=1969,
    caminho_arquivo='data/input/cep.tsv.zip',
    is_local=False
)
```

## Conhecimento em tecnologias Cloud & Big Data (AWS)

### AWS Glue
- **Compreensão:** plataforma gerenciada de ETL baseada em Apache Spark.
- **Aplicabilidade:** este pipeline local poderia ser escalado para **AWS Glue Jobs**, executando a transformação de dados em cluster (distribuído) e integrando com o catálogo de dados.
- **Experiência:** possuo conhecimento teórico sobre o propósito da ferramenta, embora minha experiência prática atual seja focada em **S3** e **Athena**.

### AWS Lambda
- **Compreensão:** computação _serverless_ orientada a eventos.
- **Aplicabilidade:** ideal para atuar como gatilho (_trigger_) automático quando um novo arquivo chega ao S3, iniciando a execução do pipeline.
- **Limitação reconhecida:** devido ao timeout máximo de 15 minutos, não seria ideal para o processamento pesado dos dados em si, mas sim para a orquestração.
- **Experiência:** conhecimento conceitual sobre arquitetura serverless.

### Ferramentas com maior proficiência prática
- **Amazon S3:** armazenamento de objetos e estruturação de Data Lakes.
- **Amazon Athena:** consultas SQL diretas em arquivos (S3) para validação e análise exploratória.


## Próximos passos e melhorias

- **Containerização:** criação de `Dockerfile` para isolar o ambiente e garantir execução idêntica em qualquer máquina.
- **Testes Automatizados:** implementação de suíte de testes unitários (`pytest`) para cobrir as funções de transformação e validação de CEP.
- **CI/CD:** configuração de pipeline no GitHub Actions para rodar linters (flake8) e testes a cada push.
- **Orquestração:** para fluxos mais complexos, adoção de **Airflow** ou **Prefect** para gerenciar dependências entre tarefas.


## Licença

Desenvolvido como case para avaliação de competências para engenheiro de dados.
