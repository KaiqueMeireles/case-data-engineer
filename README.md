# Pipeline ETL de Endereços com CEP (ViaCEP API)

Pipeline de ETL (Extraction, Transformation and Load) robusto e resiliente para processamento de dados de endereços brasileiros. O sistema consome uma lista local de CEPs, consulta a API ViaCEP de forma paralela com controle inteligente de rate limiting thread-safe, aplica validações rigorosas de qualidade e consistência, e guarda/exporta os resultados em múltiplos formatos (SQLite, JSON, XML e CSV).
## Destaques

- **Rate Limiting Thread-Safe:** `Lock` + janela deslizante garantem máximo de 50 req/min mesmo com 3 threads paralelas
- **Processamento Paralelo:** `ThreadPoolExecutor` reduz tempo de execução em ~3x (166min → 55min para 10k CEPs)
- **Resiliência:** retry automático com backoff exponencial + timeout configurável
- **Logging Dual:** console limpo (ERROR+) para execução, arquivo completo (DEBUG+) para análise
- **Validação Completa:** constraint UNIQUE no DB, verificação de duplicatas, limpeza de dados
- **Mock Integrado:** modo offline com 500 workers para testes rápidos sem dependência da API
## Pré-requisitos

- **Python 3.12+**
- **Pip** (gerenciador de pacotes Python)

## Dataset

O arquivo `cep.tsv.zip` foi obtido do dataset público no Kaggle:

**Fonte:** [CEP Brasil - Kaggle](https://www.kaggle.com/datasets/diegomariano/cep-brasil?select=cep.tsv)

O arquivo já está disponível no projeto em `data/input/cep.tsv.zip`.

## Instalação e execução

```bash
# 1. Clone o repositório (ou descompacte o arquivo)
git clone <url-do-repositorio>
cd case-data-engineer

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Execute a pipeline
python main.py
```

## Configuração (opcional)

Edite `main.py` para ajustar:

```python
tamanho_amostra = 10_000  # CEPs a processar (use 10-100 para testes rápidos, mas o padrão é 10.000)
caminho_arquivo = 'data/input/dataset_origin.txt'  # Arquivo de entrada com a lista de CEPs
is_local = False      # False: API real (ViaCEP) | True: mock para testes offline
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

## Tecnologias utilizadas

- **Python 3.12+:** linguagem principal.
- **Pandas:** manipulação e transformação de dados.
- **SQLite3:** persistência relacional.
- **Requests:** cliente HTTP com retry automático.
- **lxml:** exportação em formato XML.
- **Threading:** paralelização com `Lock` para thread-safety.

## Estrutura do projeto

```
case-data-engineer/
├── data/
│   ├── input/
│   │   ├── dataset_origin.txt     # Dataset de CEPs (texto)
│   │   └── cep.tsv.zip            # Dataset compactado (Kaggle)
│   └── output/                     # Gerado automaticamente
│       ├── base_enderecos.db
│       ├── enderecos.json
│       ├── enderecos.xml
│       ├── enderecos_erros.csv
│       └── pipeline_diagnosis.log
├── src/
│   ├── __init__.py
│   ├── etl.py                      # Orquestrador principal
│   ├── get_cep_info.py             # Cliente API ViaCEP
│   ├── rate_limit.py               # Rate limiting thread-safe
│   ├── get_cep_list.py             # Carregamento e amostragem
│   ├── data_transformation.py      # Validação e normalização
│   ├── database.py                 # Persistência SQLite
│   ├── export_data.py              # Exportação múltiplos formatos
│   └── utils.py                    # Logging e utilitários
├── tests/
│   ├── __init__.py
│   └── test_get_cep_info.py        # Mock para testes offline
├── main.py                          # Ponto de entrada
├── requirements.txt                 # Dependências do projeto
└── README.md                        # Documentação
```

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

### Rate Limiting Thread-Safe com Janela Deslizante
- **Problema:** Com 3 workers paralelos, simples delays causavam race conditions e violações do limite de 50 req/min.
- **Solução:** Implementação de `Lock` + janela deslizante de 60 segundos que rastreia todas as requisições.
- **Funcionamento:** 
  - Cada thread deve adquirir o lock antes de fazer uma requisição.
  - Sistema verifica quantas requisições foram feitas nos últimos 60 segundos.
  - Se atingiu o limite (50), calcula quanto tempo esperar para a requisição mais antiga sair da janela.
  - Distribui requisições uniformemente (~1.2s entre cada).
- **Resultado:** Garantia absoluta de não ultrapassar 50 req/min, mesmo com processamento paralelo.


### Limpeza Automática de Arquivos e Banco de Dados
- Função `limpar_arquivos_saida()` em `src/export_data.py` remove arquivos JSON, XML e CSV de execuções anteriores.
- Função `_limpar_log_anterior()` em `src/utils.py` remove o arquivo de diagnóstico anterior.
- Banco de dados recriado a cada execução (reset=True) para garantir dados limpos.
- Chamadas automáticas no início do pipeline.
- Evita acúmulo e confusão com resultados de execuções anteriores.

### Logging com Dois Níveis
- **Console:** apenas ERROR e CRITICAL (console de execução mais limpo, mostra só problemas graves).
- **Arquivo (pipeline_diagnosis.log):** todos os níveis (DEBUG, INFO, WARNING, ERROR, CRITICAL) para análise detalhada.
- Função de configuração centralizada em `src/utils.py` (`configurar_logging()`).
- Chamada centralizada em `main.py`, no ponto de entrada da pipeline.
- Eventos importantes registrados com `logger.warning()` e `logger.info()`.
- Exceções críticas disparam `raise` apropriadamente.



### Separação de Responsabilidades
- **Modularização:** cada módulo tem propósito e escopo bem definidos.
  - `main.py` → ponto de entrada da aplicação.
  - `etl.py` → orquestração do pipeline.
  - `get_cep_info.py` → comunicação com API.
  - `rate_limit.py` → controle de rate limiting thread-safe com Lock e janela deslizante.
  - `get_cep_list.py` → carregamento dos dados iniciais.
  - `data_transformation.py` → transformação e validação de dados.
  - `database.py` → criação, inserção e configuração do banco de dados.
  - `export_data.py` → exportação e formatação dos resultados.
  - `utils.py` → funções utilitárias gerais (logging).
  - `tests/test_get_cep_info.py` → mock da API para testes offline.
- **Type Hints:** anotações de tipo em todas as funções para melhor IDE support e type checking.
- **Docstrings <b><span style="color: #4285F4;">G</span><span style="color: #EA4335;">o</span><span style="color: #FBBC04;">o</span><span style="color: #4285F4;">g</span><span style="color: #34A853;">l</span><span style="color: #EA4335;">e</span></b> Style:** documentação consistente e legível.

### Tratamento de erros
- Validação de entrada com `ValueError` e `FileNotFoundError`.
- Erros de rede tratados com retry automático.
- Erros no processamento do CEP registrados em CSV para análise posterior.

### Estratégia de Mock e Testes
- Implementação de um módulo _mock_ (`tests/test_get_cep_info.py`) para simular a API.
- Permite validação de fluxo, testes de carga e desenvolvimento offline sem risco de bloqueio.

## Performance

- **Modo local (mock):** 500 workers → testes muito rápidos (10.000 CEPs em ~1-2 minutos).
- **Modo API com rate limiting thread-safe:** 3 workers + controle rigoroso com Lock.
  - Sistema garante máximo de 50 requisições por minuto.
  - Distribui requisições uniformemente (~1.2s entre cada).
  - Estimativa para 10.000 CEPs: ~3-4 horas.
- **Retry policy:** backoff exponencial (total=2, backoff_factor=0.5) para lidar com instabilidades.
- **Timeout:** 5 segundos por requisição para evitar travamentos.
- **Pool de conexões:** `HTTPAdapter` reutiliza conexões HTTP para reduzir overhead.


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
