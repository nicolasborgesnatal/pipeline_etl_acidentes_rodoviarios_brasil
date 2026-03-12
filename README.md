# 🚦 Pipeline ETL de Acidentes Rodoviários no Brasil

> Case técnico para a vaga de **Estagiário em Engenharia de Dados - Cobli**

---

## 📋 Sobre o Projeto

Este repositório contém um pipeline de ETL (Extração, Transformação e Carga) desenvolvido em **Python** e **SQL** para processar dados brutos de acidentes rodoviários no Brasil, disponibilizados pela PRF (Polícia Rodoviária Federal).

O pipeline segue a arquitetura **Medalhão** (Bronze → Prata), preservando os dados originais e gerando uma camada analítica limpa e filtrada para o estado de **São Paulo (SP)**.

---

## 📁 Estrutura do Repositório

```
.
├── acidentes_brasil.zip                                    # Dataset original compactado
├── pipeline_etl_acidentes_rodoviarios_brasil.py            # Script principal do pipeline em Python
├── pipeline_etl_acidentes_rodoviarios_brasil.sql           # Queries SQL do pipeline
├── camada_bronze.zip/
│   └── acidentes_brasil_raw.csv                            # Cópia exata dos dados brutos
├── camada_prata.zip/
│   └── acidentes_sp_tratado.csv                            # Dados limpos e filtrados (SP)
└── README.md
```

---

## ⚙️ Instruções de Execução

### Pré-requisitos

- Python **3.8+**
- pip (gerenciador de pacotes do Python)

### 1. Clone o repositório

```bash
git clone https://github.com/nicolasborgesnatal/pipeline_etl_acidentes_rodoviarios_brasil.git
cd pipeline_etl_acidentes_rodoviarios_brasil
```

### 2. Instale as dependências

O pipeline utiliza apenas a biblioteca **Pandas**. Para instalá-la:

```bash
pip install pandas
```

Ou, se preferir usar um arquivo de dependências:

```bash
pip install -r requirements.txt
```

> **`requirements.txt`**
> ```
> pandas>=1.5.0
> ```

### 3. Extraia o dataset

O arquivo `acidentes_brasil.zip` já está incluído no repositório. Extraia o conteúdo na raiz do projeto:

- **Windows:** clique com o botão direito no arquivo → **Extrair aqui**
- **Linux/macOS:** execute o comando abaixo no terminal:

```bash
unzip acidentes_brasil.zip
```

Após a extração, o arquivo `acidentes_brasil.csv` deve estar na raiz do projeto, no mesmo diretório do script.

### 4. Execute o pipeline

```bash
python pipeline_etl_acidentes_rodoviarios_brasil.py
```

Após a execução, os resultados estarão disponíveis em:
- `camada_bronze/acidentes_brasil_raw.csv` - dados brutos preservados
- `camada_prata/acidentes_sp_tratado.csv` - dados limpos e filtrados

---

### Execução via SQL (opcional)

Caso prefira rodar o pipeline via SQL, utilize o arquivo `pipeline_etl_acidentes_rodoviarios_brasil.sql` em um cliente MySQL (ex: MySQL Workbench, DBeaver).

> **Atenção:** antes de executar, atualize o caminho do arquivo na instrução `LOAD DATA LOCAL INFILE`:
> ```sql
> LOAD DATA LOCAL INFILE 'caminho/para/acidentes_brasil.csv'
> ```

---

## 🔄 Documentação das Transformações (Camada Prata)

### 1. Padronização de Colunas

**O que foi feito:** todos os nomes de colunas foram convertidos para letras minúsculas e os espaços substituídos por underscores (`_`). Também foi corrigido um erro de encoding que renomeava a coluna `id` para `ï»¿id`.

**Por quê:** nomes inconsistentes de colunas são uma das causas mais comuns de erros em pipelines. A padronização elimina ambiguidades, facilita queries SQL e segue boas práticas de nomenclatura em engenharia de dados.

---

### 2. Conversão de Datas

**O que foi feito:** a coluna `data_inversa`, que chegava como texto no formato `DD/MM/YYYY`, foi convertida para o tipo `datetime` (Python) / `DATE` (SQL). Registros com datas inválidas ou não-parseáveis foram sinalizados como `NaT` (Not a Time) e posteriormente removidos.

**Por quê:** datas armazenadas como string impedem qualquer operação temporal (ordenação, agrupamento por período, cálculo de intervalos). A conversão correta é pré-requisito para análises cronológicas confiáveis.

---

### 3. Tratamento de Valores Nulos

**O que foi feito:** colunas do tipo texto com valores ausentes foram preenchidas com a string `"Nao Informado"`. Registros cujo campo `data_inversa` permaneceu nulo após a conversão foram removidos.

**Por quê:** a estratégia de preenchimento foi escolhida para não perder registros que ainda possuem informações relevantes em outras colunas (como UF, tipo de acidente, etc.). Já a remoção de linhas sem data válida é justificada pelo fato de que a data é a dimensão central de qualquer análise temporal, ou seja, um registro sem ela não tem valor analítico.

---

### 4. Filtragem por Estado

**O que foi feito:** os dados foram filtrados para manter apenas ocorrências do estado de **São Paulo (UF = 'SP')**.

**Por quê:** São Paulo concentra o maior volume de acidentes do país e é um recorte representativo para análises regionais. A filtragem também reduz o volume de dados processados nas etapas seguintes, simulando um caso de uso real onde analistas trabalham com dados segmentados por região.

---

## 📊 Diferencial - Consulta SQL: Total de Acidentes por Dia

A query abaixo responde à pergunta *"Qual o total de acidentes por dia?"* utilizando os dados da camada Prata:

```sql
SELECT
    data_inversa,
    COUNT(*) AS total_acidentes
FROM acidentes_sp_prata
GROUP BY data_inversa
ORDER BY data_inversa;
```

**Equivalente em Python (Pandas):**

```python
acidentes_por_dia = (
    df.groupby("data_inversa")
    .size()
    .reset_index(name="total_acidentes")
    .sort_values("data_inversa")
)
print(acidentes_por_dia.head())
```

---

## ⏰ Diferencial - Automação: Agendamento Diário do Pipeline

Para executar o script automaticamente todos os dias, as principais abordagens são:

### Linux/macOS - Cron Job

Edite o crontab com `crontab -e` e adicione a linha abaixo para rodar o pipeline todo dia às 6h da manhã:

```bash
0 6 * * * /usr/bin/python3 /caminho/para/pipeline_etl_acidentes_rodoviarios_brasil.py >> /caminho/para/logs/etl.log 2>&1
```

### Windows - Agendador de Tarefas

Crie uma nova tarefa no **Agendador de Tarefas do Windows** apontando para o executável do Python e o script `pipeline_etl_acidentes_rodoviarios_brasil.py`, com gatilho diário no horário desejado.

### Nuvem - Soluções Gerenciadas

Em ambientes de produção, ferramentas como **Apache Airflow**, **AWS EventBridge + Lambda**, **Google Cloud Scheduler + Cloud Functions** ou **Azure Data Factory** são mais robustas, pois oferecem monitoramento, retry automático em caso de falha, alertas e histórico de execuções.

---

## 🧩 Relato de Desafios

### 1. Encoding do arquivo CSV

**Desafio:** ao tentar ler o arquivo com a codificação padrão UTF-8, o Pandas retornava erros imediatamente. Além disso, o nome da coluna `id` aparecia corrompido como `ï»¿id`, um sintoma clássico de BOM (Byte Order Mark) em arquivos `latin1`.

**Solução:** utilizei o parâmetro `encoding="latin1"` na leitura e adicionei um `rename` explícito para corrigir o nome da coluna. No futuro, o ideal seria padronizar a origem dos dados para UTF-8 desde a geração.

---

### 2. Linhas malformadas no CSV

**Desafio:** algumas linhas do dataset tinham número incorreto de campos, causando falhas na leitura.

**Solução:** o parâmetro `on_bad_lines="skip"` do Pandas permite ignorar essas linhas sem interromper o pipeline. Registrar quais linhas foram descartadas seria uma melhoria para produção.

---

### 3. Conversão de datas com múltiplos formatos

**Desafio:** a coluna `data_inversa` nem sempre seguia o mesmo padrão de formatação, gerando valores `NaT` após a conversão.

**Solução:** utilizei `errors="coerce"` para converter silenciosamente os valores inválidos em `NaT` e depois os removi com `dropna`. Isso evita que o pipeline quebre por dados inconsistentes.

---

### 4. Compatibilidade entre Python e SQL

**Desafio:** manter os dois scripts (Python e SQL) com as mesmas transformações e resultados equivalentes exigiu atenção redobrada, especialmente no tratamento de nulos e na conversão de datas, que têm sintaxes bem diferentes.

**Solução:** documentei cada etapa em ambas as linguagens, validando manualmente que os outputs eram equivalentes. Em produção, testes automatizados de qualidade de dados (ex: com a biblioteca `great_expectations`) resolveriam isso de forma mais escalável.

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Uso |
|---|---|
| Python 3 | Linguagem principal do pipeline |
| Pandas | Manipulação e transformação dos dados |
| MySQL | Pipeline alternativo via SQL |
| Logging | Rastreabilidade das etapas do pipeline |
| GitHub | Versionamento do código |

---

## 👤 Autor

**Nícolas Borges Natal**. Desenvolvido como parte do processo seletivo para **Estagiário em Engenharia de Dados - Cobli**.
