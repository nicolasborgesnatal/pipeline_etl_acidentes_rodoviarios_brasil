# ------------------------------------------------------------------------------------
# Pipeline ETL de Acidentes Rodoviários no Brasil - Estagiário em Engenharia de Dados - Cobli
# ------------------------------------------------------------------------------------

# Importa a biblioteca pandas para manipulação e análise de dados
import pandas as pd

# Importa a biblioteca logging para registrar mensagens de execução do pipeline
import logging

# Importa Path para manipulação mais segura de caminhos de arquivos
from pathlib import Path


# Configurações do projeto

# Define o diretório onde o script está localizado
BASE_DIR = Path(__file__).resolve().parent

# Define o caminho do arquivo CSV com os dados brutos
ARQUIVO_ORIGEM = BASE_DIR / "acidentes_brasil.csv"

# Define a pasta onde será armazenada a camada Bronze (dados brutos)
PASTA_BRONZE = BASE_DIR / "camada_bronze"

# Define a pasta onde será armazenada a camada Prata (dados tratados)
PASTA_PRATA = BASE_DIR / "camada_prata"

# Define qual estado será utilizado para análise
ESTADO_ANALISE = "SP"

# Cria a pasta Bronze caso ela ainda não exista
PASTA_BRONZE.mkdir(exist_ok=True)

# Cria a pasta Prata caso ela ainda não exista
PASTA_PRATA.mkdir(exist_ok=True)


# Configuração de logs

# Configura o sistema de logs para registrar eventos do pipeline
logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(levelname)s - %(message)s"
)


# Ingestão dos dados

# Realiza a leitura do dataset de acidentes rodoviários e retorna um DataFrame com os dados brutos
def carregar_dados():

    # Registra no log o início da ingestão dos dados
    logging.info("Iniciando ingestão dos dados.")

    try:

        # Lê o arquivo CSV contendo os acidentes rodoviários
        df = pd.read_csv(
            ARQUIVO_ORIGEM,
            sep=";",
            encoding="latin1",
            on_bad_lines="skip",
            low_memory=False
        )

        # Registra no log que os dados foram carregados com sucesso
        logging.info("Dados carregados com sucesso.")

        # Retorna o DataFrame com os dados carregados
        return df

    except Exception as e:

        # Registra no log que ocorreu um erro durante a leitura do arquivo
        logging.error(f"Erro ao carregar dados: {e}")

        # Interrompe a execução do pipeline e repassa o erro
        raise


# Camada bronze

# Salva uma cópia exata dos dados brutos na camada Bronze
def salvar_bronze(df):

    # Define o caminho onde o arquivo Bronze será salvo
    bronze_path = PASTA_BRONZE / "acidentes_brasil_raw.csv"

    # Salva o DataFrame bruto no formato CSV
    df.to_csv(bronze_path, index=False)

    # Registra no log que a camada Bronze foi criada
    logging.info("Camada Bronze criada com sucesso.")


# Inspeção dos dados

# Exibe informações gerais sobre o dataset
def inspecionar_dados(df):

    # Registra no log o início da inspeção dos dados
    logging.info("Inspecionando estrutura dos dados.")

    # Exibe informações como tipos de dados, colunas e valores nulos
    df.info()


# Transformações

# Padroniza os nomes das colunas para facilitar análises futuras
def padronizar_colunas(df):

    # Remove espaços extras, converte para minúsculas e substitui espaços por underline
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Corrige possível erro de encoding na coluna id
    df.rename(columns={"ï»¿id": "id"}, inplace=True)

    # Registra no log que a padronização foi concluída
    logging.info("Colunas padronizadas.")

    # Retorna o DataFrame com as colunas padronizadas
    return df


# Converte a coluna de data para o formato datetime
def converter_datas(df):

    # Converte a coluna data_inversa para formato de data
    df["data_inversa"] = pd.to_datetime(
        df["data_inversa"],
        format="%d/%m/%Y",
        errors="coerce"
    )

    # Registra no log que a conversão foi realizada
    logging.info("Conversão de datas realizada.")

    # Retorna o DataFrame atualizado
    return df


# Substitui valores nulos em colunas categóricas
def tratar_nulos(df):

    # Seleciona todas as colunas do tipo texto
    colunas_texto = df.select_dtypes(include="object").columns

    # Preenche valores nulos com "Nao Informado"
    for col in colunas_texto:
        df[col] = df[col].fillna("Nao Informado")

    # Registra no log que o tratamento foi concluído
    logging.info("Tratamento de valores nulos concluído.")

    # Retorna o DataFrame tratado
    return df


# Remove registros que não possuem data válida
def remover_datas_invalidas(df):

    # Remove registros onde a coluna de data está vazia
    df = df.dropna(subset=["data_inversa"])

    # Registra no log quantos registros restaram após a limpeza
    logging.info(f"Total de registros após limpeza: {len(df)}")

    # Retorna o DataFrame filtrado
    return df


# Filtragem

# Filtra os dados para um estado específico
def filtrar_estado(df, estado):

    # Seleciona apenas registros do estado definido
    df_estado = df[df["uf"] == estado]

    # Registra no log quantos acidentes existem nesse estado
    logging.info(f"Total de acidentes em {estado}: {len(df_estado)}")

    # Retorna o DataFrame filtrado
    return df_estado


# Camada prata

# Salva os dados tratados na camada Prata
def salvar_prata(df):

    # Define o caminho onde o arquivo tratado será salvo
    prata_path = PASTA_PRATA / "acidentes_sp_tratado.csv"

    # Salva o DataFrame tratado
    df.to_csv(prata_path, index=False)

    # Registra no log a criação da camada Prata
    logging.info("Camada Prata criada com sucesso.")


# Consulta analítica

# Gera uma análise simples de acidentes por dia
def gerar_analise(df):

    # Agrupa os acidentes por data
    acidentes_por_dia = (
        df.groupby("data_inversa")
        .size()
        .reset_index(name="total_acidentes")
        .sort_values("data_inversa")
    )

    # Registra no log que a análise foi gerada
    logging.info("Consulta analítica gerada.")

    # Mostra as primeiras linhas da análise
    print(acidentes_por_dia.head())


# Pipeline principal

# Executa todas as etapas do pipeline ETL
def main():

    # Registra no log o início do pipeline
    logging.info("Iniciando pipeline ETL.")

    # Carrega os dados do arquivo CSV
    df = carregar_dados()

    # Salva os dados brutos na camada Bronze
    salvar_bronze(df)

    # Inspeciona a estrutura do dataset
    inspecionar_dados(df)

    # Padroniza os nomes das colunas
    df = padronizar_colunas(df)

    # Converte a coluna de data
    df = converter_datas(df)

    # Trata valores nulos
    df = tratar_nulos(df)

    # Remove registros com datas inválidas
    df = remover_datas_invalidas(df)

    # Filtra os dados para o estado definido
    df_estado = filtrar_estado(df, ESTADO_ANALISE)

    # Salva os dados tratados na camada Prata
    salvar_prata(df_estado)

    # Gera uma análise simples dos dados
    gerar_analise(df_estado)

    # Registra no log que o pipeline foi finalizado
    logging.info("Pipeline finalizado com sucesso.\n")


# Execução do script

# Executa o pipeline quando o script é rodado diretamente
if __name__ == "__main__":
    main()