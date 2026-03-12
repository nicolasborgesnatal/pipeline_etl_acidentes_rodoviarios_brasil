-- Pipeline ETL de Acidentes Rodoviários no Brasil - Estagiário em Engenharia de Dados - Cobli


-- 1. Criação do banco de dados

CREATE DATABASE IF NOT EXISTS pipeline_etl_acidentes;
USE pipeline_etl_acidentes;


-- 2. Criação da tabela inicial que receberá o CSV
-- Camada de ingestão

DROP TABLE IF EXISTS acidentes_brasil;

CREATE TABLE acidentes_brasil (
    id INT,
    data_inversa VARCHAR(20),
    dia_semana VARCHAR(20),
    horario VARCHAR(10),
    uf VARCHAR(2),
    br INT,
    km VARCHAR(10),
    municipio VARCHAR(100),
    causa_acidente VARCHAR(255),
    tipo_acidente VARCHAR(100),
    classificacao_acidente VARCHAR(100),
    fase_dia VARCHAR(50),
    sentido_via VARCHAR(50),
    condicao_metereologica VARCHAR(100),
    tipo_pista VARCHAR(50),
    tracado_via VARCHAR(50),
    uso_solo VARCHAR(50),
    pessoas INT,
    mortos INT,
    feridos_leves INT,
    feridos_graves INT,
    ilesos INT,
    ignorados INT,
    feridos INT,
    veiculos INT,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    regional VARCHAR(50),
    delegacia VARCHAR(50),
    uop VARCHAR(50)
);


-- 3. Importação do CSV para a tabela inicial

LOAD DATA LOCAL INFILE 'C:/Users/SeuUsuario/Downloads/acidentes_brasil.csv'
INTO TABLE acidentes_brasil
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- 4. Criação da camada Bronze
-- Cópia exata dos dados brutos

DROP TABLE IF EXISTS acidentes_brasil_bronze;

CREATE TABLE acidentes_brasil_bronze AS
SELECT *
FROM acidentes_brasil;


-- 5. Inspeção da estrutura dos dados

DESCRIBE acidentes_brasil_bronze;

SELECT COUNT(*) AS total_registros
FROM acidentes_brasil_bronze;


-- 6. Conversão da coluna de data

UPDATE acidentes_brasil_bronze
SET data_inversa = STR_TO_DATE(data_inversa, '%d/%m/%Y');

-- Alteração do tipo da coluna para DATE
ALTER TABLE acidentes_brasil_bronze
MODIFY COLUMN data_inversa DATE;


-- 7. Tratamento de valores nulos

UPDATE acidentes_brasil_bronze
SET municipio = COALESCE(municipio, 'Nao Informado'),
    causa_acidente = COALESCE(causa_acidente, 'Nao Informado'),
    tipo_acidente = COALESCE(tipo_acidente, 'Nao Informado'),
    classificacao_acidente = COALESCE(classificacao_acidente, 'Nao Informado'),
    fase_dia = COALESCE(fase_dia, 'Nao Informado'),
    sentido_via = COALESCE(sentido_via, 'Nao Informado'),
    condicao_metereologica = COALESCE(condicao_metereologica, 'Nao Informado'),
    tipo_pista = COALESCE(tipo_pista, 'Nao Informado'),
    tracado_via = COALESCE(tracado_via, 'Nao Informado'),
    uso_solo = COALESCE(uso_solo, 'Nao Informado');


-- 8. Remoção de registros com datas inválidas

DELETE FROM acidentes_brasil_bronze
WHERE data_inversa IS NULL;


-- 9. Criação da camada Prata
-- Filtragem para o estado de São Paulo

DROP TABLE IF EXISTS acidentes_sp_prata;

CREATE TABLE acidentes_sp_prata AS
SELECT *
FROM acidentes_brasil_bronze
WHERE uf = 'SP';


-- 10. Criação de índice para melhorar performance

CREATE INDEX idx_data_acidente
ON acidentes_sp_prata(data_inversa);


-- 11. Consulta analítica
-- Total de acidentes por dia

SELECT
    data_inversa,
    COUNT(*) AS total_acidentes
FROM acidentes_sp_prata
GROUP BY data_inversa
ORDER BY data_inversa;