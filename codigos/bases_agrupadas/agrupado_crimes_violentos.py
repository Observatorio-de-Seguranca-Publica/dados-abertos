import pandas as pd
import geopandas as gpd
import numpy as np
from impala.dbapi import connect

# Função para ler o arquivo de credenciais
def get_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

# Função para conectar ao banco de dados
def get_conn_and_cursor(db='db_bisp_reds_reporting', credentials_file='C:/Users/x15501492/Downloads/Credenciamento Python.txt'):
    credentials = get_credentials(credentials_file)
    conn = connect(host='10.100.62.20', port=21051, use_ssl=True, auth_mechanism="PLAIN",
                   user=credentials['username'], password=credentials['password'], database=db)
    cursor = conn.cursor()
    return conn, cursor

# Função para executar query e retornar dataframe
def executa_query_retorna_df(query, db='db_bisp_reds_reporting'):
    conn, cursor = get_conn_and_cursor(db)  
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    conn.close()
    return df

# Função para listar tabelas no banco de dados
def tabelas(filtro='', db='db_bisp_reds_reporting'):
    conn, cursor = get_conn_and_cursor(db)
    cursor.execute('SHOW TABLES')
    tabelas_nomes = cursor.fetchall()    
    conn.close()
    tabelas_filtradas = [tupla_tabela[0] for tupla_tabela in tabelas_nomes if filtro in tupla_tabela[0]]
    return tabelas_filtradas

# Função para listar bancos de dados
def bancos_de_dados():
    conn, cursor = get_conn_and_cursor()
    try:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        accessible_databases = []
        for db in databases:
            try:
                cursor.execute(f"USE {db[0]}")
                accessible_databases.append(db[0])
            except:
                pass
        return accessible_databases
    finally:
        cursor.close()
        conn.close()

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = '''         WITH 
                        municipios as (
                            SELECT DISTINCT oco.nome_municipio, oco.codigo_municipio
                            FROM db_bisp_reds_reporting.tb_ocorrencia as oco
                            WHERE oco.ocorrencia_uf = 'MG'
                        ),
                        periodos as (
                            SELECT DISTINCT YEAR(data_hora_fato) as ano_fato, MONTH(data_hora_fato) as mes_fato
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            WHERE oco.data_hora_fato >= '2019-01-01 00:00:00.000'
                            AND oco.data_hora_fato < '2025-12-01 00:00:00.000' 
                        ),
                        naturezas AS (
                            SELECT DISTINCT
                                oco.natureza_codigo,
                                CASE 
                                WHEN oco.natureza_codigo = 'D01213' THEN 'ESTUPRO'
                                WHEN oco.natureza_codigo = 'D01217' THEN 'ESTUPRO DE VULNERAVEL'
                                WHEN oco.natureza_codigo = 'C01158' THEN 'EXTORSAO'
                                WHEN oco.natureza_codigo = 'C01159' THEN 'EXTORSAO MEDIANTE SEQUESTRO'
                                WHEN oco.natureza_codigo = 'B01121' THEN 'HOMICIDIO'
                                WHEN oco.natureza_codigo = 'C01157' THEN 'ROUBO'
                                WHEN oco.natureza_codigo = 'B01148' THEN 'SEQUESTRO E CARCERE PRIVADO'
                                WHEN oco.natureza_codigo = 'B01504' THEN 'FEMINICIDIO'
                                ELSE oco.natureza_descricao
                                END as natureza_descricao,
                                CASE 
                                WHEN oco.natureza_codigo IN ('C01159') THEN 'CONSUMADO'         -- só consumado
                                WHEN oco.natureza_codigo IN ('B01121','B01504') THEN 'TENTADO'          -- só tentado (se for o caso)
                                ELSE NULL
                                END AS only_status
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            WHERE oco.natureza_codigo IN ('D01213','D01217','C01158','C01159','C01157','B01148','B01121','B01504')
                        ),
                        naturezas_exploded as (
                                SELECT natureza_codigo, natureza_descricao, 'CONSUMADO' AS status FROM naturezas WHERE (only_status IS NULL OR only_status='CONSUMADO')
                                UNION ALL
                                SELECT natureza_codigo, natureza_descricao, 'TENTADO'  AS status FROM naturezas WHERE (only_status IS NULL OR only_status='TENTADO')
                        ),
                        contagem AS (
                            SELECT COUNT(oco.numero_ocorrencia) as "Registros",
                                   oco.natureza_descricao,
                                   oco.natureza_codigo,
                                   CASE 
                                   WHEN oco.natureza_codigo = 'C01159' THEN 'CONSUMADO'
                                   WHEN oco.natureza_codigo IN ('B01121','B01504') THEN 'TENTADO'
                                   ELSE oco.natureza_consumado
                                   END AS status,
                                   YEAR (oco.data_hora_fato) as "ano_fato",
                                   MONTH (oco.data_hora_fato) as "mes_fato",
                                   oco.nome_municipio,
                                   oco.codigo_municipio,
                                   mun.risp_completa as "RISP",
                                   mun.rmbh as "RMBH"
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            LEFT JOIN db_bisp_shared.tb_populacao_risp as mun
                              ON oco.codigo_municipio = mun.codigo_ibge
                            LEFT JOIN db_bisp_shared.vw_dim_tempo as temp
                              ON oco.sqtempo_fato = temp.sqtempo
                            WHERE oco.data_hora_fato >= '2019-01-01 00:00:00.000'
                            AND oco.data_hora_fato < '2025-12-01 00:00:00.000'
                            AND oco.ocorrencia_uf = 'MG'
                            AND oco.ind_estado IN ('F', 'R')
                            AND (
                                (oco.natureza_codigo = 'D01213' AND oco.natureza_consumado IN ('CONSUMADO', 'TENTADO'))
                                OR (oco.natureza_codigo = 'D01217' AND oco.natureza_consumado IN ('CONSUMADO', 'TENTADO'))
                                OR (oco.natureza_codigo = 'C01158' AND oco.natureza_consumado IN ('CONSUMADO', 'TENTADO'))
                                OR (oco.natureza_codigo = 'C01159' AND oco.natureza_consumado = 'CONSUMADO')
                                OR (oco.natureza_codigo = 'C01157' AND oco.natureza_consumado IN ('CONSUMADO', 'TENTADO'))
                                OR (oco.natureza_codigo = 'B01148' AND oco.natureza_consumado IN ('CONSUMADO', 'TENTADO'))
                                OR (oco.natureza_codigo = 'B01121' AND oco.natureza_consumado = 'TENTADO')
                                OR (oco.natureza_codigo = 'B01504' AND oco.natureza_consumado = 'TENTADO')
                                )
                            GROUP BY oco.natureza_descricao,
                                     oco.natureza_codigo,
                                     CASE 
                                     WHEN oco.natureza_codigo = 'C01159' THEN 'CONSUMADO'
                                     WHEN oco.natureza_codigo IN ('B01121','B01504') THEN 'TENTADO'
                                     ELSE oco.natureza_consumado
                                     END,
                                     oco.nome_municipio,
                                     oco.codigo_municipio,
                                     MONTH (oco.data_hora_fato),
                                     YEAR (oco.data_hora_fato),
                                     mun.risp_completa,
                                     mun.rmbh
                        )
                        SELECT 
                            COALESCE(c.registros, 0) as "Registros",
                            ne.natureza_descricao || ' ' || nE.status as "Natureza",
                            m.nome_municipio as "Municipio",
                            m.codigo_municipio as "Cód. IBGE",
                            p.mes_fato as "Mês",
                            p.ano_fato as "Ano Fato",
                            pop.risp_completa as "RISP",
                            pop.rmbh as "RMBH"
                        FROM periodos p
                        CROSS JOIN municipios m
                        CROSS JOIN naturezas_exploded ne
                        LEFT JOIN contagem c
                          ON c.ano_fato = p.ano_fato
                          AND c.mes_fato = p.mes_fato
                          AND c.nome_municipio = m.nome_municipio
                          AND c.codigo_municipio = m.codigo_municipio
                          AND c.natureza_codigo = ne.natureza_codigo
                          AND c.status = ne.status
                        LEFT JOIN db_bisp_shared.tb_populacao_risp pop
                          ON m.codigo_municipio = pop.codigo_ibge
                '''
        
    df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')

# Ordenar pelas colunas "Ano Fato", "Mês", "Município" e "Natureza"
    df.rename(columns={
    "ano fato": "Ano Fato",
    "mês": "Mês",
    "municipio": "Município",
    "natureza": "Natureza",
    "cód. ibge": "Cód. IBGE",
    "risp": "RISP",
    "rmbh": "RMBH",
    "registros": "Registros"
    }, inplace=True)
    
# Ordenar pelas colunas "Ano Fato", "Mês", "Município" e "Natureza"
    df = df.sort_values(
    by=["Ano Fato", "Mês", "Natureza", "Município"],
    ascending=[True, True, True, True]
    ).reset_index(drop=True)

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Exporta a base no computador no modelo desejado 
df.to_excel("C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/11 - Novembro/Excel/agrupado_crimes_violentos.xlsx",index=False)

# A
# T
# E
# N         A partir daqui, o código exporta as bases para csv
# Ç
# Ã
# O

# Caminhos dos arquivos
base_excel = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/11 - Novembro/Excel/agrupado_crimes_violentos.xlsx"

# 1️⃣ Lê as bases
df_excel = pd.read_excel(base_excel)

# Caminho CSV
caminho_csv = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/11 - Novembro/Banco de Dados CSV/Banco Crimes Violentos 2019 a 2025 - Atualizado Novembro 2025.csv"

# Formatação regional
df_excel = df_excel.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_excel.to_csv(
    caminho_csv,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

print('FINALIZOU :)')