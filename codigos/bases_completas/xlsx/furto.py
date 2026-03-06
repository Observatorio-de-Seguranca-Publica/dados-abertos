import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from impala.dbapi import connect
import pyproj

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

# Lê o Excel com o mapeamento
df_mapeamento = pd.read_excel("C:/Users/x15501492/OneDrive - CAMG/DIS -  Henrique/Consultas/GRUPO LOCAL IMEDIATO COM CODIGO.xlsx")

# Garante que todos os dados são strings e escapa apóstrofos
def esc(s):
    return str(s).replace("'", "''")

# Gera a CTE com os dados da planilha
linhas = []
for i, row in df_mapeamento.iterrows():
    cod_local = esc(row['Código Local Imediato'])
    desc_local = esc(row['Descrição Local Imediato'])
    cod_grupo = esc(row['Código Grupo Local Imediato'])
    desc_grupo = esc(row['Descrição Grupo Local Imediato'])

    prefixo = "SELECT" if i == 0 else "UNION ALL SELECT"
    linhas.append(f"{prefixo} '{cod_local}' AS codigo_local_imediato, "
                  f"'{desc_local}' AS descricao_local_imediato, "
                  f"'{cod_grupo}' AS codigo_grupo_local_imediato, "
                  f"'{desc_grupo}' AS descricao_grupo_local_imediato")

cte_sql = "WITH mapeamento AS (\n  " + "\n  ".join(linhas) + "\n)\n"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = cte_sql + '''SELECT oco.numero_ocorrencia as "Número REDS",
                      oco.qtd_ocorrencia as "Qtde Ocorrências",
                      oco.natureza_descricao as "Descrição Subclasse Nat Principal",
                      oco.natureza_consumado as "Tentado/Consumado Nat Principal",
                      oco.natureza_descricao || ' ' || oco.natureza_consumado as "Natureza Principal Completa",
                      CONCAT(
                          UPPER(SUBSTR(oco.natureza_descricao, 1, 1)),
                          LOWER(SUBSTR(oco.natureza_descricao, 2)),
                          ' ',
                          UPPER(SUBSTR(oco.natureza_consumado, 1, 1)),
                          LOWER(SUBSTR(oco.natureza_consumado, 2))
                          ) 
                      as "Natureza Nomenclatura Banco",
                      YEAR (oco.data_hora_fato) as "Ano Fato",
                      CASE MONTH (oco.data_hora_fato)
                        WHEN 1 THEN 'JAN'
                        WHEN 2 THEN 'FEV'
                        WHEN 3 THEN 'MAR'
                        WHEN 4 THEN 'ABR'
                        WHEN 5 THEN 'MAI'
                        WHEN 6 THEN 'JUN'
                        WHEN 7 THEN 'JUL'
                        WHEN 8 THEN 'AGO'
                        WHEN 9 THEN 'SET'
                        WHEN 10 THEN 'OUT'
                        WHEN 11 THEN 'NOV'
                        WHEN 12 THEN 'DEZ'
                        ELSE 'Mês Inválido'
                      END AS "Mês Fato Resumido",
                      MONTH (oco.data_hora_fato) as "Mês Numérico Fato",
                      CAST (oco.data_hora_fato as date) as "Data Fato",
                      temp.cddia_semana as "Dia da Semana Fato",
                      SUBSTRING(CAST(oco.data_hora_fato AS STRING), 12, 8) as "Horário Fato",
                      temp.nmfaixa_horaria1 as "Faixa 1 Hora Fato",
                      temp.nmfaixa_horaria2 as "Faixa 6 Horas Fato",
                      oco.motivo_presumido_descricao_longa as "Causa Presumida",
                      oco.instrumento_utilizado_descricao_longa as "Descrição Meio Utilizado",
                      mapeamento.descricao_grupo_local_imediato AS "Descrição Grupo Local Imediato",
                      oco.local_imediato_longa as "Descrição Local Imediato",
                      oco.tipo_logradouro_descricao as "Logradouro Ocorrência - Tipo",
                      oco.nome_bairro as "Bairro - Fato Final",
                      oco.nome_bairro || ', ' || oco.nome_municipio as "Bairro - Fato Final - Municipio",
                      oco.nome_municipio as "Município",
                      oco.codigo_municipio as "Município - Código",
                      oco.ocorrencia_uf as "UF - Sigla",
                      oco.unidade_responsavel_registro_nome as "Unid Registro Nível 8",
                      mun.risp_completa as "RISP",
                      mun.rmbh as "RMBH",
                      oco.numero_latitude as "Latitude",
                      oco.numero_longitude as "Longitude",
                      oco.complemento_natureza_descricao_longa as "Descrição Subgrupo Complemento Nat"
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_shared.tb_populacao_risp as mun
                    ON oco.codigo_municipio = mun.codigo_ibge
               LEFT JOIN db_bisp_shared.vw_dim_tempo as temp
                    ON oco.sqtempo_fato = temp.sqtempo
               LEFT JOIN mapeamento
                    ON CAST(oco.local_imediato_codigo AS STRING) = mapeamento.codigo_local_imediato
               WHERE oco.data_hora_fato >= '2022-01-01 00:00:00.000'
               AND oco.data_hora_fato < '2026-03-01 00:00:00.000'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.natureza_consumado = 'CONSUMADO'
               AND oco.natureza_codigo = 'C01155'
                '''
        
    df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')

# Corrige a capitalização
df.columns = [col.title() for col in df.columns]  # "número reds" → "Número Reds"

url = "C:/Users/x15501492/Downloads/SAD69_1.GSB"

# Define o pipeline de transformação
transformer = pyproj.Transformer.from_pipeline(
    f"+proj=pipeline +step +proj=axisswap +order=2,1 "
    "+step +proj=unitconvert +xy_in=deg +xy_out=rad "
    f"+step +proj=hgridshift +grids={url} "
    "+step +proj=unitconvert +xy_in=rad +xy_out=deg "
    "+step +proj=axisswap +order=2,1"
)
 
# Função para transformar coordenadas
def transformar_coordenadas(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return pd.Series(["", ""], index=['Latitude SIRGAS', 'Longitude SIRGAS'])
    lat_sirgas, lon_sirgas = transformer.transform(lat, lon)
    return pd.Series([lat_sirgas, lon_sirgas], index=['Latitude SIRGAS', 'Longitude SIRGAS'])
 
# Criar novas colunas com as coordenadas transformadas no DataFrame retornado pela consulta SQL
df[['Latitude SIRGAS', 'Longitude SIRGAS']] = df.apply(
    lambda row: transformar_coordenadas(row['Latitude'], row['Longitude']),
    axis=1
)
 
# Convertendo a coluna 'Valor' para string e substituindo ponto por vírgula
df['Latitude SIRGAS'] = df['Latitude SIRGAS'].astype(str).str.replace("inf", '', regex=False)
df['Longitude SIRGAS'] = df['Longitude SIRGAS'].astype(str).str.replace("inf", '', regex=False)
df['Latitude SIRGAS'] = pd.to_numeric(df['Latitude SIRGAS'])
df['Longitude SIRGAS'] = pd.to_numeric(df['Longitude SIRGAS'])
 
# fim da dtransformação lat long

# Exporta a base no computador no modelo desejado 
df.to_excel("C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/02 - Fev/XLSX - Uso interno/Furto - Jan 2022 a Fev 2026.xlsx",index=False)

print('FINALIZOU :)')