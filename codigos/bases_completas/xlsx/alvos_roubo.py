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

# Lê o Excel para o mapeamento para CTE 1
df_mapeamento = pd.read_excel("C:/Users/x15501492/Documents/Sejusp/DIS/Consultas/GRUPO LOCAL IMEDIATO COM CODIGO.xlsx")

# Lê o Excel para o mapeamento para CTE 2
df_alvo = pd.read_excel("C:/Users/x15501492/Documents/Sejusp/DIS/Consultas/alvo_corrigido.xlsx")

# Garante que todos os dados são strings e escapa apóstrofos
def esc(s):
    return str(s).replace("'", "''")

# Para mapeamento
linhas_mapeamento = []
for i, row in df_mapeamento.iterrows():
    cod_local = esc(row['Código Local Imediato'])
    desc_local = esc(row['Descrição Local Imediato'])
    cod_grupo = esc(row['Código Grupo Local Imediato'])
    desc_grupo = esc(row['Descrição Grupo Local Imediato'])

    prefixo = "SELECT" if i == 0 else "UNION ALL SELECT"
    linhas_mapeamento.append(f"{prefixo} '{cod_local}' AS codigo_local_imediato, "
                              f"'{desc_local}' AS descricao_local_imediato, "
                              f"'{cod_grupo}' AS codigo_grupo_local_imediato, "
                              f"'{desc_grupo}' AS descricao_grupo_local_imediato")
    
# Para alvo_corrigido
linhas_alvo = []
for i, row in df_alvo.iterrows():
    desc_subgrupo = esc(row['descricao_subgrupo_complemento_nat'])
    alvo_corrigido = esc(row['alvo'])

    prefixo = "SELECT" if i == 0 else "UNION ALL SELECT"
    linhas_alvo.append(f"{prefixo} '{desc_subgrupo}' AS \"descricao_subgrupo_complemento_nat\", "
                       f"'{alvo_corrigido}' AS \"Alvo\"")

cte_sql = "WITH mapeamento AS (\n  " + "\n  ".join(linhas_mapeamento) + "\n),\n"
cte_sql += "alvo_corrigido AS (\n  " + "\n  ".join(linhas_alvo) + "\n)\n"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = cte_sql + '''SELECT oco.numero_ocorrencia as "Número REDS",
                      oco.qtd_ocorrencia as "Qtde Ocorrências",
                      oco.natureza_descricao as "Descrição Subclasse Nat Principal",
                      oco.natureza_consumado as "Tentado/Consumado Nat Principal",
                      oco.natureza_descricao || ' ' || oco.natureza_consumado as "Natureza Principal Completa",
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
                      CASE
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 0 AND 1 THEN 'De 00:00 a 00:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 1 AND 2 THEN 'De 01:00 a 01:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 2 AND 3 THEN 'De 02:00 a 02:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 3 AND 4 THEN 'De 03:00 a 03:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 4 AND 5 THEN 'De 04:00 a 04:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 5 AND 6 THEN 'De 05:00 a 05:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 6 AND 7 THEN 'De 06:00 a 06:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 7 AND 8 THEN 'De 07:00 a 07:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 8 AND 9 THEN 'De 08:00 a 08:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 9 AND 10 THEN 'De 09:00 a 09:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 10 AND 11 THEN 'De 10:00 a 10:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 11 AND 12 THEN 'De 11:00 a 11:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 12 AND 13 THEN 'De 12:00 a 12:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 13 AND 14 THEN 'De 13:00 a 13:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 14 AND 15 THEN 'De 14:00 a 14:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 15 AND 16 THEN 'De 15:00 a 15:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 16 AND 17 THEN 'De 16:00 a 16:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 17 AND 18 THEN 'De 17:00 a 17:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 18 AND 19 THEN 'De 18:00 a 18:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 19 AND 20 THEN 'De 19:00 a 19:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 20 AND 21 THEN 'De 20:00 a 20:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 21 AND 22 THEN 'De 21:00 a 21:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 22 AND 23 THEN 'De 22:00 a 22:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 23 AND 24 THEN 'De 23:00 a 23:59'
                      END AS "Faixa 1 Hora Fato",
                      CASE
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 0 AND 5 THEN 'De 00:00 a 05:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 6 AND 11 THEN 'De 06:00 a 11:59'
                        WHEN EXTRACT(HOUR FROM oco.data_hora_fato) BETWEEN 12 AND 17 THEN 'De 12:00 a 17:59'
                        ELSE 'De 18:00 a 23:59'
                      END AS "Faixa 6 Horas Fato",
                      alv.alvo as "Alvo",
                      oco.motivo_presumido_descricao_longa as "Causa Presumida",
                      oco.instrumento_utilizado_descricao_longa as "Desc Longa Meio Utilizado",
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
               LEFT JOIN alvo_corrigido as alv
                    ON CAST(oco.complemento_natureza_descricao_longa AS STRING) = alv.descricao_subgrupo_complemento_nat
               WHERE oco.data_hora_fato >= '2015-01-01 00:00:00.000'
               AND oco.data_hora_fato < '2025-12-01 00:00:00.000'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.natureza_codigo IN ('C01157')
               AND oco.natureza_consumado = 'CONSUMADO'
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
df.to_excel("C:/Users/x15501492/Documents/02 - Publicações/Bases completas/XLSX - Uso interno/11 - Nov/Alvos - Roubo - Jan 2015 a Nov 2025.xlsx",index=False)

print('FINALIZOU :)')