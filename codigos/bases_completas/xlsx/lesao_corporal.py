import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from impala.dbapi import connect
import pyproj
from config.datas import (
    ano_ref,
    mes_ref,
    mes_ref_num_str,
    mes_ref_nome,
    mes_ref_abrev,
    mes_atual
)
from config.paths import base_dir, logs_dir, temp_dir, input_dir, config_dir, output_dir, codigos_dir, onedrive_dir, memorando_dir, publicacoes_dir, completas_dir, downloads_dir, produtividade_dir, grupo_local_imediato, alvo_corrigido, matriz_dir

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

# Lê o Excel com o mapeamento para CTE 1
df_mapeamento = pd.read_excel(grupo_local_imediato)
df_mapeamento['Código Local Imediato'] = (
    df_mapeamento['Código Local Imediato']
    .astype(str)
    .str.zfill(4)
)

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

data_limite = f"{ano_ref}-{mes_atual}-01 00:00:00.000"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = cte_sql + f'''SELECT oco.numero_ocorrencia as "Número REDS",
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
                      geo.latitude_sirgas2000 as "Latitude",
                      geo.longitude_sirgas2000 as "Longitude",
                      oco.complemento_natureza_descricao_longa as "Descrição Subgrupo Complemento Nat"
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_shared.tb_populacao_risp as mun
                    ON oco.codigo_municipio = mun.codigo_ibge
               LEFT JOIN db_bisp_shared.vw_dim_tempo as temp
                    ON oco.sqtempo_fato = temp.sqtempo
               LEFT JOIN mapeamento
                    ON CAST(oco.local_imediato_codigo AS STRING) = mapeamento.codigo_local_imediato
               LEFT JOIN db_bisp_reds_master.tb_ocorrencia_setores_geodata as geo
                    ON oco.numero_ocorrencia = geo.numero_ocorrencia
               WHERE oco.data_hora_fato >= '2019-01-01 00:00:00.000'
               AND oco.data_hora_fato < '{data_limite}'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.natureza_consumado = 'CONSUMADO'
               AND oco.natureza_codigo IN ('B01129')
                '''
        
    df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Corrige a capitalização
df.columns = [col.title() for col in df.columns]  # "número reds" → "Número Reds"

# Exporta a base no computador no modelo desejado 
caminho_excel = (
    f"{completas_dir}/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_abrev}/"
    f"XLSX - Uso interno/"
    f"Lesão Corporal - Jan 2019 a {mes_ref_abrev} {ano_ref}.xlsx"
)

df.to_excel(caminho_excel, index=False)

print('FINALIZOU :)')