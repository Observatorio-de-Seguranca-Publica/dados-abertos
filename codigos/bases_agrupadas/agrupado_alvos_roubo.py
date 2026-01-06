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

# Lê o Excel para o mapeamento para CTE
df_alvo = pd.read_excel("C:/Users/x15501492/Documents/Sejusp/DIS/Consultas/alvo_corrigido.xlsx")

# Garante que todos os dados são strings e escapa apóstrofos
def esc(s):
    return str(s).replace("'", "''")
    
# Para alvo_corrigido
linhas_alvo = []
for i, row in df_alvo.iterrows():
    desc_subgrupo = esc(row['descricao_subgrupo_complemento_nat'])
    alvo_corrigido = esc(row['alvo'])

    prefixo = "SELECT" if i == 0 else "UNION ALL SELECT"
    linhas_alvo.append(f"{prefixo} '{desc_subgrupo}' AS \"descricao_subgrupo_complemento_nat\", "
                       f"'{alvo_corrigido}' AS \"Alvo\"")

cte_sql = "WITH alvo_corrigido AS (\n  " + "\n  ".join(linhas_alvo) + "\n)\n"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = cte_sql + ''', 
                        municipios as (
                            SELECT DISTINCT oco.nome_municipio, oco.codigo_municipio
                            FROM db_bisp_reds_reporting.tb_ocorrencia as oco
                            WHERE oco.ocorrencia_uf = 'MG'
                        ),
                        periodos as (
                            SELECT DISTINCT YEAR(data_hora_fato) as ano_fato, MONTH(data_hora_fato) as mes_fato
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            WHERE oco.data_hora_fato >= '2015-01-01 00:00:00.000'
                            AND oco.data_hora_fato < '2026-01-01 00:00:00.000' 
                        ),
                        naturezas AS (
                            SELECT DISTINCT oco.natureza_descricao
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            WHERE oco.natureza_codigo = 'C01157'
                        ),
                        alvos AS (
                            SELECT DISTINCT alvo
                            FROM alvo_corrigido
                            WHERE alvo <> 'Outros'
                        ),
                        contagem AS (
                            SELECT COUNT(oco.numero_ocorrencia) as registros,
                                oco.natureza_descricao,
                                alv.alvo,
                                oco.nome_municipio,
                                oco.codigo_municipio,
                                MONTH (oco.data_hora_fato) as mes_fato,
                                YEAR (oco.data_hora_fato) as ano_fato,
                                mun.risp_completa,
                                mun.rmbh
                            FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
                            LEFT JOIN db_bisp_shared.tb_populacao_risp as mun
                              ON oco.codigo_municipio = mun.codigo_ibge
                            LEFT JOIN alvo_corrigido as alv
                              ON CAST(oco.complemento_natureza_descricao_longa AS STRING) = alv.descricao_subgrupo_complemento_nat
                            WHERE oco.data_hora_fato >= '2015-01-01 00:00:00.000'
                            AND oco.data_hora_fato < '2026-01-01 00:00:00.000'
                            AND oco.ocorrencia_uf = 'MG'
                            AND oco.ind_estado IN ('F', 'R')
                            AND oco.natureza_codigo = 'C01157'
                            AND oco.natureza_consumado = 'CONSUMADO'
                            GROUP BY oco.natureza_descricao,
                                     alv.alvo,
                                     oco.nome_municipio,
                                     oco.codigo_municipio,
                                     MONTH (oco.data_hora_fato),
                                     YEAR (oco.data_hora_fato),
                                     mun.risp_completa,
                                     mun.rmbh
                        )
                        SELECT 
                            COALESCE(c.registros, 0) as "Registros",
                            n.natureza_descricao as "Natureza",
                            a.alvo as "Alvos",
                            m.nome_municipio as "Municipio",
                            m.codigo_municipio as "Cód. IBGE",
                            p.mes_fato as "Mês",
                            p.ano_fato as "Ano Fato",
                            pop.risp_completa as "RISP",
                            pop.rmbh as "RMBH"
                        FROM periodos p
                        CROSS JOIN municipios m
                        CROSS JOIN naturezas n
                        CROSS JOIN alvos a
                        LEFT JOIN contagem c
                          ON c.ano_fato = p.ano_fato
                        AND c.mes_fato = p.mes_fato
                        AND c.nome_municipio = m.nome_municipio AND c.codigo_municipio = m.codigo_municipio
                        AND c.natureza_descricao = n.natureza_descricao
                        AND c.alvo = a.alvo
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
    "registros": "Registros",
    "alvos": "Alvos"
    }, inplace=True)
    
# Ordenar pelas colunas "Ano Fato", "Mês", "Município" e "Natureza"
    df = df.sort_values(
    by=["Ano Fato", "Alvos", "Mês", "Município"],
    ascending=[True, True, True, True]
    ).reset_index(drop=True)

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Exporta a base no computador no modelo desejado 
df.to_excel("C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/12 - Dezembro/Excel/agrupado_alvos_roubo.xlsx",index=False)

# A
# T
# E
# N         A partir daqui, o código exporta as bases para csv
# Ç
# Ã
# O

# Caminhos dos arquivos
base_excel = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/12 - Dezembro/Excel/agrupado_alvos_roubo.xlsx"

# 1️⃣ Lê as bases
df_excel = pd.read_excel(base_excel)

# Caminho CSV
caminho_csv = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2025/12 - Dezembro/Banco de Dados CSV/Banco Alvos de Roubo - Atualizado Dezembro 2025.csv"

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