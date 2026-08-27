import pandas as pd
import geopandas as gpd
import numpy as np
import pyproj
import hashlib
from shapely.geometry import Point
from impala.dbapi import connect
from config import paths
from config import datas
from config import paths
from config import database

data_limite = f"{datas.ano_ref}-{datas.mes_atual}-01 00:00:00.000"

# Lê o Excel com o mapeamento para CTE 1
df_mapeamento = pd.read_excel(paths.grupo_local_imediato)
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
               WHERE oco.data_hora_fato >= '2015-01-01 00:00:00.000'
               AND oco.data_hora_fato < '2018-01-01 00:00:00.000'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.natureza_consumado = 'CONSUMADO'
               AND oco.natureza_codigo = 'C01155'
                '''
        
    df = database.executa_query_retorna_df(query, db='db_bisp_reds_reporting')

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Corrige a capitalização
df.columns = [col.title() for col in df.columns]  # "número reds" → "Número Reds"

# Exporta a base no computador local 
caminho_excel = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"XLSX - Uso interno/"
    f"Furto - Jan 2015 a Dez 2017.xlsx"
)
df.to_excel(caminho_excel, index=False)

# Exporta a base na nuvem 
caminho_nuvem = (
    f"{paths.onedrive_completas_interno_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Furto - Jan 2015 a Dez 2017.xlsx"
)
df.to_excel(caminho_nuvem, index=False)

# A
# T
# E
# N         A partir daqui, o código exporta as bases para csv
# Ç
# Ã
# O

# Cria cópia para o arquivo CSV 
df_csv = df.copy()

# Anonimização do n° reds
def anonimizar_chave(valor):
    if pd.isna(valor):
        return valor
    valor = str(valor).strip()
    hash_obj = hashlib.sha256(valor.encode("utf-8"))
    return hash_obj.hexdigest()[:16]

df_csv["Número Reds"] = df_csv["Número Reds"].apply(anonimizar_chave)

# Exclui colunas do xlsx para publicação em csv
colunas_remover = [
    "Descrição Subclasse Nat Principal",
    "Tentado/Consumado Nat Principal",
    "Natureza Nomenclatura Banco",
    "Unid Registro Nível 8",
    "Latitude",
    "Longitude",
    "Descrição Subgrupo Complemento Nat",
]

df_csv = df_csv.drop(columns=colunas_remover)

# Formatação CSV (modelo para abrir em excel e exclusão de "nan")
df_csv = df_csv.map(
    lambda x: str(x).replace(".", ",")
    if isinstance(x, float) and pd.notna(x)
    else x
)
df_csv = df_csv.fillna("")

# Exporta a base no computador em csv
caminho_csv = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"CSV -Uso externo/"
    f"Furto - Jan 2015 a Dez 2017.csv"
)
df_csv.to_csv(
    caminho_csv,
    sep=";",
    index=False,
    encoding="utf-8-sig",
)

# Exporta a base na nuvem 
caminho_csv_nuvem = (
    f"{paths.onedrive_completas_externo_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Furto - Jan 2015 a Dez 2017.csv"
)
df_csv.to_csv(
    caminho_csv_nuvem,
    sep=";",
    index=False,
    encoding="utf-8-sig",
)

print('Deu bom')