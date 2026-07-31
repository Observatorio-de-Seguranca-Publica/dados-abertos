import pandas as pd
import geopandas as gpd
import numpy as np
from impala.dbapi import connect
from config import datas
from config import paths
from config import database

data_limite = f"{datas.ano_ref}-{datas.mes_atual}-01 00:00:00.000"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = f'''SELECT oco.numero_ocorrencia,
                      YEAR (oco.data_hora_fato) as ano_fato,
                      MONTH (oco.data_hora_fato) as mes_numerico_fato,
                      oco.nome_municipio,
                      oco.codigo_municipio,
                      oco.ocorrencia_uf,
                      arm.tipo_arma_descricao,
                      arm.situacao_descricao
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_reds_reporting.tb_arma_ocorrencia AS arm
                    ON oco.numero_ocorrencia = arm.numero_ocorrencia
               WHERE oco.data_hora_fato >= '2025-01-01 00:00:00.000'
               AND oco.data_hora_fato < '{data_limite}'
               AND oco.ocorrencia_uf = 'MG'
               AND arm.tipo_arma_codigo NOT IN ('0300', '0100', '0200')
               AND arm.situacao_codigo IN ('0100', '0700')
                '''
        
    df = database.executa_query_retorna_df(query, db='db_bisp_reds_reporting')

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Exporta a base no computador no modelo desejado
caminho_excel = (
    f"{paths.produtividade_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"da_armas_apreendidas.xlsx"
)

caminho_one_drive = (
    f"{paths.onedrive_produtividade_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"da_armas_apreendidas.xlsx"
)

df.to_excel(caminho_excel, index=False)
df.to_excel(caminho_one_drive, index=False)

print('FINALIZOU :)')