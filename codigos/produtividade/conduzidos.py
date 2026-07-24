import pandas as pd
import geopandas as gpd
import numpy as np
from impala.dbapi import connect
from config.datas import (
    ano_ref,
    mes_ref,
    mes_ref_num_str,
    mes_ref_nome,
    mes_ref_abrev,
    mes_atual
)
from config.paths import base_dir, logs_dir, temp_dir, input_dir, config_dir, output_dir, codigos_dir, onedrive_dir, memorando_dir, publicacoes_dir, completas_dir, downloads_dir, produtividade_dir, grupo_local_imediato, alvo_corrigido, matriz_dir
from config.database import (
    get_conn_and_cursor,
    executa_query_retorna_df,
    tabelas,
    bancos_de_dados,
)

data_limite = f"{ano_ref}-{mes_atual}-01 00:00:00.000"

# Consulta ao banco (script do dbeaver: no exemplo abaixo há um join entre a tabela de ocorrências e envolvidos)
try:
    query = f'''SELECT oco.numero_ocorrencia,
                      YEAR (oco.data_hora_fato) as ano_fato,
                      MONTH (oco.data_hora_fato) as mes_numerico_fato,
                      env.tipo_prisao_apreensao_descricao,
                      oco.nome_municipio,
                      oco.codigo_municipio,
                      oco.ocorrencia_uf
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS env
                    ON oco.numero_ocorrencia = env.numero_ocorrencia
               WHERE oco.data_hora_fato >= '2025-01-01 00:00:00.000'
               AND oco.data_hora_fato < '{data_limite}'
               AND oco.ocorrencia_uf = 'MG'
               AND env.tipo_prisao_apreensao_codigo IN ('0100', '0200', '0300', '9900', '0400')   
                '''
        
    df = executa_query_retorna_df(query, db='db_bisp_reds_reporting')

except Exception as e:
    print(f"Erro ao consultar a tabela 'tb_ocorrencia': {e}")

# Exibe as primeiras linhas do DataFrame
df.head()

# Exporta a base no computador no modelo desejado
caminho_excel = (
    f"{produtividade_dir}/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"da_conduzidos.xlsx"
)

df.to_excel(caminho_excel, index=False)

print('FINALIZOU :)')
