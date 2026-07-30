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
from config.paths import base_dir, logs_dir, config_dir, codigos_dir, onedrive_dir, onedrive_publicacao_dir, onedrive_imagens_dir, onedrive_produtividade_dir, onedrive_quantitativo_dir, onedrive_completas_externo_dir, onedrive_completas_interno_dir, memorando_dir, publicacoes_dir, completas_dir, downloads_dir, produtividade_dir, grupo_local_imediato, alvo_corrigido, matriz_dir, matriz_dir
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
                      mat.situacao_descricao,
                      mat.tipo_objeto_descricao,
                      YEAR (oco.data_hora_fato) as ano_fato,
                      MONTH (oco.data_hora_fato) as mes_numerico_fato,
                      oco.nome_municipio,
                      oco.codigo_municipio,
                      oco.ocorrencia_uf
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               LEFT JOIN db_bisp_reds_reporting.tb_material_apreendido_ocorrencia AS mat
                    ON oco.numero_ocorrencia = mat.numero_ocorrencia
               WHERE oco.data_hora_fato >= '2025-01-01 00:00:00.000'
               AND oco.data_hora_fato < '{data_limite}'
               AND oco.ocorrencia_uf = 'MG'
               AND mat.situacao_codigo IN ('0100', '0600')
               AND mat.tipo_objeto_codigo IN ('5701', '5601', '5602', '5501', '5100', '5103', '5502', '5200', '5201', '5202', '5702', '5703', '5708', '5704', '5301', '5302', '5901', '5500', '5705', '5503', '5504', '5600', '5604', '5800', '5902', '5903', '5199', '5299', '5399', '5599', '5499', '5699', '5799', '5999', '5101', '5102', '5104', '5603', '5706', '5605', '5505', '5707')
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
    f"da_drogas_apreendidas.xlsx"
)

caminho_one_drive = (
    f"{onedrive_produtividade_dir}/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"da_drogas_apreendidas.xlsx"
)

df.to_excel(caminho_excel, index=False)
df.to_excel(caminho_one_drive, index=False)

print('FINALIZOU :)')