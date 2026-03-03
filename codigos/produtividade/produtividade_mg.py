import pandas as pd
from impala.dbapi import connect

# ==============================
# VARIÁVEIS
# ==============================

ANO_1 = 2025
ANO_2 = 2026
MES = 1


# ==============================
# FUNÇÕES DE CONEXÃO
# ==============================

def get_credentials(file_path):
    credentials = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials


def get_conn_and_cursor(db='db_bisp_reds_reporting',
                        credentials_file='C:/Users/x15501492/Downloads/Credenciamento Python.txt'):
    credentials = get_credentials(credentials_file)
    conn = connect(
        host='10.100.62.20',
        port=21051,
        use_ssl=True,
        auth_mechanism="PLAIN",
        user=credentials['username'],
        password=credentials['password'],
        database=db
    )
    cursor = conn.cursor()
    return conn, cursor


def executa_query_retorna_df(query, db='db_bisp_reds_reporting'):
    conn, cursor = get_conn_and_cursor(db)
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    conn.close()
    return df


# ==============================
# FUNÇÕES DE QUERY
# ==============================

def query_total_armas(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_arma_ocorrencia AS arm
            ON oco.numero_ocorrencia = arm.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND arm.tipo_arma_codigo NOT IN ('0300', '0100', '0200')
            AND arm.situacao_codigo IN ('0100', '0700')
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """

def query_registros_armas(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(DISTINCT oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_arma_ocorrencia AS arm
            ON oco.numero_ocorrencia = arm.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND arm.tipo_arma_codigo NOT IN ('0300', '0100', '0200')
            AND arm.situacao_codigo IN ('0100', '0700')
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """
    
def query_total_simulacros(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_material_apreendido_ocorrencia AS mat
            ON oco.numero_ocorrencia = mat.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND mat.situacao_codigo IN ('0100', '0600')
            AND mat.tipo_objeto_codigo = '2020'
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """

def query_registros_drogas(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(DISTINCT oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_material_apreendido_ocorrencia AS mat
            ON oco.numero_ocorrencia = mat.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND mat.situacao_codigo IN ('0100', '0600')
            AND mat.tipo_objeto_codigo IN ('5701', '5601', '5602', '5501', '5100', '5103', '5502', '5200', '5201', '5202', '5702', '5703', '5708', '5704', '5301', '5302', '5901', '5500', '5705', '5503', '5504', '5600', '5604', '5800', '5902', '5903', '5199', '5299', '5399', '5599', '5499', '5699', '5799', '5999', '5101', '5102', '5104', '5603', '5706', '5605', '5505', '5707')
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """
    
def query_total_conduzidos(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS env
            ON oco.numero_ocorrencia = env.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND env.tipo_prisao_apreensao_codigo IN ('0100', '0200', '0300', '9900', '0400')
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """
    
def query_total_veiculos(ano1, ano2, mes):
    return f"""
        SELECT 
            YEAR(oco.data_hora_fato) AS ano,
            COUNT(oco.numero_ocorrencia) AS total
        FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
        LEFT JOIN db_bisp_reds_reporting.tb_veiculo_ocorrencia AS vei
            ON oco.numero_ocorrencia = vei.numero_ocorrencia
        WHERE YEAR(oco.data_hora_fato) IN ({ano1}, {ano2})
            AND MONTH(oco.data_hora_fato) = {mes}
            AND oco.ocorrencia_uf = 'MG'
            AND vei.situacao_placa_codigo = '0400'
        GROUP BY YEAR(oco.data_hora_fato)
        ORDER BY ano
    """

# ==============================
# EXECUÇÃO DAS QUERIES
# ==============================

def executar_indicadores():

    queries = {
        "total_armas": query_total_armas(ANO_1, ANO_2, MES),
        "registros_armas": query_registros_armas(ANO_1, ANO_2, MES),
        "total_simulacros": query_total_simulacros(ANO_1, ANO_2, MES),
        "registros_drogas": query_registros_drogas(ANO_1, ANO_2, MES),
        "total_conduzidos": query_total_conduzidos(ANO_1, ANO_2, MES),
        "total_veiculos": query_total_veiculos(ANO_1, ANO_2, MES),
    }

    resultados = {}

    for nome_indicador, query in queries.items():

        try:
            df = executa_query_retorna_df(query)

            # Extração segura dos valores
            valor_ano1 = df.loc[df["ano"] == ANO_1, "total"].values
            valor_ano2 = df.loc[df["ano"] == ANO_2, "total"].values

            valor_ano1 = int(valor_ano1[0]) if len(valor_ano1) > 0 else 0
            valor_ano2 = int(valor_ano2[0]) if len(valor_ano2) > 0 else 0

            resultados[nome_indicador] = {
                ANO_1: valor_ano1,
                ANO_2: valor_ano2
            }

        except Exception as e:
            print(f"Erro no indicador {nome_indicador}: {e}")

    return resultados

# ==============================
# EXPORTA PARA EXCEL
# ==============================
    
def exporta_excel(resultados):

    linhas = []

    for indicador, valores in resultados.items():
        linhas.append([
            indicador,
            valores[2025],
            valores[2026]
        ])

    df_export = pd.DataFrame(
        linhas,
        columns=["Indicador", "2025", "2026"]
    )

    return df_export

# ==============================
# MAIN
# ==============================

if __name__ == "__main__":

    resultados = executar_indicadores()

    print("\nRESULTADOS MG:")
    for indicador, valores in resultados.items():
        print(indicador, "->", valores)

    df_export = exporta_excel(resultados)

    df_export.to_excel("C:/Users/x15501492/Documents/02 - Publicações/08 - Produtividade/2026/02 - Fevereiro/produtividade_mg.xlsx",index=False)

    print("\nArquivo Excel exportado com sucesso!")
    print("\nFINALIZOU :)")