import pandas as pd
import itertools
from impala.dbapi import connect

# Funções auxiliares
def get_credentials(file_path):
    creds = {}
    with open(file_path, 'r') as f:
        for line in f:
            key, value = line.strip().split('=')
            creds[key] = value
    return creds

def get_conn_and_cursor(db='db_bisp_reds_reporting', credentials_file='C:/Users/x15501492/Downloads/Credenciamento Python.txt'):
    creds = get_credentials(credentials_file)
    conn = connect(
        host='10.100.62.20',
        port=21051,
        use_ssl=True,
        auth_mechanism="PLAIN",
        user=creds['username'],
        password=creds['password'],
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

# --- helpers ---
def norm_ibge(x):
    try:
        return str(int(float(x))).zfill(7)  # trata float do Excel (ex: 3106200.0) e padroniza 7 dígitos
    except Exception:
        return None

# 1. Lê a planilha
arquivo = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/08 - Ago/XLSX - Uso interno/Vítimas de Homicidio Consumado - Jan 2012 a Ago 2025.xlsx"
aba = "Vítimas"
df = pd.read_excel(arquivo, sheet_name=aba)

# 2. Consulta lista completa de municípios de MG no banco
query_municipios = """
    SELECT DISTINCT oco.nome_municipio as municipio,
                    oco.codigo_municipio as cod_ibge,
                    mun.risp_completa as risp,
                    mun.rmbh as rmbh
    FROM db_bisp_reds_reporting.tb_ocorrencia as oco
    LEFT JOIN db_bisp_shared.tb_populacao_risp mun
      ON oco.codigo_municipio = mun.codigo_ibge
    WHERE oco.ocorrencia_uf = 'MG'
"""
municipios = executa_query_retorna_df(query_municipios)

# 3. Normaliza chaves
municipios["Cod. IBGE"] = municipios["cod_ibge"].apply(norm_ibge)
municipios.rename(columns={"municipio": "Município", "risp": "RISP", "rmbh": "RMBH"}, inplace=True)

df["Cod. IBGE"] = df["Município - Código - FATO"].apply(norm_ibge)
df["Mês"] = pd.to_numeric(df["Mês Numérico Fato"], errors="coerce").astype("Int64")
df["Ano"] = pd.to_numeric(df["Ano Fato"], errors="coerce").astype("Int64")
df["Natureza"] = df["Natureza Nomenclatura Banco"].astype(str)

# 4. Listas únicas para o esqueleto
naturezas = sorted(df["Natureza"].dropna().unique().tolist())
periodos = df.loc[df["Mês"].notna() & df["Ano"].notna(), ["Ano", "Mês"]].drop_duplicates()
ibges = municipios["Cod. IBGE"].dropna().unique().tolist()

# 5. Esqueleto completo (produto cartesiano)
base = pd.DataFrame(
    itertools.product(naturezas, ibges, periodos.itertuples(index=False, name=None)),
    columns=["Natureza", "Cod. IBGE", "Periodo"]
)
base[["Ano", "Mês"]] = pd.DataFrame(base["Periodo"].tolist(), index=base.index)
base.drop(columns=["Periodo"], inplace=True)

# 6. Contagem a partir do Excel (só pelas chaves estáveis!)
contagem = (
    df.groupby(["Natureza", "Cod. IBGE", "Ano", "Mês"])["Número REDS"]
      .count()  # se quiser REDS distintos, use .nunique()
      .reset_index(name="Registros")
)

# 7. Junta o esqueleto com as contagens (APENAS pelas chaves estáveis)
res = base.merge(contagem, how="left", on=["Natureza", "Cod. IBGE", "Ano", "Mês"])
res["Registros"] = res["Registros"].fillna(0).astype(int)

# 8. Anexa Município/RISP/RMBH a partir do IBGE
res = res.merge(
    municipios[["Cod. IBGE", "Município", "RISP", "RMBH"]],
    how="left",
    on="Cod. IBGE"
)

# 9. Ordena colunas e linhas
res = res[["Registros", "Natureza", "Município", "Cod. IBGE", "Mês", "Ano", "RISP", "RMBH"]]
res = res.sort_values(["Ano", "Mês", "Natureza", "Município"]).reset_index(drop=True)

# 10. Exportar para Excel
saida = "C:/Users/x15501492/Documents/Sejusp/DIS/Dados abertos/Automatização/Agrupados/agrupado_vitimas_homicidio_consumado.xlsx" 
res.to_excel(saida, index=False)

print("Base agrupada gerada com sucesso em:", saida)
