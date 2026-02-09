import pandas as pd
import itertools
from impala.dbapi import connect

# Funções auxiliares ....
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
        return str(int(float(x)))  # trata float do Excel (ex: 3106200.0) e padroniza 7 dígitos
    except Exception:
        return None

# 1. Lê a planilha
arquivo = "C:/Users/x15501492/Downloads/BDHC_formatado_registros.xlsx"
aba = "Sheet1"
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
municipios["Cód. IBGE"] = municipios["cod_ibge"].apply(norm_ibge)
municipios.rename(columns={"municipio": "Município", "risp": "RISP", "rmbh": "RMBH"}, inplace=True)

df["Cód. IBGE"] = df["Município - Código"].apply(norm_ibge)
df["Mês"] = pd.to_numeric(df["Mês Numérico Fato"], errors="coerce").astype("Int64")
df["Ano Fato"] = pd.to_numeric(df["Ano Fato"], errors="coerce").astype("Int64")
df["Natureza"] = df["Natureza Principal Completa"].astype(str).str.upper()
df["Natureza"] = df["Natureza"].astype(str) + " (REGISTROS)"



# 4. Listas únicas para o esqueleto
naturezas = sorted(df["Natureza"].dropna().unique().tolist())
periodos = df.loc[df["Mês"].notna() & df["Ano Fato"].notna(), ["Ano Fato", "Mês"]].drop_duplicates()
ibges = municipios["Cód. IBGE"].dropna().unique().tolist()

# 5. Esqueleto completo (produto cartesiAno Fato)
#base = pd.DataFrame(
#    itertools.product(naturezas, ibges, periodos.itertuples(index=False, name=None)),
#    columns=["Natureza", "Cód. IBGE", "Periodo"]
#)
#base[["Ano Fato", "Mês"]] = pd.DataFrame(base["Periodo"].tolist(), index=base.index)
#base.drop(columns=["Periodo"], inplace=True)

bases = []

for nat in naturezas:
    # Se for feminicídio, só inclui períodos a partir de 2025
    if "FEMINICIDIO" in nat:
        periodos_nat = periodos[periodos["Ano Fato"] >= 2025]
    else:
        periodos_nat = periodos.copy()

    base_nat = pd.DataFrame(
        itertools.product([nat], ibges, periodos_nat.itertuples(index=False, name=None)),
        columns=["Natureza", "Cód. IBGE", "Periodo"]
    )

    bases.append(base_nat)

# Junta todas as bases parciais
base = pd.concat(bases, ignore_index=True)

# Divide o período em Ano Fato e Mês
base[["Ano Fato", "Mês"]] = pd.DataFrame(base["Periodo"].tolist(), index=base.index)
base.drop(columns=["Periodo"], inplace=True)



# 6. Contagem a partir do Excel (só pelas chaves estáveis!)
contagem = (
    df.groupby(["Natureza", "Cód. IBGE", "Ano Fato", "Mês"])["Número Reds"]
      .count()  # se quiser REDS distintos, use .nunique()
      .reset_index(name="Registros")
)


# 7. Junta o esqueleto com as contagens (APENAS pelas chaves estáveis)
res = base.merge(contagem, how="left", on=["Natureza", "Cód. IBGE", "Ano Fato", "Mês"])
res["Registros"] = res["Registros"].fillna(0).astype(int)

# 8. Anexa Município/RISP/RMBH a partir do IBGE
res = res.merge(
    municipios[["Cód. IBGE", "Município", "RISP", "RMBH"]],
    how="left",
    on="Cód. IBGE"
)

# 9. Ordena colunas e linhas
res = res[["Registros", "Natureza", "Município", "Cód. IBGE", "Mês", "Ano Fato", "RISP", "RMBH"]]
res = res.sort_values(["Ano Fato", "Mês", "Natureza", "Município"]).reset_index(drop=True)

# 10. Exportar para Excel
saida = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/agrupado_registros_homicidio_consumado.xlsx" 
res.to_excel(saida, index=False)

print("Base agrupada gerada com sucesso em:", saida)

# A
# T
# E
# N         A partir daqui, o código junta os registros de homicídio consumado com a base de crimes violentos
# Ç
# Ã
# O

# Caminhos dos arquivos
agg_hc = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/agrupado_registros_homicidio_consumado.xlsx" 
agg_cv_12_18 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/12_18_agrupado_crimes_violentos.xlsx" 
agg_cv_19_24 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/19_24_agrupado_crimes_violentos.xlsx" 
agg_cv_25_26 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/25_26_agrupado_crimes_violentos.xlsx" 


# 1️⃣ Lê as bases
df_hc = pd.read_excel(agg_hc)
df_cv_12_18 = pd.read_excel(agg_cv_12_18)
df_cv_19_24 = pd.read_excel(agg_cv_19_24)
df_cv_25_26 = pd.read_excel(agg_cv_25_26)

# --- PADRONIZA TIPOS DAS TRÊS BASES ---

def norm_ibge(x):
    try:
        return str(int(float(x)))
    except:
        return None

# Cod IBGE como texto padronizado
for dfX in [df_hc, df_cv_12_18, df_cv_19_24, df_cv_25_26]:
    if "Cód. IBGE" in dfX.columns:
        dfX["Cód. IBGE"] = dfX["Cód. IBGE"].apply(norm_ibge)

# Ano Fato e Mês como inteiro (se existirem nas bases)
for dfX in [df_hc, df_cv_12_18, df_cv_19_24, df_cv_25_26]:
    if "Ano Fato" in dfX.columns:
        dfX["Ano Fato"] = pd.to_numeric(dfX["Ano Fato"], errors="coerce").astype("Int64")
    if "Mês" in dfX.columns:
        dfX["Mês"] = pd.to_numeric(dfX["Mês"], errors="coerce").astype("Int64")


# 2️⃣ Filtra a BDHC apenas entre 2012 e 2018
df_vhc_filtrada_12_18 = df_hc[
    (df_hc["Ano Fato"] >= 2012) & (df_hc["Ano Fato"] <= 2018)
].copy()

# 3️⃣ Garante que as colunas estão iguais (ordem e nomes)
df_vhc_filtrada_12_18 = df_vhc_filtrada_12_18.reindex(columns=df_cv_12_18.columns)

# 4️⃣ Junta (empilha)
df_final_12_18 = pd.concat([df_cv_12_18, df_vhc_filtrada_12_18], ignore_index=True)

print(f"Base CV 2012–2018 original: {len(df_cv_12_18)}")
print(f"Base BDHC 2012–2018 filtrada: {len(df_vhc_filtrada_12_18)}")
print(f"→ Base unificada 2012–2018: {len(df_final_12_18)}")

saida_12_18 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/12_18_agrupado_crimes_violentos.xlsx"
df_final_12_18.to_excel(saida_12_18, index=False)
print(f"✅ Base unificadas salva em:\n{saida_12_18}")

# 5️⃣ Filtra a BDHC apenas entre 2019 e 2024
df_vhc_filtrada_19_24 = df_hc[
    (df_hc["Ano Fato"] >= 2019) & (df_hc["Ano Fato"] <= 2024)
].copy()

# 6️⃣ Garante que as colunas estão iguais (ordem e nomes)
df_vhc_filtrada_19_24 = df_vhc_filtrada_19_24.reindex(columns=df_cv_19_24.columns)

# 4️⃣ Junta (empilha)
df_final_19_24 = pd.concat([df_cv_19_24, df_vhc_filtrada_19_24], ignore_index=True)

print(f"Base CV 2019–2024 original: {len(df_cv_19_24)}")
print(f"Base BDHC 2019–2024 filtrada: {len(df_vhc_filtrada_19_24)}")
print(f"→ Base unificada 2019–2024: {len(df_final_19_24)} registros")

saida_19_24 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/19_24_agrupado_crimes_violentos.xlsx"
df_final_19_24.to_excel(saida_19_24, index=False)
print(f"✅ Base unificadas salva em:\n{saida_19_24}")

# Filtra a BDHC apenas entre 2025 e 2026
df_vhc_filtrada_25_26 = df_hc[
    (df_hc["Ano Fato"] >= 2024) & (df_hc["Ano Fato"] <= 2026)
].copy()

# Garante que as colunas estão iguais (ordem e nomes)
df_vhc_filtrada_25_26 = df_vhc_filtrada_25_26.reindex(columns=df_cv_25_26.columns)

# Junta (empilha)
df_final_25_26 = pd.concat([df_cv_25_26, df_vhc_filtrada_25_26], ignore_index=True)

print(f"Base CV 2025–2026 original: {len(df_cv_25_26)}")
print(f"Base BDHC 2025–2026 filtrada: {len(df_vhc_filtrada_25_26)}")
print(f"→ Base unificada 2025–2026: {len(df_final_25_26)} registros")

saida_25_26 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Excel/25_26_agrupado_crimes_violentos.xlsx"
df_final_25_26.to_excel(saida_25_26, index=False)
print(f"✅ Base unificadas salva em:\n{saida_25_26}")


# A
# T
# E
# N         A partir daqui, o código exporta as bases para csv
# Ç
# Ã
# O

caminho_csv_12_18 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Banco de Dados CSV/Banco Crimes Violentos 2012 a 2018 - Atualizado Janeiro 2026.csv" 
caminho_csv_19_24 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Banco de Dados CSV/Banco Crimes Violentos 2019 a 2024 - Atualizado Janeiro 2026.csv"
caminho_csv_25_26 = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/01 - Janeiro/Banco de Dados CSV/Banco Crimes Violentos 2025 a 2026 - Atualizado Janeiro 2026.csv"

# Função auxiliar para exportar com formatação BR
def exporta_csv(df, caminho):
    df_fmt = df.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)
    df_fmt.to_csv(
        caminho,
        sep=';', 
        index=False,
        encoding='utf-8-sig'
    )

# Exporta todas as bases
exporta_csv(df_final_12_18, caminho_csv_12_18)
exporta_csv(df_final_19_24, caminho_csv_19_24)
exporta_csv(df_final_25_26, caminho_csv_25_26)

print("Arquivos CSV exportados com sucesso!")