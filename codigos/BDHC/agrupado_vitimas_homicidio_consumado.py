import pandas as pd
import itertools
from impala.dbapi import connect
import dateutil.parser
from config import datas
from config import paths
from config import database

# --- helpers ---
def norm_ibge(x):
    try:
        return str(int(float(x))).zfill(7)  # trata float do Excel (ex: 3106200.0) e padroniza 7 dígitos
    except Exception:
        return None

# 1. Lê a planilha
arquivo = (
    f"{paths.publicacoes_dir}/"
    f"BDHC_formatado_vitimas.xlsx"
)
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
municipios = database.executa_query_retorna_df(query_municipios)

# 3. Normaliza chaves
municipios["Cód. IBGE"] = municipios["cod_ibge"].apply(norm_ibge)
municipios.rename(columns={"municipio": "Município", "risp": "RISP", "rmbh": "RMBH"}, inplace=True)

df["Cód. IBGE"] = df["Município - Código"].apply(norm_ibge)
df["Mês"] = pd.to_numeric(df["Mês Numérico Fato"], errors="coerce").astype("Int64")
df["Ano"] = pd.to_numeric(df["Ano Fato"], errors="coerce").astype("Int64")
df["Natureza"] = df["Natureza Nomenclatura Banco"].astype(str)

# 4. Listas únicas para o esqueleto
naturezas = sorted(df["Natureza"].dropna().unique().tolist())
periodos = df.loc[df["Mês"].notna() & df["Ano"].notna(), ["Ano", "Mês"]].drop_duplicates()
ibges = municipios["Cód. IBGE"].dropna().unique().tolist()

# 5. Esqueleto completo (produto cartesiano)
base = pd.DataFrame(
    itertools.product(naturezas, ibges, periodos.itertuples(index=False, name=None)),
    columns=["Natureza", "Cód. IBGE", "Periodo"]
)
base[["Ano", "Mês"]] = pd.DataFrame(base["Periodo"].tolist(), index=base.index)
base.drop(columns=["Periodo"], inplace=True)

# 6. Contagem a partir do Excel (só pelas chaves estáveis!)
contagem = (
    df.groupby(["Natureza", "Cód. IBGE", "Ano", "Mês"])["Número REDS"]
      .count()  # se quiser REDS distintos, use .nunique()
      .reset_index(name="Registros")
)

# 7. Junta o esqueleto com as contagens (APENAS pelas chaves estáveis)
res = base.merge(contagem, how="left", on=["Natureza", "Cód. IBGE", "Ano", "Mês"])
res["Registros"] = res["Registros"].fillna(0).astype(int)

# 8. Anexa Município/RISP/RMBH a partir do IBGE
res = res.merge(
    municipios[["Cód. IBGE", "Município", "RISP", "RMBH"]],
    how="left",
    on="Cód. IBGE"
)

# 9. Renomeia e ordena colunas e linhas
res.rename(columns={"Ano": "Ano Fato"}, inplace=True)
res = res[["Registros", "Natureza", "Município", "Cód. IBGE", "Mês", "Ano Fato", "RISP", "RMBH"]]
res = res.sort_values(["Ano Fato", "Mês", "Natureza", "Município"]).reset_index(drop=True)

# 10. Transforma coluna código IBGE em número
res["Cód. IBGE"] = pd.to_numeric(res["Cód. IBGE"], errors="coerce").astype("Int64")

# 11. Exportar para Excel
saida = (
    f"{paths.agrupadas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Excel/"
    f"agrupado_vitimas_homicidio_consumado.xlsx"
)

res.to_excel(saida, index=False)

# A
# T
# E
# N         A partir daqui, o código exporta as bases para csv
# Ç
# Ã
# O

# Caminhos dos arquivos
base_excel = saida

# 1️⃣ Lê as bases
df_excel = pd.read_excel(base_excel)

# Caminho CSV
caminho_csv = (
    f"{paths.agrupadas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Banco de Dados CSV/"
    f"Banco Vítimas de Homicídio Consumado - Atualizado {datas.mes_ref_nome} {datas.ano_ref}.csv"
)

# Formatação regional
df_excel = df_excel.map(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_excel.to_csv(
    caminho_csv,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

print('Deu bom')
