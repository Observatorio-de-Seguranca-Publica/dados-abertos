import pandas as pd
from unidecode import unidecode
from config.datas import (
    ano_ref,
    mes_ref,
    mes_ref_num_str,
    mes_ref_nome,
    mes_ref_abrev,
    mes_atual
)

# Lista de arquivos de entrada: planilhas de Crimes Violentos e dicionário de regiões
base_excel = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"11 - Publicação SESP - Site/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"Excel/"
)

arquivos_cv = [
    base_excel + "19_24_agrupado_crimes_violentos.xlsx",
    base_excel + "25_26_agrupado_crimes_violentos.xlsx"
]

caminho_regioes = "config/municipios_mg.xlsx"

# Lista para armazenar os dataframes
lista_dfs_cv = []

for arquivo in arquivos_cv:
    df = pd.read_excel(arquivo)
    
    # Padroniza nome da coluna
    df.columns = df.columns.str.strip().str.lower()
    
    # Garante que "ano fato" é numérico
    df["ano fato"] = pd.to_numeric(df["ano fato"], errors='coerce')
    
    # Filtra a partir de 2024
    df_cv_filtrado = df[df["ano fato"] >= 2024]
    
    lista_dfs_cv.append(df_cv_filtrado)

# Empilha tudo
df_cv_final = pd.concat(lista_dfs_cv, ignore_index=True)

#Tratamento das colunas
df_cv_final.columns = [
    unidecode(col.strip().lower())
    for col in df_cv_final.columns
]

print(df_cv_final.columns)

# Cria a coluna nova de mês com o mapa
mapa_meses = {
    1: "JANEIRO",
    2: "FEVEREIRO",
    3: "MARÇO",
    4: "ABRIL",
    5: "MAIO",
    6: "JUNHO",
    7: "JULHO",
    8: "AGOSTO",
    9: "SETEMBRO",
    10: "OUTUBRO",
    11: "NOVEMBRO",
    12: "DEZEMBRO"
}
df_cv_final["mes_nome"] = df_cv_final["mes"].map(mapa_meses)

#mapa_natureza = {
#    "ESTUPRO CONSUMADO": "Estupro Consumado",
#    "ESTUPRO DE VULNERAVEL CONSUMADO": "Estupro de Vulnerável Consumado",
#    "ESTUPRO DE VULNERAVEL TENTADO": "Estupro de Vulnerável Tentado",
#    "ESTUPRO TENTADO": "Estupro Tentado",
#    "EXTORSAO CONSUMADO": "Extorsão Consumado",
#    "EXTORSAO MEDIANTE SEQUESTRO CONSUMADO": "Extorsão Mediante Sequestro Consumado",
#    "EXTORSAO TENTADO": "Extorsão Tentado",
#    "FURTO": "Furto Consumado",
#    "HOMICIDIO CONSUMADO (REGISTROS)": "Homicídio Consumado (Registros)",
#    "HOMICIDIO TENTADO": "Homicídio Tentado",
#    "LESAO CORPORAL": "Lesão Corporal Consumado",
#    "ROUBO CONSUMADO": "Roubo Consumado",
#    "ROUBO TENTADO": "Roubo Tentado",
#    "SEQUESTRO E CARCERE PRIVADO CONSUMADO": "Sequestro e Cárcere Privado Consumado",
#    "SEQUESTRO E CARCERE PRIVADO TENTADO": "Sequestro e Cárcere Privado Tentado",
#    "Homicídio Consumado (Vitimas)": "Vítima de Homicídio Consumado"
#}
#
## Cria a coluna nova de natureza com o mapa
#df_cv_final["Natureza Corrigida"] = df_cv_final["natureza"].map(mapa_natureza)

print(df_cv_final.columns)

# Exclusão Feminicídio
df_cv_final["natureza"] = df_cv_final["natureza"].str.strip().str.upper()

naturezas_excluir = [
    "FEMINICIDIO TENTADO",
    "FEMINICIDIO CONSUMADO (REGISTROS)"
]
df_cv_final = df_cv_final[~df_cv_final["natureza"].isin(naturezas_excluir)]

# Lê tabela de regiões
df_regioes = pd.read_excel(caminho_regioes)

# Padroniza nomes
df_regioes.columns = df_regioes.columns.str.strip().str.lower()

# Mantém apenas colunas necessárias
df_regioes = df_regioes[["cod. ibge", "regiao"]]

# Padroniza código ibge como str
df_regioes["cod. ibge"] = (
    df_regioes["cod. ibge"]
    .astype(str)
    .str.strip()
)
df_cv_final["cod. ibge"] = (
    df_cv_final["cod. ibge"]
    .astype(str)
    .str.strip()
)

# Cria a coluna região
df_cv_final = df_cv_final.merge(
    df_regioes,
    on="cod. ibge",
    how="left"
)

print(df_cv_final.columns.tolist())

colunas_finais = [
    "registros",
    "natureza",
    "municipio",
    "cod. ibge",
    "regiao",
    "mes",
    "ano fato",
    "risp",
    "rmbh",
    "mes_nome"
]

df_cv_final = df_cv_final[colunas_finais]

print(df_cv_final.columns)

# Ordena
df_cv_final = df_cv_final.sort_values(
    by=["ano fato", "mes", "natureza", "municipio"],
    ascending=[True, True, True, True]
).reset_index(drop=True)

# Caminho de saída
caminho_saida = (
    f"C:/Users/x15501492/OneDrive - CAMG/DIS -  Henrique/Paper/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"paper_automatico_{mes_ref_abrev}.xlsx"
)
            
# Exporta
with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
    df_cv_final.to_excel(writer, sheet_name='Dados CV', index=False)

print("Deu bom")