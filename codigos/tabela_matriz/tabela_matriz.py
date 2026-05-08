import pandas as pd
from config.datas import (
    ano_ref,
    mes_ref,
    mes_ref_num_str,
    mes_ref_nome,
    mes_ref_abrev,
    mes_atual
)

# CRIMES VIOLENTOS
# Lista de arquivos de entrada

base_excel = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"11 - Publicação SESP - Site/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"Excel/"
)

arquivos_cv = [
    base_excel + "agrupado_furto.xlsx",
    base_excel + "agrupado_lesao_corporal.xlsx",
    base_excel + "25_26_agrupado_crimes_violentos.xlsx",
    base_excel + "agrupado_vitimas_homicidio_consumado.xlsx"
]

# Lista para armazenar os dataframes
lista_dfs_cv = []

for arquivo in arquivos_cv:
    df = pd.read_excel(arquivo)
    
    # Padroniza nome da coluna
    df.columns = df.columns.str.strip().str.lower()
    
    # Garante que "Ano Fato" é numérico
    df["ano fato"] = pd.to_numeric(df["ano fato"], errors='coerce')
    
    # Filtra a partir de 2025
    df_cv_filtrado = df[df["ano fato"] >= 2025]
    
    lista_dfs_cv.append(df_cv_filtrado)

# Empilha tudo
df_cv_final = pd.concat(lista_dfs_cv, ignore_index=True)

mapa_natureza = {
    "ESTUPRO CONSUMADO": "Estupro Consumado",
    "ESTUPRO DE VULNERAVEL CONSUMADO": "Estupro de Vulnerável Consumado",
    "ESTUPRO DE VULNERAVEL TENTADO": "Estupro de Vulnerável Tentado",
    "ESTUPRO TENTADO": "Estupro Tentado",
    "EXTORSAO CONSUMADO": "Extorsão Consumado",
    "EXTORSAO MEDIANTE SEQUESTRO CONSUMADO": "Extorsão Mediante Sequestro Consumado",
    "EXTORSAO TENTADO": "Extorsão Tentado",
    "FURTO": "Furto Consumado",
    "HOMICIDIO CONSUMADO (REGISTROS)": "Homicídio Consumado (Registros)",
    "HOMICIDIO TENTADO": "Homicídio Tentado",
    "LESAO CORPORAL": "Lesão Corporal Consumado",
    "ROUBO CONSUMADO": "Roubo Consumado",
    "ROUBO TENTADO": "Roubo Tentado",
    "SEQUESTRO E CARCERE PRIVADO CONSUMADO": "Sequestro e Cárcere Privado Consumado",
    "SEQUESTRO E CARCERE PRIVADO TENTADO": "Sequestro e Cárcere Privado Tentado",
    "Homicídio Consumado (Vitimas)": "Vítima de Homicídio Consumado"
}

# Cria a coluna nova com base no dicionário
df_cv_final["Natureza Corrigida"] = df_cv_final["natureza"].map(mapa_natureza)

# Se quiser evitar valores nulos (caso algo não esteja no dicionário)
df_cv_final["Natureza Corrigida"] = df_cv_final["Natureza Corrigida"].fillna(df_cv_final["natureza"])

df_cv_final["natureza"] = df_cv_final["natureza"].str.strip().str.upper()

# Exclusão Feminicídio
naturezas_excluir = [
    "FEMINICIDIO TENTADO",
    "FEMINICIDIO CONSUMADO (REGISTROS)"
]

df_cv_final = df_cv_final[~df_cv_final["natureza"].isin(naturezas_excluir)]

# Ordena
df_cv_final = df_cv_final.sort_values(
    by=["ano fato", "mês", "natureza", "município"],
    ascending=[True, True, True, True]
).reset_index(drop=True)

# ALVOS DE FURTO E ROUBO
# Lista de arquivos de entrada
arquivos_alvos = [
    base_excel + "agrupado_alvos_roubo.xlsx",
    base_excel + "agrupado_roubo_veiculos.xlsx",
    base_excel + "agrupado_alvos_furto.xlsx",
    base_excel + "agrupado_furto_veiculos.xlsx"
]

# Lista para armazenar os dataframes
lista_dfs_alvos = []

for arquivo in arquivos_alvos:
    df = pd.read_excel(arquivo)
    
    # Padroniza nome da coluna
    df.columns = df.columns.str.strip().str.lower()
    
    # Garante que "Ano Fato" é numérico
    df["ano fato"] = pd.to_numeric(df["ano fato"], errors='coerce')
    
    # Filtra a partir de 2025
    df_alvos_filtrado = df[df["ano fato"] >= 2025]
    
    lista_dfs_alvos.append(df_alvos_filtrado)

# Empilha tudo
df_alvos_final = pd.concat(lista_dfs_alvos, ignore_index=True)

# Ordena
df_alvos_final = df_alvos_final.sort_values(
    by=["ano fato", "mês", "natureza", "município"],
    ascending=[True, True, True, True]
).reset_index(drop=True)

# Caminho de saída
caminho_saida = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"06 - Monitoamento SIGPLAN/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"matriz_automatizada.xlsx"
)
            
# Exporta
with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
    df_cv_final.to_excel(writer, sheet_name='Dados CV', index=False)
    df_alvos_final.to_excel(writer, sheet_name='Dados Alvos', index=False)

print("Deu bom")