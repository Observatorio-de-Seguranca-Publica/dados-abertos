import pandas as pd
import numpy as np
from impala.dbapi import connect

# Caminhos dos arquivos do BDHC
base_cv_12_21 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/XLSX - Uso interno/Crimes Violentos - Jan 2012 a Dez 2021.xlsx"
base_cv_22_26 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/XLSX - Uso interno/Crimes Violentos - Jan 2022 a Jan 2026.xlsx"

# 1️⃣ Lê as bases
df_cv_12_21 = pd.read_excel(base_cv_12_21)
df_cv_22_26 = pd.read_excel(base_cv_22_26)

# Lista de colunas a remover
colunas_excluir = [
    "Descrição Subclasse Nat Principal", "Tentado/Consumado Nat Principal", "Natureza Nomenclatura Banco",
    "Logradouro Ocorrência", "Unid Registro Nível 8", "Latitude", "Longitude", "Latitude SIRGAS", "Longitude SIRGAS"
]

# Remover colunas desnecessárias
df_cv_12_21 = df_cv_12_21.drop(columns=colunas_excluir, errors="ignore")
df_cv_22_26 = df_cv_22_26.drop(columns=colunas_excluir, errors="ignore")

# 8️⃣ Salva resultado
# Caminho de saída para CSV
caminho_csv_1 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/CSV -Uso externo/Crimes Violentos - Jan 2012 a Dez 2021.csv" 

# Formatação regional
df_cv_12_21 = df_cv_12_21.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_cv_12_21.to_csv(
    caminho_csv_1,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

# Caminho de saída para CSV
caminho_csv_2 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/CSV -Uso externo/Crimes Violentos - Jan 2022 a Jan 2026.csv" 

# Formatação regional
df_cv_22_26 = df_cv_22_26.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_cv_22_26.to_csv(
    caminho_csv_2,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

print("Arquivos CSV exportados com sucesso!")

print(f"✅ Bases unificadas salvas em:\n{caminho_csv_1} e \n{caminho_csv_2}")