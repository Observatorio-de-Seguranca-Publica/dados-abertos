import pandas as pd
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

# Caminhos dos arquivos do BDHC
base_cv_12_21 = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"Bases completas/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_abrev}/"
    f"XLSX - Uso interno/"
    f"Crimes Violentos - Jan 2012 a Dez 2021.xlsx"
)

base_cv_22_26 = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"Bases completas/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_abrev}/"
    f"XLSX - Uso interno/"
    f"Crimes Violentos - Jan 2022 a {mes_ref_abrev} {ano_ref}.xlsx"
)

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
caminho_csv_1 = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"Bases completas/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_abrev}/"
    f"CSV -Uso externo/"
    f"Crimes Violentos - Jan 2012 a Dez 2021.csv"
)

# Formatação regional
df_cv_12_21 = df_cv_12_21.map(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_cv_12_21.to_csv(
    caminho_csv_1,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

# Caminho de saída para CSV
caminho_csv_2 = (
    f"C:/Users/x15501492/Documents/02 - Publicações/"
    f"Bases completas/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_abrev}/"
    f"CSV -Uso externo/"
    f"Crimes Violentos - Jan 2022 a {mes_ref_abrev} {ano_ref}.csv"
)

# Formatação regional
df_cv_22_26 = df_cv_22_26.map(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_cv_22_26.to_csv(
    caminho_csv_2,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

print("Arquivos CSV exportados com sucesso!")

print(f"✅ Bases unificadas salvas em:\n{caminho_csv_1} e \n{caminho_csv_2}")