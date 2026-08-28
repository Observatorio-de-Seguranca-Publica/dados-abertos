import pandas as pd
import numpy as np
from impala.dbapi import connect
from config import datas
from config import paths

# Caminhos dos arquivos de CV
base_cv_12_21 = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"XLSX - Uso interno/"
    f"Crimes Violentos - Jan 2012 a Dez 2021.xlsx"
)

base_cv_22_26 = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"XLSX - Uso interno/"
    f"Crimes Violentos - Jan 2022 a {datas.mes_ref_abrev} {datas.ano_ref}.xlsx"
)

# Lê as bases
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


# Formatação regional sem afetar nulos
df_cv_12_21 = df_cv_12_21.map(
    lambda x: str(x).replace('.', ',')
    if isinstance(x, float) and pd.notna(x)
    else x
)
df_cv_22_26 = df_cv_22_26.map(
    lambda x: str(x).replace('.', ',')
    if isinstance(x, float) and pd.notna(x)
    else x
)

# Remove NaN/None/NaT do dataframe inteiro
df_cv_12_21 = df_cv_12_21.fillna('')
df_cv_22_26 = df_cv_22_26.fillna('')

# Salva resultado
# Caminho de saída para CSV
saida_12_21_local = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"CSV -Uso externo/"
    f"Crimes Violentos - Jan 2012 a Dez 2021.csv"
)
saida_12_21_nuvem = (
    f"{paths.onedrive_completas_externo_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Crimes Violentos - Jan 2012 a Dez 2021.csv"
)
saida_22_26_local = (
    f"{paths.completas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"CSV -Uso externo/"
    f"Crimes Violentos - Jan 2022 a {datas.mes_ref_abrev} {datas.ano_ref}.csv"
)
saida_22_26_nuvem = (
    f"{paths.onedrive_completas_externo_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Crimes Violentos - Jan 2022 a {datas.mes_ref_abrev} {datas.ano_ref}.csv"
)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_cv_12_21.to_csv(
    saida_12_21_local,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig',  # adiciona BOM, compatível com Excel
    na_rep=''
)
df_cv_12_21.to_csv(
    saida_12_21_nuvem,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig',  # adiciona BOM, compatível com Excel
    na_rep=''
)
df_cv_22_26.to_csv(
    saida_22_26_local,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig',  # adiciona BOM, compatível com Excel
    na_rep=''
)
df_cv_22_26.to_csv(
    saida_22_26_nuvem,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig',  # adiciona BOM, compatível com Excel
    na_rep=''
)

print("Arquivos CSV exportados com sucesso!")
print(f"Ok - Bases unificadas salvas em:\n{saida_12_21_local}, \n{saida_12_21_nuvem}, \n{saida_22_26_local} e \n{saida_22_26_nuvem}")