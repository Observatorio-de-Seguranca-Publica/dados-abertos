import pandas as pd
import numpy as np
from impala.dbapi import connect

# Caminhos dos arquivos
bdhc_tratado = "C:/Users/x15501492/Downloads/BDHC_formatado_registros.xlsx"
base_cv_12_21 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/12 - Dez/XLSX - Uso interno/Crimes Violentos - Jan 2012 a Dez 2021.xlsx"
base_cv_22_25 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/12 - Dez/XLSX - Uso interno/Crimes Violentos - Jan 2022 a Nov 2025.xlsx"

# 1️⃣ Lê as bases
df_bdhc = pd.read_excel(bdhc_tratado)
df_cv_12_21 = pd.read_excel(base_cv_12_21)
df_cv_22_25 = pd.read_excel(base_cv_22_25)

# 2️⃣ Filtra a BDHC apenas entre 2012 e 2021
df_bdhc_filtrada_12_21 = df_bdhc[
    (df_bdhc["Ano Fato"] >= 2012) & (df_bdhc["Ano Fato"] <= 2021)
].copy()

# 3️⃣ Garante que as colunas estão iguais (ordem e nomes)
#    Mantém as colunas que existem nas duas bases
df_bdhc_filtrada_12_21 = df_bdhc_filtrada_12_21.reindex(columns=df_cv_12_21.columns)

# 4️⃣ Junta (empilha)
df_final_12_21 = pd.concat([df_cv_12_21, df_bdhc_filtrada_12_21], ignore_index=True)

print(f"Base CV 2012–2021 original: {len(df_cv_12_21)}")
print(f"Base BDHC 2012–2021 filtrada: {len(df_bdhc_filtrada_12_21)}")
print(f"→ Base unificada 2012–2021: {len(df_final_12_21)}")

# 5️⃣ Filtra a BDHC apenas entre 2022 e 2025
df_bdhc_filtrada_22_25 = df_bdhc[
    (df_bdhc["Ano Fato"] >= 2022) & (df_bdhc["Ano Fato"] <= 2025)
].copy()

# 6️⃣ Garante que as colunas estão iguais (ordem e nomes)
#    Mantém as colunas que existem nas duas bases
df_bdhc_filtrada_22_25 = df_bdhc_filtrada_22_25.reindex(columns=df_cv_22_25.columns)

# 7️⃣ Junta (empilha)
df_final_22_25 = pd.concat([df_cv_22_25, df_bdhc_filtrada_22_25], ignore_index=True)

print(f"Base CV 2022–2025 original: {len(df_cv_22_25)}")
print(f"Base BDHC 2022–2025 filtrada: {len(df_bdhc_filtrada_22_25)}")
print(f"→ Base unificada 2022–2025: {len(df_final_22_25)}")

print(f"Total final após junção (2012–2021): {len(df_final_12_21)} registros")
print(f"Total final após junção (2022–2025): {len(df_final_22_25)} registros")

# 8️⃣ Salva resultado
saida_12_21 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/12 - Dez/XLSX - Uso interno/Crimes Violentos - Jan 2012 a Dez 2021.xlsx"
df_final_12_21.to_excel(saida_12_21, index=False)
saida_22_25 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/12 - Dez/XLSX - Uso interno/Crimes Violentos - Jan 2022 a Nov 2025.xlsx"
df_final_22_25.to_excel(saida_22_25, index=False)

print(f"✅ Bases unificadas salvas em:\n{saida_12_21} e \n{saida_22_25}")
