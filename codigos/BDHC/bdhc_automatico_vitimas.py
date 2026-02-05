import pandas as pd
import dateutil.parser

# Caminho do arquivo original
arquivo = "C:/Users/x15501492/Downloads/bdhc.xlsx"
sheet = "BD_HC_FATAL_ARMAZÉM"  # ajuste se necessário

# --- Ler com máxima segurança (lê tudo como texto para evitar inferências de tipos) ---
df = pd.read_excel(arquivo, sheet_name=sheet, dtype=str)
print("Total original (linhas):", len(df))

# Normaliza o texto da coluna antes do filtro
df["REDS desconsiderado?"] = (
    df["REDS desconsiderado?"]
    .astype(str)           # garante string
    .str.strip()           # remove espaços extras
    .str.upper()           # tudo maiúsculo
)

# Agora aplica o filtro com segurança
df = df[df["REDS desconsiderado?"] == "NÃO"]
print("Total após filtro 'NÃO' (corrigido):", len(df))


# --- 2) Remover colunas desnecessárias (sua lista) ---
colunas_excluir = [
    "Número REDS Envolvido", "Nº REDS considerado", "Natureza do Delito Completa",
    "Logradouro Ocorrência Não Cadastrado - FATO", "Logradouro Ocorrência - FATO FINAL",
    "Número Logradouro - FATO", "Logradouro Cruzamento - Tipo - FATO", "Logradouro Cruzamento - FATO",
    "Logradouro Cruzamento Não Cadastrado - FATO", "Ponto de Referência - FATO", "RISP - FATO - Antiga",
    "Território de Desenvolvimento", "Território de Desenvolvimento Atual", "Latitude GEOSITE",
    "Longitude GEOSITE", "Endereço Completo", "Latitude Google", "Longitude Google",
    "Coordenada Procurada?", "Tipo de correção", "Nome Envolvido", "Data Nascimento",
    "Faixa Etária", "Faixa Etária SENASP", "Sexo ajustado", "Nome Mãe", "Nome Pai",
    "Grupo Tipo Envolvimento", "Grau Lesão", "Envolvido Civil/Militar", "Policial em Serviço",
    "Ocupação Atual", "Estado Civil", "Documento Identidade", "CPF", "Grupo Causa Presumida - Código",
    "Grupo Causa Presumida", "Causa Presumida - Código", "Meio Utilizado - Código",
    "Código Grupo Compl Nat", "Desc Longa Grupo Compl Nat", "Código Subgrupo Compl Nat",
    "Desc Longa Subgrupo Compl Nat", "Código Local Imediato", "Município Naturalidade",
    "UF Naturalidade - Sigla", "Logradouro Envolvido", "Logradouro Envolvido Não Cadastrado",
    "Número Logradouro Envolvido", "Data Ajuste Endereço", "Hora Ajuste Endereço",
    "Situação Ajuste Endereço", "Tipo Boletim de Ocorrências", "Órgão Unidade Registro",
    "Unidade Área Civil", "Unidade Área Militar", "Unid Registro Nível 6", "Data Inclusão REDS",
    "Data Fechamento REDS", "Natureza Secundária 1", "Natureza Secundária 2", "Natureza Secundária 3",
    "Histórico Ocorrência", "CPC", "COD. Setor Censitário", "TipoIncons", "CMunIBGE",
    "NMunIBGE", "CompMun", 'REDS desconsiderado?'
]

# Remover colunas desnecessárias
df = df.drop(columns=colunas_excluir, errors="ignore")

# Renomear colunas
df = df.rename(columns={
    'Número Envolvido/Ocorrência': 'Qtd Envolvidos',
    'Natureza Principal Completa': 'Natureza Principal Final',
    'Logradouro Ocorrência - Tipo - FATO': 'Logradouro Ocorrência - Tipo',
    'BAIRRO - FATO FINAL': 'Bairro - Fato Final',
    'Município - FATO': 'Município',
    'Município - Código - FATO': 'Município - Código',
    'UF - Sigla - FATO': 'Uf - Sigla',
    'RISP - FATO - Atual': 'Risp',
    'RMBH': 'Rmbh',
    'Latitude Final': 'Latitude',
    'Longitude Final': 'Longitude'
})

# Adicionar novas colunas
df["Descrição Subclasse Nat Principal"] = "HOMICIDIO"
df["Tentado/Consumado Nat Principal"] = "CONSUMADO"
df["Natureza Nomenclatura Banco"] = "Homicídio Consumado (Vitimas)"
df["Mês Fato Resumido"] = ""
df["Faixa 6 Horas Fato"] = ""
df["Tipo_Envolvimento_Lesão_Final"] = "VÍTIMA FATAL"
df["Bairro - Fato Final - Municipio"] = df["Bairro - Fato Final"].fillna('') + ", " + df["Município"].fillna('')

#Mês Fato Resumido (map seguro) ---
mapa_meses = {
    "1": "JAN", "2": "FEV", "3": "MAR", "4": "ABR", "5": "MAI", "6": "JUN",
    "7": "JUL", "8": "AGO", "9": "SET", "10": "OUT", "11": "NOV", "12": "DEZ"
}

# Normaliza e remove .0 se vier como "1.0"
if "Mês Numérico Fato" in df.columns:
    df["Mês Numérico Fato"] = df["Mês Numérico Fato"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Mês Fato Resumido"] = df["Mês Numérico Fato"].map(mapa_meses)
else:
    df["Mês Fato Resumido"] = None
    

# dicionário risps
mapa_risp = {
    "01ª RISP - Belo Horizonte": "RISP 1 - BH", "02ª RISP - Contagem": "RISP 2 - CONTAGEM",
    "03ª RISP - Vespasiano": "RISP 3 - VESPASIANO", "04ª RISP - Juiz de Fora": "RISP 4 - JUIZ DE FORA",
    "05ª RISP - Uberaba": "RISP 5 - UBERABA", "06ª RISP - Lavras": "RISP 6 - LAVRAS",
    "07ª RISP - Divinópolis": "RISP 7 - DIVINÓPOLIS", "08ª RISP - Governador Valadares": "RISP 8 - GOV. VALADARES",
    "09ª RISP - Uberlândia": "RISP 9 - UBERLÂNDIA", "10ª RISP - Patos de Minas": "RISP 10 - PATOS DE MINAS",
    "11ª RISP - Montes Claros": "RISP 11 - MONTES CLAROS", "12ª RISP - Ipatinga": "RISP 12 - IPATINGA",
    "13ª RISP - Barbacena": "RISP 13 - BARBACENA", "14ª RISP - Curvelo": "RISP 14 - CURVELO",
    "15ª RISP - Teófilo Otoni": "RISP 15 - TEÓFILO OTONI", "16ª RISP - Unaí": "RISP 16 - UNAÍ",
    "17ª RISP - Pouso Alegre": "RISP 17 - POUSO ALEGRE", "18ª RISP - Poços de Caldas": "RISP 18 - POÇOS DE CALDAS",
    "19ª RISP - Sete Lagoas": "RISP 19 - SETE LAGOAS"
}

# transforma risps
if "Risp" in df.columns:
    df["Risp"] = df["Risp"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Risp"] = df["Risp"].map(mapa_risp)
else:
    df["Risp"] = None
    
# dicionário dias
mapa_dias = {
    "SEGUNDA-FEIRA": "1", "TERÇA-FEIRA": "2", "QUARTA-FEIRA": "3", "QUINTA-FEIRA": "4", "SEXTA-FEIRA": "5", "SÁBADO": "6", "DOMINGO": "7"
}

# transforma dia da semana
if "Dia da Semana Fato" in df.columns:
    df["Dia da Semana Fato"] = df["Dia da Semana Fato"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Dia da Semana Fato"] = df["Dia da Semana Fato"].map(mapa_dias)
else:
    df["Dia da Semana Fato"] = None
    
# --- Formatar 'Data Fato' ---
if "Data Fato" in df.columns:
    df["Data Fato"] = pd.to_datetime(df["Data Fato"], errors="coerce")  # converte para datetime
    df["Data Fato"] = df["Data Fato"].dt.strftime("%d/%m/%Y")           # formata dd/mm/aaaa

# dicionário rmbh
mapa_rmbh = {
    "1) Belo Horizonte": "NÃO", "2) RMBH (sem BH)": "SIM", "3) Interior de MG": "NÃO"
}

# transforma rmbh
if "Rmbh" in df.columns:
    df["Rmbh"] = df["Rmbh"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Rmbh"] = df["Rmbh"].map(mapa_rmbh)
else:
    df["Rmbh"] = None

if "Qtde Ocorrências" in df.columns:
    df["Qtde Ocorrências"] = 1
    df["Qtde Ocorrências"] = df["Qtde Ocorrências"].astype(int)

# --- Conversão de horário mais tolerante ---
def classificar_faixa6(hora):
    try:
        if pd.isna(hora):
            return None
        if isinstance(hora, str):
            hora = pd.to_datetime(hora, errors="coerce").time()
        if hora is None:
            return None
        h = hora.hour
        if 0 <= h <= 5:
            return "De 00:00 a 05:59"
        elif 6 <= h <= 11:
            return "De 06:00 a 11:59"
        elif 12 <= h <= 17:
            return "De 12:00 a 17:59"
        else:
            return "De 18:00 a 23:59"
    except Exception:
        return None

# Aplica a função diretamente sem forçar o formato de tempo
df["Faixa 6 Horas Fato"] = df["Horário Fato"].apply(classificar_faixa6)

# --- 8) Reordenar colunas conforme sua lista (mantendo só as que existem) ---
ordem_colunas = [
    'Número REDS', 'Qtd Envolvidos', 'Descrição Subclasse Nat Principal', 'Tentado/Consumado Nat Principal',
    'Natureza Principal Final', 'Natureza Nomenclatura Banco', 'Ano Fato', 'Mês Fato Resumido',
    'Mês Numérico Fato', 'Data Fato', 'Dia da Semana Fato', 'Horário Fato', 'Faixa 1 Hora Fato',
    'Faixa 6 Horas Fato', 'Causa Presumida', 'Desc Longa Meio Utilizado', 'Descrição Grupo Local Imediato',
    'Descrição Local Imediato', 'Logradouro Ocorrência - Tipo', 'Bairro - FATO',
    'Bairro Não Cadastrado - FATO', 'Bairro - Fato Final', 'Bairro - Fato Final - Municipio',
    'Município', 'Município - Código', 'Uf - Sigla', 'Unid Registro Nível 9',
    'Risp', 'Rmbh', 'Tipo_Envolvimento_Lesão_Final', 'Idade Aparente', 'Sexo', 'Cútis', 'Escolaridade',
    'Relação Vítima/Autor', 'Tipo Logradouro Envolvido', 'Bairro Envolvido',
    'Bairro Envolvido Não Cadastrado', 'Município Envolvido', 'UF Envolvido - Nome',
    'Latitude', 'Longitude'
]
colunas_finais = [c for c in ordem_colunas if c in df.columns]
df = df[colunas_finais]

# --- 9) Salvar resultado final ---
out1 = "C:/Users/x15501492/Downloads/BDHC_formatado_vitimas.xlsx"
df.to_excel(out1, index=False)
print("Salvo em:", out1)
print("Linhas finais:", len(df))

# --- 10) Salvar em dados abertos ---
out2 = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/XLSX - Uso interno/Vítimas de Homicidio Consumado - Jan 2012 a Jan 2026.xlsx"
df.to_excel(out2, index=False)
print("Salvo em:", out2)
print("Linhas finais:", len(df))

# Lista de colunas a remover
colunas_excluir = [
    "Descrição Subclasse Nat Principal", "Tentado/Consumado Nat Principal", "Natureza Nomenclatura Banco",
    "Logradouro Ocorrência", "Unid Registro Nível 8", "Latitude", "Longitude"
]

# Remover colunas desnecessárias
df_csv = df.drop(columns=colunas_excluir, errors="ignore")

# Caminho de saída para CSV
caminho_csv = "C:/Users/x15501492/Documents/02 - Publicações/Bases completas/2026/01 - Jan/CSV -Uso externo/Vítimas de Homicidio Consumado - Jan 2012 a Jan 2026.csv" 

# Formatação regional
df_csv = df_csv.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, float) else x)

# Exporta com separador ";" e encoding compatível com Excel PT-BR
df_csv.to_csv(
    caminho_csv,
    sep=';',            # separador padrão BR
    index=False,        # sem índice numérico
    encoding='utf-8-sig'  # adiciona BOM, compatível com Excel
)

print("Arquivos CSV exportados com sucesso!")