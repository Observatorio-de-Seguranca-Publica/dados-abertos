import pandas as pd
import numpy as np
from impala.dbapi import connect

# Caminho do arquivo
arquivo = "C:/Users/x15501492/Downloads/BDHC.xlsx"

# Lista de colunas a remover
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
    "NMunIBGE", "CompMun"
]

# Leitura da planilha
df = pd.read_excel(arquivo, sheet_name="Vítimas")

# Remover colunas
df = df.drop(columns=colunas_excluir, errors="ignore")

# Exibir resumo do resultado
print(f"Planilha carregada com {df.shape[0]} linhas e {df.shape[1]} colunas após a exclusão.")

# (Opcional) Salvar resultado em novo arquivo
df.to_excel("C:/Users/x15501492/Downloads/BDHC_limpando.xlsx", index=False)
