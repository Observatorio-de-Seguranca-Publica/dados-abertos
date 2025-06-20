import pandas as pd
import numpy as np
from impala.dbapi import connect

# Caminho do arquivo original
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

# Remover colunas desnecessárias
df = df.drop(columns=colunas_excluir, errors="ignore")

# Renomear colunas conforme solicitado
df = df.rename(columns={
    'Número Envolvido/Ocorrência': 'Qtd Envolvidos',
    'Natureza Principal Completa': 'Natureza Principal Final',
    'Logradouro Ocorrência - Tipo - FATO': 'Logradouro Ocorrência - Tipo',
    'RISP - FATO - Atual': 'RISP',
    'Latitude Final': 'Latitude',
    'Longitude Final': 'Longitude'
})

# Adicionar novas colunas conforme solicitado
df["Descrição Subclasse Nat Principal"] = "HOMICIDIO"
df["Tentado/Consumado Nat Principal"] = "CONSUMADO"
df["Natureza Nomenclatura Banco"] = "Homicídio Consumado (Vitimas)"
df["Mês Fato Resumido"] = ""  # Coluna vazia
df["Faixa 6 Horas Fato"] = ""  # Coluna vazia

# Coluna com junção do bairro e município
df["BAIRRO - FATO FINAL -Município"] = df["BAIRRO - FATO FINAL"].fillna('') + ", " + df["Município - FATO"].fillna('')

# Última coluna com valor fixo
df["Tipo_Envolvimento_Lesão_Final"] = "VÍTIMA FATAL"

# Reordenar colunas
ordem_colunas = [
    'Número REDS', 'Qtd Envolvidos', 'Descrição Subclasse Nat Principal', 'Tentado/Consumado Nat Principal',
    'Natureza Principal Final', 'Natureza Nomenclatura Banco', 'Ano Fato', 'Mês Fato Resumido',
    'Mês Numérico Fato', 'Data Fato', 'Dia da Semana Fato', 'Horário Fato', 'Faixa 1 Hora Fato',
    'Faixa 6 Horas Fato', 'Causa Presumida', 'Desc Longa Meio Utilizado', 'Descrição Grupo Local Imediato',
    'Descrição Local Imediato', 'Logradouro Ocorrência - Tipo', 'Bairro - FATO',
    'Bairro Não Cadastrado - FATO', 'BAIRRO - FATO FINAL', 'BAIRRO - FATO FINAL -Município',
    'Município - FATO', 'Município - Código - FATO', 'UF - Sigla - FATO', 'Unid Registro Nível 9',
    'RISP', 'RMBH', 'Tipo_Envolvimento_Lesão_Final', 'Idade Aparente', 'Sexo', 'Cútis', 'Escolaridade',
    'Relação Vítima/Autor', 'Tipo Logradouro Envolvido', 'Bairro Envolvido',
    'Bairro Envolvido Não Cadastrado', 'Município Envolvido', 'UF Envolvido - Nome',
    'Latitude', 'Longitude', 'REDS desconsiderado?'
]

# Reorganiza apenas se todas as colunas estiverem presentes
colunas_finais = [col for col in ordem_colunas if col in df.columns]
df = df[colunas_finais]

# Salvar resultado final
df.to_excel("C:/Users/x15501492/Downloads/BDHC_formatado.xlsx", index=False)
