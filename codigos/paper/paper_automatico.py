import pandas as pd
import numpy as np
import os
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

print("Deu bom: planilha gerada")

# A
# T
# E
# N         A partir daqui, o código exporta os arquivos txt para o paper dos municípios
# Ç
# Ã
# O

# Pasta de destino dos papers de município
caminho_saida_paper_municipios = (
    f"C:/Users/x15501492/OneDrive - CAMG/DIS -  Henrique/Paper/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"Municípios"
)

# Lista de municípios
municipios_mg = '''ABADIA DOS DOURADOS,ABAETE,ABRE-CAMPO,ACAIACA,ACUCENA,AGUA BOA,AGUA COMPRIDA,AGUANIL,AGUAS FORMOSAS,AGUAS VERMELHAS,AIMORES,AIURUOCA,ALAGOA,ALBERTINA,ALEM PARAIBA,ALFENAS,ALFREDO VASCONCELOS,ALMENARA,ALPERCATA,ALPINOPOLIS,ALTEROSA,ALTO CAPARAO,ALTO JEQUITIBA,ALTO RIO DOCE,ALVARENGA,ALVINOPOLIS,ALVORADA DE MINAS,AMPARO DA SERRA,ANDRADAS,ANDRELANDIA,ANGELANDIA,ANTONIO CARLOS,ANTONIO DIAS,ANTONIO PRADO DE MINAS,ARACAI,ARACITABA,ARACUAI,ARAGUARI,ARANTINA,ARAPONGA,ARAPORA,ARAPUA,ARAUJOS,ARAXA,ARCEBURGO,ARCOS,AREADO,ARGIRITA,ARICANDUVA,ARINOS,ASTOLFO DUTRA,ATALEIA,AUGUSTO DE LIMA,BAEPENDI,BALDIM,BAMBUI,BANDEIRA,BANDEIRA DO SUL,BARAO DE COCAIS,BARAO DO MONTE ALTO,BARBACENA,BARRA LONGA,BARROSO,BELA VISTA DE MINAS,BELMIRO BRAGA,BELO HORIZONTE,BELO ORIENTE,BELO VALE,BERILO,BERIZAL,BERTOPOLIS,BETIM,BIAS FORTES,BICAS,BIQUINHAS,BOA ESPERANCA,BOCAINA DE MINAS,BOCAIUVA,BOM DESPACHO,BOM JARDIM DE MINAS,BOM JESUS DA PENHA,BOM JESUS DO AMPARO,BOM JESUS DO GALHO,BOM REPOUSO,BOM SUCESSO,BONFIM,BONFINOPOLIS DE MINAS,BONITO DE MINAS,BORDA DA MATA,BOTELHOS,BOTUMIRIM,BRAS PIRES,BRASILANDIA DE MINAS,BRASILIA DE MINAS,BRASOPOLIS,BRAUNAS,BRUMADINHO,BUENO BRANDAO,BUENOPOLIS,BUGRE,BURITIS,BURITIZEIRO,CABECEIRA GRANDE,CABO VERDE,CACHOEIRA DA PRATA,CACHOEIRA DE MINAS,CACHOEIRA DE PAJEU,CACHOEIRA DOURADA,CAETANOPOLIS,CAETE,CAIANA,CAJURI,CALDAS,CAMACHO,CAMANDUCAIA,CAMBUI,CAMBUQUIRA,CAMPANARIO,CAMPANHA,CAMPESTRE,CAMPINA VERDE,CAMPO AZUL,CAMPO BELO,CAMPO DO MEIO,CAMPO FLORIDO,CAMPOS ALTOS,CAMPOS GERAIS,CANA VERDE,CANAA,CANAPOLIS,CANDEIAS,CANTAGALO,CAPARAO,CAPELA NOVA,CAPELINHA,CAPETINGA,CAPIM BRANCO,CAPINOPOLIS,CAPITAO ANDRADE,CAPITAO ENEIAS,CAPITOLIO,CAPUTIRA,CARAI,CARANAIBA,CARANDAI,CARANGOLA,CARATINGA,CARBONITA,CAREACU,CARLOS CHAGAS,CARMESIA,CARMO DA CACHOEIRA,CARMO DA MATA,CARMO DE MINAS,CARMO DO CAJURU,CARMO DO PARANAIBA,CARMO DO RIO CLARO,CARMOPOLIS DE MINAS,CARNEIRINHO,CARRANCAS,CARVALHOPOLIS,CARVALHOS,CASA GRANDE,CASCALHO RICO,CASSIA,CATAGUASES,CATAS ALTAS,CATAS ALTAS DA NORUEGA,CATUJI,CATUTI,CAXAMBU,CEDRO DO ABAETE,CENTRAL DE MINAS,CENTRALINA,CHACARA,CHALE,CHAPADA DO NORTE,CHAPADA GAUCHA,CHIADOR,CIPOTANEA,CLARAVAL,CLARO DOS POCOES,CLAUDIO,COIMBRA,COLUNA,COMENDADOR GOMES,COMERCINHO,CONCEICAO DA APARECIDA,CONCEICAO DA BARRA DE MINAS,CONCEICAO DAS ALAGOAS,CONCEICAO DAS PEDRAS,CONCEICAO DE IPANEMA,CONCEICAO DO MATO DENTRO,CONCEICAO DO PARA,CONCEICAO DO RIO VERDE,CONCEICAO DOS OUROS,CONEGO MARINHO,CONFINS,CONGONHAL,CONGONHAS,CONGONHAS DO NORTE,CONQUISTA,CONSELHEIRO LAFAIETE,CONSELHEIRO PENA,CONSOLACAO,CONTAGEM,COQUEIRAL,CORACAO DE JESUS,CORDISBURGO,CORDISLANDIA,CORINTO,COROACI,COROMANDEL,CORONEL FABRICIANO,CORONEL MURTA,CORONEL PACHECO,CORONEL XAVIER CHAVES,CORREGO DANTA,CORREGO DO BOM JESUS,CORREGO FUNDO,CORREGO NOVO,COUTO DE MAG.DE MINAS,CRISOLITA,CRISTAIS,CRISTALIA,CRISTIANO OTONI,CRISTINA,CRUCILANDIA,CRUZEIRO DA FORTALEZA,CRUZILIA,CUPARAQUE,CURRAL DE DENTRO,CURVELO,DATAS,DELFIM MOREIRA,DELFINOPOLIS,DELTA,DESCOBERTO,DESTERRO DE ENTRE-RIOS,DESTERRO DO MELO,DIAMANTINA,DIOGO DE VASCONCELOS,DIONISIO,DIVINESIA,DIVINO,DIVINO DAS LARANJEIRAS,DIVINOLANDIA DE MINAS,DIVINOPOLIS,DIVISA ALEGRE,DIVISA NOVA,DIVISOPOLIS,DOM BOSCO,DOM CAVATI,DOM JOAQUIM,DOM SILVERIO,DOM VICOSO,DONA EUSEBIA,DORES DE CAMPOS,DORES DE GUANHAES,DORES DO INDAIA,DORES DO TURVO,DORESOPOLIS,DOURADOQUARA,DURANDE,ELOI MENDES,ENGENHEIRO CALDAS,ENGENHEIRO NAVARRO,ENTRE-FOLHAS,ENTRE-RIOS DE MINAS,ERVALIA,ESMERALDAS,ESPERA FELIZ,ESPINOSA,ESPIRITO SANTO DO DOURADO,ESTIVA,ESTRELA DO INDAIA,ESTRELA DO SUL,ESTRELA-DALVA,EUGENOPOLIS,EWBANK DA CAMARA,EXTREMA,FAMA,FARIA LEMOS,FELICIO DOS SANTOS,FELISBURGO,FELIXLANDIA,FERNANDES TOURINHO,FERROS,FERVEDOURO,FLORESTAL,FORMIGA,FORMOSO,FORTALEZA DE MINAS,FORTUNA DE MINAS,FRANCISCO BADARO,FRANCISCO DUMONT,FRANCISCO SA,FRANCISCOPOLIS,FREI GASPAR,FREI INOCENCIO,FREI LAGONEGRO,FRONTEIRA,FRONTEIRA DOS VALES,FRUTA DE LEITE,FRUTAL,FUNILANDIA,GALILEIA,GAMELEIRAS,GLAUCILANDIA,GOIABEIRA,GOIANA,GONCALVES,GONZAGA,GOUVEIA,GOVERNADOR VALADARES,GRAO-MOGOL,GRUPIARA,GUANHAES,GUAPE,GUARACIABA,GUARACIAMA,GUARANESIA,GUARANI,GUARARA,GUARDA-MOR,GUAXUPE,GUIDOVAL,GUIMARANIA,GUIRICEMA,GURINHATA,HELIODORA,IAPU,IBERTIOGA,IBIA,IBIAI,IBIRACATU,IBIRACI,IBIRITE,IBITIURA DE MINAS,IBITURUNA,ICARAI DE MINAS,IGARAPE,IGARATINGA,IGUATAMA,IJACI,ILICINEA,IMBE DE MINAS,INCONFIDENTES,INDAIABIRA,INDIANOPOLIS,INGAI,INHAPIM,INHAUMA,INIMUTABA,IPABA,IPANEMA,IPATINGA,IPIACU,IPUIUNA,IRAI DE MINAS,ITABIRA,ITABIRINHA,ITABIRITO,ITACAMBIRA,ITACARAMBI,ITAGUARA,ITAIPE,ITAJUBA,ITAMARANDIBA,ITAMARATI DE MINAS,ITAMBACURI,ITAMBE DO MATO DENTRO,ITAMOGI,ITAMONTE,ITANHANDU,ITANHOMI,ITAOBIM,ITAPAGIPE,ITAPECERICA,ITAPEVA,ITATIAIUCU,ITAU DE MINAS,ITAUNA,ITAVERAVA,ITINGA,ITUETA,ITUIUTABA,ITUMIRIM,ITURAMA,ITUTINGA,JABOTICATUBAS,JACINTO,JACUI,JACUTINGA,JAGUARACU,JAIBA,JAMPRUCA,JANAUBA,JANUARIA,JAPARAIBA,JAPONVAR,JECEABA,JENIPAPO DE MINAS,JEQUERI,JEQUITAI,JEQUITIBA,JEQUITINHONHA,JESUANIA,JOAIMA,JOANESIA,JOAO MONLEVADE,JOAO PINHEIRO,JOAQUIM FELICIO,JORDANIA,JOSE GONCALVES DE MINAS,JOSE RAYDAN,JOSENOPOLIS,JUATUBA,JUIZ DE FORA,JURAMENTO,JURUAIA,JUVENILIA,LADAINHA,LAGAMAR,LAGOA DA PRATA,LAGOA DOS PATOS,LAGOA DOURADA,LAGOA FORMOSA,LAGOA GRANDE,LAGOA SANTA,LAJINHA,LAMBARI,LAMIM,LARANJAL,LASSANCE,LAVRAS,LEANDRO FERREIRA,LEME DO PRADO,LEOPOLDINA,LIBERDADE,LIMA DUARTE,LIMEIRA DO OESTE,LONTRA,LUISBURGO,LUISLANDIA,LUMINARIAS,LUZ,MACHACALIS,MACHADO,MADRE DE DEUS DE MINAS,MALACACHETA,MAMONAS,MANGA,MANHUACU,MANHUMIRIM,MANTENA,MAR DE ESPANHA,MARAVILHAS,MARIA DA FE,MARIANA,MARILAC,MARIO CAMPOS,MARIPA DE MINAS,MARLIERIA,MARMELOPOLIS,MARTINHO CAMPOS,MARTINS SOARES,MATA VERDE,MATERLANDIA,MATEUS LEME,MATHIAS LOBATO,MATIAS BARBOSA,MATIAS CARDOSO,MATIPO,MATO VERDE,MATOZINHOS,MATUTINA,MEDEIROS,MEDINA,MENDES PIMENTEL,MERCES,MESQUITA,MINAS NOVAS,MINDURI,MIRABELA,MIRADOURO,MIRAI,MIRAVANIA,MOEDA,MOEMA,MONJOLOS,MONSENHOR PAULO,MONTALVANIA,MONTE ALEGRE DE MINAS,MONTE AZUL,MONTE BELO,MONTE CARMELO,MONTE FORMOSO,MONTE SANTO DE MINAS,MONTE SIAO,MONTES CLAROS,MONTEZUMA,MORADA NOVA DE MINAS,MORRO DA GARCA,MORRO DO PILAR,MUNHOZ,MURIAE,MUTUM,MUZAMBINHO,NACIP RAYDAN,NANUQUE,NAQUE,NATALANDIA,NATERCIA,NAZARENO,NEPOMUCENO,NINHEIRA,NOVA BELEM,NOVA ERA,NOVA LIMA,NOVA MODICA,NOVA PONTE,NOVA PORTEIRINHA,NOVA RESENDE,NOVA SERRANA,NOVA UNIAO,NOVO CRUZEIRO,NOVO ORIENTE DE MINAS,NOVORIZONTE,OLARIA,OLHOS-D'AGUA,OLIMPIO NORONHA,OLIVEIRA,OLIVEIRA FORTES,ONCA DO PITANGUI,ORATORIOS,ORIZANIA,OURO BRANCO,OURO FINO,OURO PRETO,OURO VERDE DE MINAS,PADRE CARVALHO,PADRE PARAISO,PAI PEDRO,PAINEIRAS,PAINS,PAIVA,PALMA,PALMOPOLIS,PAPAGAIOS,PARA DE MINAS,PARACATU,PARAGUACU,PARAISOPOLIS,PARAOPEBA,PASSA QUATRO,PASSA TEMPO,PASSA VINTE,PASSABEM,PASSOS,PATIS,PATOS DE MINAS,PATROCINIO,PATROCINIO DO MURIAE,PAULA CANDIDO,PAULISTAS,PAVAO,PECANHA,PEDRA AZUL,PEDRA BONITA,PEDRA DO ANTA,PEDRA DO INDAIA,PEDRA DOURADA,PEDRALVA,PEDRAS DE MARIA DA CRUZ,PEDRINOPOLIS,PEDRO LEOPOLDO,PEDRO TEIXEIRA,PEQUERI,PEQUI,PERDIGAO,PERDIZES,PERDOES,PERIQUITO,PESCADOR,PIAU,PIEDADE DE CARATINGA,PIEDADE DE PONTE NOVA,PIEDADE DO RIO GRANDE,PIEDADE DOS GERAIS,PIMENTA,PINGO D'AGUA,PINTOPOLIS,PIRACEMA,PIRAJUBA,PIRANGA,PIRANGUCU,PIRANGUINHO,PIRAPETINGA,PIRAPORA,PIRAUBA,PITANGUI,PIUMHI,PLANURA,POCO FUNDO,POCOS DE CALDAS,POCRANE,POMPEU,PONTE NOVA,PONTO CHIQUE,PONTO DOS VOLANTES,PORTEIRINHA,PORTO FIRME,POTE,POUSO ALEGRE,POUSO ALTO,PRADOS,PRATA,PRATAPOLIS,PRATINHA,PRESIDENTE BERNARDES,PRESIDENTE JUSCELINO,PRESIDENTE KUBITSCHEK,PRESIDENTE OLEGARIO,PRUDENTE DE MORAIS,QUARTEL GERAL,QUELUZITO,RAPOSOS,RAUL SOARES,RECREIO,REDUTO,RESENDE COSTA,RESPLENDOR,RESSAQUINHA,RIACHINHO,RIACHO DOS MACHADOS,RIBEIRAO DAS NEVES,RIBEIRAO VERMELHO,RIO ACIMA,RIO CASCA,RIO DO PRADO,RIO DOCE,RIO ESPERA,RIO MANSO,RIO NOVO,RIO PARANAIBA,RIO PARDO DE MINAS,RIO PIRACICABA,RIO POMBA,RIO PRETO,RIO VERMELHO,RITAPOLIS,ROCHEDO DE MINAS,RODEIRO,ROMARIA,ROSARIO DA LIMEIRA,RUBELITA,RUBIM,SABARA,SABINOPOLIS,SACRAMENTO,SALINAS,SALTO DA DIVISA,SANTA BARBARA,SANTA BARBARA DO LESTE,SANTA BARBARA DO MONTE VERDE,SANTA BARBARA DO TUGURIO,SANTA CRUZ DE MINAS,SANTA CRUZ DE SALINAS,SANTA CRUZ DO ESCALVADO,SANTA EFIGENIA DE MINAS,SANTA FE DE MINAS,SANTA HELENA DE MINAS,SANTA JULIANA,SANTA LUZIA,SANTA MARGARIDA,SANTA MARIA DE ITABIRA,SANTA MARIA DO SALTO,SANTA MARIA DO SUACUI,SANTA RITA DE CALDAS,SANTA RITA DE JACUTINGA,SANTA RITA DE MINAS,SANTA RITA DO IBITIPOCA,SANTA RITA DO ITUETO,SANTA RITA DO SAPUCAI,SANTA ROSA DA SERRA,SANTA VITORIA,SANTANA DA VARGEM,SANTANA DE CATAGUASES,SANTANA DE PIRAPAMA,SANTANA DO DESERTO,SANTANA DO GARAMBEU,SANTANA DO JACARE,SANTANA DO MANHUACU,SANTANA DO PARAISO,SANTANA DO RIACHO,SANTANA DOS MONTES,SANTO ANT DO AMPARO,SANTO ANTONIO DO AVENTUREIRO,SANTO ANTONIO DO GRAMA,SANTO ANTONIO DO ITAMBE,SANTO ANTONIO DO JACINTO,SANTO ANTONIO DO MONTE,SANTO ANTONIO DO RETIRO,SANTO ANTONIO DO RIO ABAIXO,SANTO HIPOLITO,SANTOS DUMONT,SAO BENTO ABADE,SAO BRAS DO SUACUI,SAO DOMINGOS DAS DORES,SAO DOMINGOS DO PRATA,SAO FELIX DE MINAS,SAO FRANCISCO,SAO FRANCISCO DE PAULA,SAO FRANCISCO DE SALES,SAO FRANCISCO DO GLORIA,SAO GERALDO,SAO GERALDO DA PIEDADE,SAO GERALDO DO BAIXIO,SAO GONCALO DO ABAETE,SAO GONCALO DO PARA,SAO GONCALO DO RIO ABAIXO,SAO GONCALO DO RIO PRETO,SAO GONCALO DO SAPUCAI,SAO GOTARDO,SAO JOAO BATISTA GLORIA,SAO JOAO DA LAGOA,SAO JOAO DA MATA,SAO JOAO DA PONTE,SAO JOAO DAS MISSOES,SAO JOAO DEL REI,SAO JOAO DO MANHUACU,SAO JOAO DO MANTENINHA,SAO JOAO DO ORIENTE,SAO JOAO DO PACUI,SAO JOAO DO PARAISO,SAO JOAO EVANGELISTA,SAO JOAO NEPOMUCENO,SAO JOAQUIM DE BICAS,SAO JOSE DA BARRA,SAO JOSE DA LAPA,SAO JOSE DA SAFIRA,SAO JOSE DA VARGINHA,SAO JOSE DO ALEGRE,SAO JOSE DO DIVINO,SAO JOSE DO GOIABAL,SAO JOSE DO JACURI,SAO JOSE DO MANTIMENTO,SAO LOURENCO,SAO MIGUEL DO ANTA,SAO PEDRO DA UNIAO,SAO PEDRO DO SUACUI,SAO PEDRO DOS FERROS,SAO ROMAO,SAO ROQUE DE MINAS,SAO SEB.DO RIO PRETO,SAO SEBAST. DO MARANHAO,SAO SEBASTIAO DA BELA VISTA,SAO SEBASTIAO DA VARGEM ALEGRE,SAO SEBASTIAO DO ANTA,SAO SEBASTIAO DO OESTE,SAO SEBASTIAO DO RIO VERDE,SAO SEBASTIAO PARAISO,SAO TIAGO,SAO TOMAS DE AQUINO,SAO TOME DAS LETRAS,SAO VICENTE DE MINAS,SAPUCAI-MIRIM,SARDOA,SARZEDO,SEM-PEIXE,SENADOR AMARAL,SENADOR CORTES,SENADOR FIRMINO,SENADOR JOSE BENTO,SENADOR MODESTINO GONCALVES,SENHORA DE OLIVEIRA,SENHORA DO PORTO,SENHORA DOS REMEDIOS,SERICITA,SERITINGA,SERRA AZUL DE MINAS,SERRA DA SAUDADE,SERRA DO SALITRE,SERRA DOS AIMORES,SERRANIA,SERRANOPOLIS DE MINAS,SERRANOS,SERRO,SETE LAGOAS,SETUBINHA,SILVEIRANIA,SILVIANOPOLIS,SIMAO PEREIRA,SIMONESIA,SOBRALIA,SOLEDADE DE MINAS,TABULEIRO,TAIOBEIRAS,TAPARUBA,TAPIRA,TAPIRAI,TAQUARACU DE MINAS,TARUMIRIM,TEIXEIRAS,TEOFILO OTONI,TIMOTEO,TIRADENTES,TIROS,TOCANTINS,TOCOS DO MOJI,TOLEDO,TOMBOS,TRES CORACOES,TRES MARIAS,TRES PONTAS,TUMIRITINGA,TUPACIGUARA,TURMALINA,TURVOLANDIA,UBA,UBAI,UBAPORANGA,UBERABA,UBERLANDIA,UMBURATIBA,UNAI,UNIAO DE MINAS,URUANA DE MINAS,URUCANIA,URUCUIA,VARGEM ALEGRE,VARGEM BONITA,VARGEM GRANDE DO RIO PARDO,VARGINHA,VARJAO DE MINAS,VARZEA DA PALMA,VARZELANDIA,VAZANTE,VERDELANDIA,VEREDINHA,VERISSIMO,VERMELHO NOVO,VESPASIANO,VICOSA,VIEIRAS,VIRGEM DA LAPA,VIRGINIA,VIRGINOPOLIS,VIRGOLANDIA,VISCONDE DO RIO BRANCO,VOLTA GRANDE,WENCESLAU BRAZ
'''

# Divide em lista e remove espaços extras
lista_municipios = [m.strip() for m in municipios_mg.split(',')]

# Função auxiliar para formatação de listas
def format_lista(lista):
    if len(lista) > 1:
        return ', '.join(lista[:-1]) + ' e ' + lista[-1]
    elif lista:
        return lista[0]
    else:
        return ''
    
# Loop por município
for municipio in lista_municipios:
    municipio_upper = municipio.upper()

    texto = ""
    texto2 = ""
    
    # Parte 1: Comparação 2024 x 2025 (ano completo)
    df_mun = df_cv_final[df_cv_final['municipio'].str.upper() == municipio_upper]
    df_mun_1 = df_mun[df_mun['ano fato'].isin([2024, 2025])]

    if not df_mun_1.empty:
        df_agg = df_mun_1.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2024, 2025]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2025] - df_pivot[2024]) / df_pivot[2024].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{idx} (passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto += f"{municipio_upper}\n"
        texto += f"Em uma análise de Janeiro a Dezembro de 2025 frente ao mesmo período de 2024, o município de {municipio.title()} "

        if reducao:
            texto += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."

    # Parte 2: Comparação mensal 2025 x 2026
    df_mun_2 = df_cv_final[
        (df_cv_final['municipio'].str.upper() == municipio_upper) &
        (df_cv_final['ano fato'].isin([2025, 2026])) &
        (df_cv_final['mes'] == mes_ref)
    ]

    if not df_mun_2.empty:
        df_agg = df_mun_2.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2025, 2026]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2026] - df_pivot[2025]) / df_pivot[2025].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{idx} (passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {mes_ref_nome} de 2026 frente ao mesmo período de 2025, o município de {municipio.title()} "

        if reducao:
            texto2 += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto2 += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto2 += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto2 += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto2 += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."
            
    # Se algum texto foi gerado, salva
    if texto or texto2:
        caminho_txt = os.path.join(caminho_saida_paper_municipios, f"{municipio_upper}.txt")
        with open(caminho_txt, "w", encoding="utf-8") as f:
            f.write(texto.strip() + "\n\n" + texto2.strip())

        print(f"Relatório gerado para: {municipio}")
    else:
        print(f"Sem dados para: {municipio}")
        
        
# A
# T
# E
# N         A partir daqui, o código exporta os arquivos txt para o paper das RISPs
# Ç
# Ã
# O

# Pasta de destino dos papers de RISP
caminho_saida_paper_risps = (
    f"C:/Users/x15501492/OneDrive - CAMG/DIS -  Henrique/Paper/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"RISP"
)

# Lista de RISPs
risps_mg = '''RISP 1 - BH,RISP 2 - CONTAGEM,RISP 3 - VESPASIANO,RISP 4 - JUIZ DE FORA,RISP 5 - UBERABA,RISP 6 - LAVRAS,RISP 7 - DIVINÓPOLIS,RISP 8 - GOV. VALADARES,RISP 9 - UBERLÂNDIA,RISP 10 - PATOS DE MINAS,RISP 11 - MONTES CLAROS,RISP 12 - IPATINGA,RISP 13 - BARBACENA,RISP 14 - CURVELO,RISP 15 - TEÓFILO OTONI,RISP 16 - UNAÍ,RISP 17 - POUSO ALEGRE,RISP 18 - POÇOS DE CALDAS,RISP 19 - SETE LAGOAS
'''

# Divide em lista e remove espaços extras
lista_risps = [m.strip() for m in risps_mg.split(',')]

# Função auxiliar para formatação de listas
def format_lista(lista):
    if len(lista) > 1:
        return ', '.join(lista[:-1]) + ' e ' + lista[-1]
    elif lista:
        return lista[0]
    else:
        return ''
    
# Loop por RISP
for risp in lista_risps:
    risp_upper = risp.upper()

    texto = ""
    texto2 = ""

    # Parte 1: Comparação 2024 x 2025 (ano completo)
    df_risp = df_cv_final[df_cv_final['risp'].str.upper() == risp_upper]
    df_risp_1 = df_risp[df_risp['ano fato'].isin([2024, 2025])]

    if not df_risp_1.empty:
        df_agg = df_risp_1.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2024, 2025]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2025] - df_pivot[2024]) / df_pivot[2024].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            risp25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{idx} (passando de {q24} para {q25} {risp25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {risp25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {risp25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto += f"{risp_upper}\n"
        texto += f"Em uma análise de Janeiro a Dezembro de 2025 frente ao mesmo período de 2024, a {risp.title()} "

        if reducao:
            texto += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."
            
    # Parte 2: Comparação Mensal 2025 x 2026
    df_risp_2 = df_cv_final[
        (df_cv_final['risp'].str.upper() == risp_upper) &
        (df_cv_final['ano fato'].isin([2025, 2026])) &
        (df_cv_final['mes'] == mes_ref)
    ]

    if not df_risp_2.empty:
        df_agg = df_risp_2.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2025, 2026]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2026] - df_pivot[2025]) / df_pivot[2025].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            risp26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{idx} (passando de {q25} para {q26} {risp26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {risp26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {risp26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {mes_ref_nome} de 2026 frente ao mesmo período de 2025, a {risp.title()} "

        if reducao:
            texto2 += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto2 += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto2 += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto2 += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto2 += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."

    # Se algum texto foi gerado, salva
    if texto or texto2:
        caminho_txt = os.path.join(caminho_saida_paper_risps, f"{risp_upper}.txt")
        with open(caminho_txt, "w", encoding="utf-8") as f:
            f.write(texto.strip() + "\n\n" + texto2.strip())

        print(f"Relatório gerado para: {risp}")
    else:
        print(f"Sem dados para: {risp}")
        
# A
# T
# E
# N         A partir daqui, o código exporta os arquivos txt para o paper das Mesorregiões
# Ç
# Ã
# O

# Pasta de destino dos papers de Mesorregião
caminho_saida_paper_mesorregioes = (
    f"C:/Users/x15501492/OneDrive - CAMG/DIS -  Henrique/Paper/"
    f"{ano_ref}/"
    f"{mes_ref_num_str} - {mes_ref_nome}/"
    f"Mesorregiões"
)

# Lista de Mesorregiões
regioes_mg = '''TRIÂNGULO MINEIRO E ALTO PARANAÍBA,CENTRAL MINEIRA,ZONA DA MATA,VALE DO RIO DOCE,OESTE DE MINAS,VALE DO MUCURI,NORTE DE MINAS,SUL E SUDOESTE DE MINAS,CAMPO DAS VERTENTES,JEQUITINHONHA,METROPOLITANA DE BELO HORIZONTE,NOROESTE DE MINAS
'''

# Divide em lista e remove espaços extras
lista_regioes = [m.strip() for m in regioes_mg.split(',')]

# Função auxiliar para formatação de listas
def format_lista(lista):
    if len(lista) > 1:
        return ', '.join(lista[:-1]) + ' e ' + lista[-1]
    elif lista:
        return lista[0]
    else:
        return ''
    
# Loop por mesorregião
for regiao in lista_regioes:
    regiao_upper = regiao.upper()

    texto = ""
    texto2 = ""

    # Parte 1: Comparação 2024 x 2025 (ano completo)
    df_reg = df_cv_final[df_cv_final['regiao'].str.upper() == regiao_upper]
    df_reg_1 = df_reg[df_reg['ano fato'].isin([2024, 2025])]

    if not df_reg_1.empty:
        df_agg = df_reg_1.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2024, 2025]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2025] - df_pivot[2024]) / df_pivot[2024].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{idx} (passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto += f"{regiao_upper}\n"
        texto += f"Em uma análise de Janeiro a Dezembro de 2025 frente ao mesmo período de 2024, a mesorregião de {regiao.title()} "

        if reducao:
            texto += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."
            
    # Parte 2: Comparação Mensal 2025 x 2026
    df_reg_2 = df_cv_final[
        (df_cv_final['regiao'].str.upper() == regiao_upper) &
        (df_cv_final['ano fato'].isin([2025, 2026])) &
        (df_cv_final['mes'] == mes_ref)
    ]

    if not df_reg_2.empty:
        df_agg = df_reg_2.groupby(['natureza', 'ano fato'])['registros'].sum().reset_index()
        df_pivot = df_agg.pivot(index='natureza', columns='ano fato', values='registros').fillna(0).astype(int)

        for ano in [2025, 2026]:
            if ano not in df_pivot.columns:
                df_pivot[ano] = 0

        df_pivot['Percentual'] = ((df_pivot[2026] - df_pivot[2025]) / df_pivot[2025].replace(0, np.nan)) * 100
        df_pivot = df_pivot.fillna(0)

        reducao, aumento, sem_variacao = [], [], []

        for idx, row in df_pivot.iterrows():
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{idx} (passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{idx} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(idx)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {mes_ref_nome} de 2026 frente ao mesmo período de 2025, a mesorregião de {regiao.title()} "

        if reducao:
            texto2 += f"apresentou uma redução nos crimes de {format_lista(reducao)}. "
        else:
            texto2 += "não apresentou redução em nenhum dos crimes analisados. "

        if aumento:
            texto2 += f"Contudo, houve aumento ou passou a apresentar registro nos crimes de {format_lista(aumento)}. "
        else:
            texto2 += "Também não houve aumento em nenhum dos crimes analisados. "

        if sem_variacao:
            texto2 += f"Os demais crimes violentos se mantiveram com o mesmo número de registros nos dois anos analisados: {format_lista(sem_variacao)}."

    # Se algum texto foi gerado, salva
    if texto or texto2:
        caminho_txt = os.path.join(caminho_saida_paper_mesorregioes, f"{regiao_upper}.txt")
        with open(caminho_txt, "w", encoding="utf-8") as f:
            f.write(texto.strip() + "\n\n" + texto2.strip())

        print(f"Relatório gerado para: {regiao}")
    else:
        print(f"Sem dados para: {regiao}")