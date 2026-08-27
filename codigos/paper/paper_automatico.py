import pandas as pd
import numpy as np
import os
from unidecode import unidecode
from config import datas
from config import paths

# Lista de arquivos de entrada: planilhas de Crimes Violentos e dicionário de regiões
base_excel = (
    f"{paths.agrupadas_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
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

df_cv_final["municipio"] = (
    df_cv_final["municipio"]
    .astype(str)
    .str.strip()
    .str.upper()
)

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
    f"{paths.paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"paper_automatico_{datas.mes_ref_abrev}.xlsx"
)

# Caminho de saída
caminho_saida_one_drive = (
    f"{paths.onedrive_paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"paper_automatico_{datas.mes_ref_abrev}.xlsx"
)

f"{paths.agrupadas_dir}/"
f"{datas.ano_ref}/"
f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
            
# Exporta
with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
    df_cv_final.to_excel(writer, sheet_name='Dados CV', index=False)
    
with pd.ExcelWriter(caminho_saida_one_drive, engine='openpyxl') as writer:
    df_cv_final.to_excel(writer, sheet_name='Dados CV', index=False)

print("Deu bom: planilha gerada")

# A
# T
# E
# N         A partir daqui, o código exporta os arquivos txt para o paper dos municípios
# Ç
# Ã
# O

# Pastas de destino dos papers de município
caminho_saida_paper_municipios = (
    f"{paths.paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Municípios"
)

caminho_saida_paper_municipios_one_drive = (
    f"{paths.onedrive_paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
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
    
# Dicionário de correção de municípios
dict_municipios = {
    "ABADIA DOS DOURADOS": "Abadia dos Dourados",
    "ABAETE": "Abaeté",
    "ABRE-CAMPO": "Abre Campo",
    "ACAIACA": "Acaiaca",
    "ACUCENA": "Açucena",
    "AGUA BOA": "Água Boa",
    "AGUA COMPRIDA": "Água Comprida",
    "AGUANIL": "Aguanil",
    "AGUAS FORMOSAS": "Águas Formosas",
    "AGUAS VERMELHAS": "Águas Vermelhas",
    "AIMORES": "Aimorés",
    "AIURUOCA": "Aiuruoca",
    "ALAGOA": "Alagoa",
    "ALBERTINA": "Albertina",
    "ALEM PARAIBA": "Além Paraíba",
    "ALFENAS": "Alfenas",
    "ALFREDO VASCONCELOS": "Alfredo Vasconcelos",
    "ALMENARA": "Almenara",
    "ALPERCATA": "Alpercata",
    "ALPINOPOLIS": "Alpinópolis",
    "ALTEROSA": "Alterosa",
    "ALTO CAPARAO": "Alto Caparaó",
    "ALTO JEQUITIBA": "Alto Jequitibá",
    "ALTO RIO DOCE": "Alto Rio Doce",
    "ALVARENGA": "Alvarenga",
    "ALVINOPOLIS": "Alvinópolis",
    "ALVORADA DE MINAS": "Alvorada de Minas",
    "AMPARO DO SERRA": "Amparo do Serra",
    "ANDRADAS": "Andradas",
    "ANDRELANDIA": "Andrelândia",
    "ANGELANDIA": "Angelândia",
    "ANTONIO CARLOS": "Antônio Carlos",
    "ANTONIO DIAS": "Antônio Dias",
    "ANTONIO PRADO DE MINAS": "Antônio Prado de Minas",
    "ARACAI": "Araçaí",
    "ARACITABA": "Aracitaba",
    "ARACUAI": "Araçuaí",
    "ARAGUARI": "Araguari",
    "ARANTINA": "Arantina",
    "ARAPONGA": "Araponga",
    "ARAPORA": "Araporã",
    "ARAPUA": "Arapuá",
    "ARAUJOS": "Araújos",
    "ARAXA": "Araxá",
    "ARCEBURGO": "Arceburgo",
    "ARCOS": "Arcos",
    "AREADO": "Areado",
    "ARGIRITA": "Argirita",
    "ARICANDUVA": "Aricanduva",
    "ARINOS": "Arinos",
    "ASTOLFO DUTRA": "Astolfo Dutra",
    "ATALEIA": "Ataléia",
    "AUGUSTO DE LIMA": "Augusto de Lima",
    "BAEPENDI": "Baependi",
    "BALDIM": "Baldim",
    "BAMBUI": "Bambuí",
    "BANDEIRA": "Bandeira",
    "BANDEIRA DO SUL": "Bandeira do Sul",
    "BARAO DE COCAIS": "Barão de Cocais",
    "BARAO DE MONTE ALTO": "Barão do Monte Alto",
    "BARBACENA": "Barbacena",
    "BARRA LONGA": "Barra Longa",
    "BARROSO": "Barroso",
    "BELA VISTA DE MINAS": "Bela Vista de Minas",
    "BELMIRO BRAGA": "Belmiro Braga",
    "BELO HORIZONTE": "Belo Horizonte",
    "BELO ORIENTE": "Belo Oriente",
    "BELO VALE": "Belo Vale",
    "BERILO": "Berilo",
    "BERIZAL": "Berizal",
    "BERTOPOLIS": "Bertópolis",
    "BETIM": "Betim",
    "BIAS FORTES": "Bias Fortes",
    "BICAS": "Bicas",
    "BIQUINHAS": "Biquinhas",
    "BOA ESPERANCA": "Boa Esperança",
    "BOCAINA DE MINAS": "Bocaina de Minas",
    "BOCAIUVA": "Bocaiúva",
    "BOM DESPACHO": "Bom Despacho",
    "BOM JARDIM DE MINAS": "Bom Jardim de Minas",
    "BOM JESUS DA PENHA": "Bom Jesus da Penha",
    "BOM JESUS DO AMPARO": "Bom Jesus do Amparo",
    "BOM JESUS DO GALHO": "Bom Jesus do Galho",
    "BOM REPOUSO": "Bom Repouso",
    "BOM SUCESSO": "Bom Sucesso",
    "BONFIM": "Bonfim",
    "BONFINOPOLIS DE MINAS": "Bonfinópolis de Minas",
    "BONITO DE MINAS": "Bonito de Minas",
    "BORDA DA MATA": "Borda da Mata",
    "BOTELHOS": "Botelhos",
    "BOTUMIRIM": "Botumirim",
    "BRAS PIRES": "Brás Pires",
    "BRASILANDIA DE MINAS": "Brasilândia de Minas",
    "BRASILIA DE MINAS": "Brasília de Minas",
    "BRASOPOLIS": "Braúnas",
    "BRAUNAS": "Brazópolis",
    "BRUMADINHO": "Brumadinho",
    "BUENO BRANDAO": "Bueno Brandão",
    "BUENOPOLIS": "Buenópolis",
    "BUGRE": "Bugre",
    "BURITIS": "Buritis",
    "BURITIZEIRO": "Buritizeiro",
    "CABECEIRA GRANDE": "Cabeceira Grande",
    "CABO VERDE": "Cabo Verde",
    "CACHOEIRA DA PRATA": "Cachoeira da Prata",
    "CACHOEIRA DE MINAS": "Cachoeira de Minas",
    "CACHOEIRA DE PAJEU": "Cachoeira de Pajeú",
    "CACHOEIRA DOURADA": "Cachoeira Dourada",
    "CAETANOPOLIS": "Caetanópolis",
    "CAETE": "Caeté",
    "CAIANA": "Caiana",
    "CAJURI": "Cajuri",
    "CALDAS": "Caldas",
    "CAMACHO": "Camacho",
    "CAMANDUCAIA": "Camanducaia",
    "CAMBUI": "Cambuí",
    "CAMBUQUIRA": "Cambuquira",
    "CAMPANARIO": "Campanário",
    "CAMPANHA": "Campanha",
    "CAMPESTRE": "Campestre",
    "CAMPINA VERDE": "Campina Verde",
    "CAMPO AZUL": "Campo Azul",
    "CAMPO BELO": "Campo Belo",
    "CAMPO DO MEIO": "Campo do Meio",
    "CAMPO FLORIDO": "Campo Florido",
    "CAMPOS ALTOS": "Campos Altos",
    "CAMPOS GERAIS": "Campos Gerais",
    "CANA VERDE": "Cana Verde",
    "CANAA": "Canaã",
    "CANAPOLIS": "Canápolis",
    "CANDEIAS": "Candeias",
    "CANTAGALO": "Cantagalo",
    "CAPARAO": "Caparaó",
    "CAPELA NOVA": "Capela Nova",
    "CAPELINHA": "Capelinha",
    "CAPETINGA": "Capetinga",
    "CAPIM BRANCO": "Capim Branco",
    "CAPINOPOLIS": "Capinópolis",
    "CAPITAO ANDRADE": "Capitão Andrade",
    "CAPITAO ENEAS": "Capitão Enéas",
    "CAPITOLIO": "Capitólio",
    "CAPUTIRA": "Caputira",
    "CARAI": "Caraí",
    "CARANAIBA": "Caranaíba",
    "CARANDAI": "Carandaí",
    "CARANGOLA": "Carangola",
    "CARATINGA": "Caratinga",
    "CARBONITA": "Carbonita",
    "CAREACU": "Careaçu",
    "CARLOS CHAGAS": "Carlos Chagas",
    "CARMESIA": "Carmésia",
    "CARMO DA CACHOEIRA": "Carmo da Cachoeira",
    "CARMO DA MATA": "Carmo da Mata",
    "CARMO DE MINAS": "Carmo de Minas",
    "CARMO DO CAJURU": "Carmo do Cajuru",
    "CARMO DO PARANAIBA": "Carmo do Paranaíba",
    "CARMO DO RIO CLARO": "Carmo do Rio Claro",
    "CARMOPOLIS DE MINAS": "Carmópolis de Minas",
    "CARNEIRINHO": "Carneirinho",
    "CARRANCAS": "Carrancas",
    "CARVALHOPOLIS": "Carvalhópolis",
    "CARVALHOS": "Carvalhos",
    "CASA GRANDE": "Casa Grande",
    "CASCALHO RICO": "Cascalho Rico",
    "CASSIA": "Cássia",
    "CATAGUASES": "Cataguases",
    "CATAS ALTAS": "Catas Altas",
    "CATAS ALTAS DA NORUEGA": "Catas Altas da Noruega",
    "CATUJI": "Catuji",
    "CATUTI": "Catuti",
    "CAXAMBU": "Caxambu",
    "CEDRO DO ABAETE": "Cedro do Abaeté",
    "CENTRAL DE MINAS": "Central de Minas",
    "CENTRALINA": "Centralina",
    "CHACARA": "Chácara",
    "CHALE": "Chalé",
    "CHAPADA DO NORTE": "Chapada do Norte",
    "CHAPADA GAUCHA": "Chapada Gaúcha",
    "CHIADOR": "Chiador",
    "CIPOTANEA": "Cipotânea",
    "CLARAVAL": "Claraval",
    "CLARO DOS POCOES": "Claro dos Poções",
    "CLAUDIO": "Cláudio",
    "COIMBRA": "Coimbra",
    "COLUNA": "Coluna",
    "COMENDADOR GOMES": "Comendador Gomes",
    "COMERCINHO": "Comercinho",
    "CONCEICAO DA APARECIDA": "Conceição da Aparecida",
    "CONCEICAO DA BARRA DE MINAS": "Conceição da Barra de Minas",
    "CONCEICAO DAS ALAGOAS": "Conceição das Alagoas",
    "CONCEICAO DAS PEDRAS": "Conceição das Pedras",
    "CONCEICAO DE IPANEMA": "Conceição de Ipanema",
    "CONCEICAO DO MATO DENTRO": "Conceição do Mato Dentro",
    "CONCEICAO DO PARA": "Conceição do Pará",
    "CONCEICAO DO RIO VERDE": "Conceição do Rio Verde",
    "CONCEICAO DOS OUROS": "Conceição dos Ouros",
    "CONEGO MARINHO": "Cônego Marinho",
    "CONFINS": "Confins",
    "CONGONHAL": "Congonhal",
    "CONGONHAS": "Congonhas",
    "CONGONHAS DO NORTE": "Congonhas do Norte",
    "CONQUISTA": "Conquista",
    "CONSELHEIRO LAFAIETE": "Conselheiro Lafaiete",
    "CONSELHEIRO PENA": "Conselheiro Pena",
    "CONSOLACAO": "Consolação",
    "CONTAGEM": "Contagem",
    "COQUEIRAL": "Coqueiral",
    "CORACAO DE JESUS": "Coração de Jesus",
    "CORDISBURGO": "Cordisburgo",
    "CORDISLANDIA": "Cordislândia",
    "CORINTO": "Corinto",
    "COROACI": "Coroaci",
    "COROMANDEL": "Coromandel",
    "CORONEL FABRICIANO": "Coronel Fabriciano",
    "CORONEL MURTA": "Coronel Murta",
    "CORONEL PACHECO": "Coronel Pacheco",
    "CORONEL XAVIER CHAVES": "Coronel Xavier Chaves",
    "CORREGO DANTA": "Córrego Danta",
    "CORREGO DO BOM JESUS": "Córrego do Bom Jesus",
    "CORREGO FUNDO": "Córrego Fundo",
    "CORREGO NOVO": "Córrego Novo",
    "COUTO DE MAGALHAES DE MINAS": "Couto de Magalhães de Minas",
    "CRISOLITA": "Crisólita",
    "CRISTAIS": "Cristais",
    "CRISTALIA": "Cristália",
    "CRISTIANO OTONI": "Cristiano Otoni",
    "CRISTINA": "Cristina",
    "CRUCILANDIA": "Crucilândia",
    "CRUZEIRO DA FORTALEZA": "Cruzeiro da Fortaleza",
    "CRUZILIA": "Cruzília",
    "CUPARAQUE": "Cuparaque",
    "CURRAL DE DENTRO": "Curral de Dentro",
    "CURVELO": "Curvelo",
    "DATAS": "Datas",
    "DELFIM MOREIRA": "Delfim Moreira",
    "DELFINOPOLIS": "Delfinópolis",
    "DELTA": "Delta",
    "DESCOBERTO": "Descoberto",
    "DESTERRO DE ENTRE-RIOS": "Desterro de Entre Rios",
    "DESTERRO DO MELO": "Desterro do Melo",
    "DIAMANTINA": "Diamantina",
    "DIOGO DE VASCONCELOS": "Diogo de Vasconcelos",
    "DIONISIO": "Dionísio",
    "DIVINESIA": "Divinésia",
    "DIVINO": "Divino",
    "DIVINO DAS LARANJEIRAS": "Divino das Laranjeiras",
    "DIVINOLANDIA DE MINAS": "Divinolândia de Minas",
    "DIVINOPOLIS": "Divinópolis",
    "DIVISA ALEGRE": "Divisa Alegre",
    "DIVISA NOVA": "Divisa Nova",
    "DIVISOPOLIS": "Divisópolis",
    "DOM BOSCO": "Dom Bosco",
    "DOM CAVATI": "Dom Cavati",
    "DOM JOAQUIM": "Dom Joaquim",
    "DOM SILVERIO": "Dom Silvério",
    "DOM VICOSO": "Dom Viçoso",
    "DONA EUSEBIA": "Dona Euzébia",
    "DORES DE CAMPOS": "Dores de Campos",
    "DORES DE GUANHAES": "Dores de Guanhães",
    "DORES DO INDAIA": "Dores do Indaiá",
    "DORES DO TURVO": "Dores do Turvo",
    "DORESOPOLIS": "Doresópolis",
    "DOURADOQUARA": "Douradoquara",
    "DURANDE": "Durandé",
    "ELOI MENDES": "Elói Mendes",
    "ENGENHEIRO CALDAS": "Engenheiro Caldas",
    "ENGENHEIRO NAVARRO": "Engenheiro Navarro",
    "ENTRE-FOLHAS": "Entre Folhas",
    "ENTRE-RIOS DE MINAS": "Entre Rios de Minas",
    "ERVALIA": "Ervália",
    "ESMERALDAS": "Esmeraldas",
    "ESPERA FELIZ": "Espera Feliz",
    "ESPINOSA": "Espinosa",
    "ESPIRITO SANTO DO DOURADO": "Espírito Santo do Dourado",
    "ESTIVA": "Estiva",
    "ESTRELA DO INDAIA": "Estrela Dalva",
    "ESTRELA DO SUL": "Estrela do Indaiá",
    "ESTRELA-DALVA": "Estrela do Sul",
    "EUGENOPOLIS": "Eugenópolis",
    "EWBANK DA CAMARA": "Ewbank da Câmara",
    "EXTREMA": "Extrema",
    "FAMA": "Fama",
    "FARIA LEMOS": "Faria Lemos",
    "FELICIO DOS SANTOS": "Felício dos Santos",
    "FELISBURGO": "Felisburgo",
    "FELIXLANDIA": "Felixlândia",
    "FERNANDES TOURINHO": "Fernandes Tourinho",
    "FERROS": "Ferros",
    "FERVEDOURO": "Fervedouro",
    "FLORESTAL": "Florestal",
    "FORMIGA": "Formiga",
    "FORMOSO": "Formoso",
    "FORTALEZA DE MINAS": "Fortaleza de Minas",
    "FORTUNA DE MINAS": "Fortuna de Minas",
    "FRANCISCO BADARO": "Francisco Badaró",
    "FRANCISCO DUMONT": "Francisco Dumont",
    "FRANCISCO SA": "Francisco Sá",
    "FRANCISCOPOLIS": "Franciscópolis",
    "FREI GASPAR": "Frei Gaspar",
    "FREI INOCENCIO": "Frei Inocêncio",
    "FREI LAGONEGRO": "Frei Lagonegro",
    "FRONTEIRA": "Fronteira",
    "FRONTEIRA DOS VALES": "Fronteira dos Vales",
    "FRUTA DE LEITE": "Fruta de Leite",
    "FRUTAL": "Frutal",
    "FUNILANDIA": "Funilândia",
    "GALILEIA": "Galiléia",
    "GAMELEIRAS": "Gameleiras",
    "GLAUCILANDIA": "Glaucilândia",
    "GOIABEIRA": "Goiabeira",
    "GOIANA": "Goianá",
    "GONCALVES": "Gonçalves",
    "GONZAGA": "Gonzaga",
    "GOUVEIA": "Gouveia",
    "GOVERNADOR VALADARES": "Governador Valadares",
    "GRAO-MOGOL": "Grão Mogol",
    "GRUPIARA": "Grupiara",
    "GUANHAES": "Guanhães",
    "GUAPE": "Guapé",
    "GUARACIABA": "Guaraciaba",
    "GUARACIAMA": "Guaraciama",
    "GUARANESIA": "Guaranésia",
    "GUARANI": "Guarani",
    "GUARARA": "Guarará",
    "GUARDA-MOR": "Guarda-Mor",
    "GUAXUPE": "Guaxupé",
    "GUIDOVAL": "Guidoval",
    "GUIMARANIA": "Guimarânia",
    "GUIRICEMA": "Guiricema",
    "GURINHATA": "Gurinhatã",
    "HELIODORA": "Heliodora",
    "IAPU": "Iapu",
    "IBERTIOGA": "Ibertioga",
    "IBIA": "Ibiá",
    "IBIAI": "Ibiaí",
    "IBIRACATU": "Ibiracatu",
    "IBIRACI": "Ibiraci",
    "IBIRITE": "Ibirité",
    "IBITIURA DE MINAS": "Ibitiúra de Minas",
    "IBITURUNA": "Ibituruna",
    "ICARAI DE MINAS": "Icaraí de Minas",
    "IGARAPE": "Igarapé",
    "IGARATINGA": "Igaratinga",
    "IGUATAMA": "Iguatama",
    "IJACI": "Ijaci",
    "ILICINEA": "Ilicínea",
    "IMBE DE MINAS": "Imbé de Minas",
    "INCONFIDENTES": "Inconfidentes",
    "INDAIABIRA": "Indaiabira",
    "INDIANOPOLIS": "Indianópolis",
    "INGAI": "Ingaí",
    "INHAPIM": "Inhapim",
    "INHAUMA": "Inhaúma",
    "INIMUTABA": "Inimutaba",
    "IPABA": "Ipaba",
    "IPANEMA": "Ipanema",
    "IPATINGA": "Ipatinga",
    "IPIACU": "Ipiaçu",
    "IPUIUNA": "Ipuiúna",
    "IRAI DE MINAS": "Iraí de Minas",
    "ITABIRA": "Itabira",
    "ITABIRINHA": "Itabirinha",
    "ITABIRITO": "Itabirito",
    "ITACAMBIRA": "Itacambira",
    "ITACARAMBI": "Itacarambi",
    "ITAGUARA": "Itaguara",
    "ITAIPE": "Itaipé",
    "ITAJUBA": "Itajubá",
    "ITAMARANDIBA": "Itamarandiba",
    "ITAMARATI DE MINAS": "Itamarati de Minas",
    "ITAMBACURI": "Itambacuri",
    "ITAMBE DO MATO DENTRO": "Itambé do Mato Dentro",
    "ITAMOGI": "Itamogi",
    "ITAMONTE": "Itamonte",
    "ITANHANDU": "Itanhandu",
    "ITANHOMI": "Itanhomi",
    "ITAOBIM": "Itaobim",
    "ITAPAGIPE": "Itapagipe",
    "ITAPECERICA": "Itapecerica",
    "ITAPEVA": "Itapeva",
    "ITATIAIUCU": "Itatiaiuçu",
    "ITAU DE MINAS": "Itaú de Minas",
    "ITAUNA": "Itaúna",
    "ITAVERAVA": "Itaverava",
    "ITINGA": "Itinga",
    "ITUETA": "Itueta",
    "ITUIUTABA": "Ituiutaba",
    "ITUMIRIM": "Itumirim",
    "ITURAMA": "Iturama",
    "ITUTINGA": "Itutinga",
    "JABOTICATUBAS": "Jaboticatubas",
    "JACINTO": "Jacinto",
    "JACUI": "Jacuí",
    "JACUTINGA": "Jacutinga",
    "JAGUARACU": "Jaguaraçu",
    "JAIBA": "Jaíba",
    "JAMPRUCA": "Jampruca",
    "JANAUBA": "Janaúba",
    "JANUARIA": "Januária",
    "JAPARAIBA": "Japaraíba",
    "JAPONVAR": "Japonvar",
    "JECEABA": "Jeceaba",
    "JENIPAPO DE MINAS": "Jenipapo de Minas",
    "JEQUERI": "Jequeri",
    "JEQUITAI": "Jequitaí",
    "JEQUITIBA": "Jequitibá",
    "JEQUITINHONHA": "Jequitinhonha",
    "JESUANIA": "Jesuânia",
    "JOAIMA": "Joaíma",
    "JOANESIA": "Joanésia",
    "JOAO MONLEVADE": "João Monlevade",
    "JOAO PINHEIRO": "João Pinheiro",
    "JOAQUIM FELICIO": "Joaquim Felício",
    "JORDANIA": "Jordânia",
    "JOSE GONCALVES DE MINAS": "José Gonçalves de Minas",
    "JOSE RAYDAN": "José Raydan",
    "JOSENOPOLIS": "Josenópolis",
    "JUATUBA": "Juatuba",
    "JUIZ DE FORA": "Juiz de Fora",
    "JURAMENTO": "Juramento",
    "JURUAIA": "Juruaia",
    "JUVENILIA": "Juvenília",
    "LADAINHA": "Ladainha",
    "LAGAMAR": "Lagamar",
    "LAGOA DA PRATA": "Lagoa da Prata",
    "LAGOA DOS PATOS": "Lagoa dos Patos",
    "LAGOA DOURADA": "Lagoa Dourada",
    "LAGOA FORMOSA": "Lagoa Formosa",
    "LAGOA GRANDE": "Lagoa Grande",
    "LAGOA SANTA": "Lagoa Santa",
    "LAJINHA": "Lajinha",
    "LAMBARI": "Lambari",
    "LAMIM": "Lamim",
    "LARANJAL": "Laranjal",
    "LASSANCE": "Lassance",
    "LAVRAS": "Lavras",
    "LEANDRO FERREIRA": "Leandro Ferreira",
    "LEME DO PRADO": "Leme do Prado",
    "LEOPOLDINA": "Leopoldina",
    "LIBERDADE": "Liberdade",
    "LIMA DUARTE": "Lima Duarte",
    "LIMEIRA DO OESTE": "Limeira do Oeste",
    "LONTRA": "Lontra",
    "LUISBURGO": "Luisburgo",
    "LUISLANDIA": "Luislândia",
    "LUMINARIAS": "Luminárias",
    "LUZ": "Luz",
    "MACHACALIS": "Machacalis",
    "MACHADO": "Machado",
    "MADRE DE DEUS DE MINAS": "Madre de Deus de Minas",
    "MALACACHETA": "Malacacheta",
    "MAMONAS": "Mamonas",
    "MANGA": "Manga",
    "MANHUACU": "Manhuaçu",
    "MANHUMIRIM": "Manhumirim",
    "MANTENA": "Mantena",
    "MAR DE ESPANHA": "Mar de Espanha",
    "MARAVILHAS": "Maravilhas",
    "MARIA DA FE": "Maria da Fé",
    "MARIANA": "Mariana",
    "MARILAC": "Marilac",
    "MARIO CAMPOS": "Mário Campos",
    "MARIPA DE MINAS": "Maripá de Minas",
    "MARLIERIA": "Marliéria",
    "MARMELOPOLIS": "Marmelópolis",
    "MARTINHO CAMPOS": "Martinho Campos",
    "MARTINS SOARES": "Martins Soares",
    "MATA VERDE": "Mata Verde",
    "MATERLANDIA": "Materlândia",
    "MATEUS LEME": "Mateus Leme",
    "MATHIAS LOBATO": "Mathias Lobato",
    "MATIAS BARBOSA": "Matias Barbosa",
    "MATIAS CARDOSO": "Matias Cardoso",
    "MATIPO": "Matipó",
    "MATO VERDE": "Mato Verde",
    "MATOZINHOS": "Matozinhos",
    "MATUTINA": "Matutina",
    "MEDEIROS": "Medeiros",
    "MEDINA": "Medina",
    "MENDES PIMENTEL": "Mendes Pimentel",
    "MERCES": "Mercês",
    "MESQUITA": "Mesquita",
    "MINAS NOVAS": "Minas Novas",
    "MINDURI": "Minduri",
    "MIRABELA": "Mirabela",
    "MIRADOURO": "Miradouro",
    "MIRAI": "Miraí",
    "MIRAVANIA": "Miravânia",
    "MOEDA": "Moeda",
    "MOEMA": "Moema",
    "MONJOLOS": "Monjolos",
    "MONSENHOR PAULO": "Monsenhor Paulo",
    "MONTALVANIA": "Montalvânia",
    "MONTE ALEGRE DE MINAS": "Monte Alegre de Minas",
    "MONTE AZUL": "Monte Azul",
    "MONTE BELO": "Monte Belo",
    "MONTE CARMELO": "Monte Carmelo",
    "MONTE FORMOSO": "Monte Formoso",
    "MONTE SANTO DE MINAS": "Monte Santo de Minas",
    "MONTE SIAO": "Monte Sião",
    "MONTES CLAROS": "Montes Claros",
    "MONTEZUMA": "Montezuma",
    "MORADA NOVA DE MINAS": "Morada Nova de Minas",
    "MORRO DA GARCA": "Morro da Garça",
    "MORRO DO PILAR": "Morro do Pilar",
    "MUNHOZ": "Munhoz",
    "MURIAE": "Muriaé",
    "MUTUM": "Mutum",
    "MUZAMBINHO": "Muzambinho",
    "NACIP RAYDAN": "Nacip Raydan",
    "NANUQUE": "Nanuque",
    "NAQUE": "Naque",
    "NATALANDIA": "Natalândia",
    "NATERCIA": "Natércia",
    "NAZARENO": "Nazareno",
    "NEPOMUCENO": "Nepomuceno",
    "NINHEIRA": "Ninheira",
    "NOVA BELEM": "Nova Belém",
    "NOVA ERA": "Nova Era",
    "NOVA LIMA": "Nova Lima",
    "NOVA MODICA": "Nova Módica",
    "NOVA PONTE": "Nova Ponte",
    "NOVA PORTEIRINHA": "Nova Porteirinha",
    "NOVA RESENDE": "Nova Resende",
    "NOVA SERRANA": "Nova Serrana",
    "NOVA UNIAO": "Nova União",
    "NOVO CRUZEIRO": "Novo Cruzeiro",
    "NOVO ORIENTE DE MINAS": "Novo Oriente de Minas",
    "NOVORIZONTE": "Novorizonte",
    "OLARIA": "Olaria",
    "OLHOS-D'AGUA": "Olhos-d'Água",
    "OLIMPIO NORONHA": "Olímpio Noronha",
    "OLIVEIRA": "Oliveira",
    "OLIVEIRA FORTES": "Oliveira Fortes",
    "ONCA DO PITANGUI": "Onça de Pitangui",
    "ORATORIOS": "Oratórios",
    "ORIZANIA": "Orizânia",
    "OURO BRANCO": "Ouro Branco",
    "OURO FINO": "Ouro Fino",
    "OURO PRETO": "Ouro Preto",
    "OURO VERDE DE MINAS": "Ouro Verde de Minas",
    "PADRE CARVALHO": "Padre Carvalho",
    "PADRE PARAISO": "Padre Paraíso",
    "PAI PEDRO": "Pai Pedro",
    "PAINEIRAS": "Paineiras",
    "PAINS": "Pains",
    "PAIVA": "Paiva",
    "PALMA": "Palma",
    "PALMOPOLIS": "Palmópolis",
    "PAPAGAIOS": "Papagaios",
    "PARA DE MINAS": "Pará de Minas",
    "PARACATU": "Paracatu",
    "PARAGUACU": "Paraguaçu",
    "PARAISOPOLIS": "Paraisópolis",
    "PARAOPEBA": "Paraopeba",
    "PASSA QUATRO": "Passa Quatro",
    "PASSA TEMPO": "Passa Tempo",
    "PASSA VINTE": "Passa Vinte",
    "PASSABEM": "Passabém",
    "PASSOS": "Passos",
    "PATIS": "Patis",
    "PATOS DE MINAS": "Patos de Minas",
    "PATROCINIO": "Patrocínio",
    "PATROCINIO DO MURIAE": "Patrocínio do Muriaé",
    "PAULA CANDIDO": "Paula Cândido",
    "PAULISTAS": "Paulistas",
    "PAVAO": "Pavão",
    "PECANHA": "Peçanha",
    "PEDRA AZUL": "Pedra Azul",
    "PEDRA BONITA": "Pedra Bonita",
    "PEDRA DO ANTA": "Pedra do Anta",
    "PEDRA DO INDAIA": "Pedra do Indaiá",
    "PEDRA DOURADA": "Pedra Dourada",
    "PEDRALVA": "Pedralva",
    "PEDRAS DE MARIA DA CRUZ": "Pedras de Maria da Cruz",
    "PEDRINOPOLIS": "Pedrinópolis",
    "PEDRO LEOPOLDO": "Pedro Leopoldo",
    "PEDRO TEIXEIRA": "Pedro Teixeira",
    "PEQUERI": "Pequeri",
    "PEQUI": "Pequi",
    "PERDIGAO": "Perdigão",
    "PERDIZES": "Perdizes",
    "PERDOES": "Perdões",
    "PERIQUITO": "Periquito",
    "PESCADOR": "Pescador",
    "PIAU": "Piau",
    "PIEDADE DE CARATINGA": "Piedade de Caratinga",
    "PIEDADE DE PONTE NOVA": "Piedade de Ponte Nova",
    "PIEDADE DO RIO GRANDE": "Piedade do Rio Grande",
    "PIEDADE DOS GERAIS": "Piedade dos Gerais",
    "PIMENTA": "Pimenta",
    "PINGO D'AGUA": "Pingo-d'Água",
    "PINTOPOLIS": "Pintópolis",
    "PIRACEMA": "Piracema",
    "PIRAJUBA": "Pirajuba",
    "PIRANGA": "Piranga",
    "PIRANGUCU": "Piranguçu",
    "PIRANGUINHO": "Piranguinho",
    "PIRAPETINGA": "Pirapetinga",
    "PIRAPORA": "Pirapora",
    "PIRAUBA": "Piraúba",
    "PITANGUI": "Pitangui",
    "PIUMHI": "Piumhi",
    "PLANURA": "Planura",
    "POCO FUNDO": "Poço Fundo",
    "POCOS DE CALDAS": "Poços de Caldas",
    "POCRANE": "Pocrane",
    "POMPEU": "Pompéu",
    "PONTE NOVA": "Ponte Nova",
    "PONTO CHIQUE": "Ponto Chique",
    "PONTO DOS VOLANTES": "Ponto dos Volantes",
    "PORTEIRINHA": "Porteirinha",
    "PORTO FIRME": "Porto Firme",
    "POTE": "Poté",
    "POUSO ALEGRE": "Pouso Alegre",
    "POUSO ALTO": "Pouso Alto",
    "PRADOS": "Prados",
    "PRATA": "Prata",
    "PRATAPOLIS": "Pratápolis",
    "PRATINHA": "Pratinha",
    "PRESIDENTE BERNARDES": "Presidente Bernardes",
    "PRESIDENTE JUSCELINO": "Presidente Juscelino",
    "PRESIDENTE KUBITSCHEK": "Presidente Kubitschek",
    "PRESIDENTE OLEGARIO": "Presidente Olegário",
    "PRUDENTE DE MORAIS": "Prudente de Morais",
    "QUARTEL GERAL": "Quartel Geral",
    "QUELUZITO": "Queluzito",
    "RAPOSOS": "Raposos",
    "RAUL SOARES": "Raul Soares",
    "RECREIO": "Recreio",
    "REDUTO": "Reduto",
    "RESENDE COSTA": "Resende Costa",
    "RESPLENDOR": "Resplendor",
    "RESSAQUINHA": "Ressaquinha",
    "RIACHINHO": "Riachinho",
    "RIACHO DOS MACHADOS": "Riacho dos Machados",
    "RIBEIRAO DAS NEVES": "Ribeirão das Neves",
    "RIBEIRAO VERMELHO": "Ribeirão Vermelho",
    "RIO ACIMA": "Rio Acima",
    "RIO CASCA": "Rio Casca",
    "RIO DO PRADO": "Rio do Prado",
    "RIO DOCE": "Rio Doce",
    "RIO ESPERA": "Rio Espera",
    "RIO MANSO": "Rio Manso",
    "RIO NOVO": "Rio Novo",
    "RIO PARANAIBA": "Rio Paranaíba",
    "RIO PARDO DE MINAS": "Rio Pardo de Minas",
    "RIO PIRACICABA": "Rio Piracicaba",
    "RIO POMBA": "Rio Pomba",
    "RIO PRETO": "Rio Preto",
    "RIO VERMELHO": "Rio Vermelho",
    "RITAPOLIS": "Ritápolis",
    "ROCHEDO DE MINAS": "Rochedo de Minas",
    "RODEIRO": "Rodeiro",
    "ROMARIA": "Romaria",
    "ROSARIO DA LIMEIRA": "Rosário da Limeira",
    "RUBELITA": "Rubelita",
    "RUBIM": "Rubim",
    "SABARA": "Sabará",
    "SABINOPOLIS": "Sabinópolis",
    "SACRAMENTO": "Sacramento",
    "SALINAS": "Salinas",
    "SALTO DA DIVISA": "Salto da Divisa",
    "SANTA BARBARA": "Santa Bárbara",
    "SANTA BARBARA DO LESTE": "Santa Bárbara do Leste",
    "SANTA BARBARA DO MONTE VERDE": "Santa Bárbara do Monte Verde",
    "SANTA BARBARA DO TUGURIO": "Santa Bárbara do Tugúrio",
    "SANTA CRUZ DE MINAS": "Santa Cruz de Minas",
    "SANTA CRUZ DE SALINAS": "Santa Cruz de Salinas",
    "SANTA CRUZ DO ESCALVADO": "Santa Cruz do Escalvado",
    "SANTA EFIGENIA DE MINAS": "Santa Efigênia de Minas",
    "SANTA FE DE MINAS": "Santa Fé de Minas",
    "SANTA HELENA DE MINAS": "Santa Helena de Minas",
    "SANTA JULIANA": "Santa Juliana",
    "SANTA LUZIA": "Santa Luzia",
    "SANTA MARGARIDA": "Santa Margarida",
    "SANTA MARIA DE ITABIRA": "Santa Maria de Itabira",
    "SANTA MARIA DO SALTO": "Santa Maria do Salto",
    "SANTA MARIA DO SUACUI": "Santa Maria do Suaçuí",
    "SANTA RITA DE CALDAS": "Santa Rita de Caldas",
    "SANTA RITA DE JACUTINGA": "Santa Rita de Ibitipoca",
    "SANTA RITA DE MINAS": "Santa Rita de Jacutinga",
    "SANTA RITA DO IBITIPOCA": "Santa Rita de Minas",
    "SANTA RITA DO ITUETO": "Santa Rita do Itueto",
    "SANTA RITA DO SAPUCAI": "Santa Rita do Sapucaí",
    "SANTA ROSA DA SERRA": "Santa Rosa da Serra",
    "SANTA VITORIA": "Santa Vitória",
    "SANTANA DA VARGEM": "Santana da Vargem",
    "SANTANA DE CATAGUASES": "Santana de Cataguases",
    "SANTANA DE PIRAPAMA": "Santana de Pirapama",
    "SANTANA DO DESERTO": "Santana do Deserto",
    "SANTANA DO GARAMBEU": "Santana do Garambéu",
    "SANTANA DO JACARE": "Santana do Jacaré",
    "SANTANA DO MANHUACU": "Santana do Manhuaçu",
    "SANTANA DO PARAISO": "Santana do Paraíso",
    "SANTANA DO RIACHO": "Santana do Riacho",
    "SANTANA DOS MONTES": "Santana dos Montes",
    "SANTO ANTONIO DO AMPARO": "Santo Antônio do Amparo",
    "SANTO ANTONIO DO AVENTUREIRO": "Santo Antônio do Aventureiro",
    "SANTO ANTONIO DO GRAMA": "Santo Antônio do Grama",
    "SANTO ANTONIO DO ITAMBE": "Santo Antônio do Itambé",
    "SANTO ANTONIO DO JACINTO": "Santo Antônio do Jacinto",
    "SANTO ANTONIO DO MONTE": "Santo Antônio do Monte",
    "SANTO ANTONIO DO RETIRO": "Santo Antônio do Retiro",
    "SANTO ANTONIO DO RIO ABAIXO": "Santo Antônio do Rio Abaixo",
    "SANTO HIPOLITO": "Santo Hipólito",
    "SANTOS DUMONT": "Santos Dumont",
    "SAO BENTO ABADE": "São Bento Abade",
    "SAO BRAS DO SUACUI": "São Brás do Suaçuí",
    "SAO DOMINGOS DAS DORES": "São Domingos das Dores",
    "SAO DOMINGOS DO PRATA": "São Domingos do Prata",
    "SAO FELIX DE MINAS": "São Félix de Minas",
    "SAO FRANCISCO": "São Francisco",
    "SAO FRANCISCO DE PAULA": "São Francisco de Paula",
    "SAO FRANCISCO DE SALES": "São Francisco de Sales",
    "SAO FRANCISCO DO GLORIA": "São Francisco do Glória",
    "SAO GERALDO": "São Geraldo",
    "SAO GERALDO DA PIEDADE": "São Geraldo da Piedade",
    "SAO GERALDO DO BAIXIO": "São Geraldo do Baixio",
    "SAO GONCALO DO ABAETE": "São Gonçalo do Abaeté",
    "SAO GONCALO DO PARA": "São Gonçalo do Pará",
    "SAO GONCALO DO RIO ABAIXO": "São Gonçalo do Rio Abaixo",
    "SAO GONCALO DO RIO PRETO": "São Gonçalo do Rio Preto",
    "SAO GONCALO DO SAPUCAI": "São Gonçalo do Sapucaí",
    "SAO GOTARDO": "São Gotardo",
    "SAO JOAO BATISTA DO GLORIA": "São João Batista do Glória",
    "SAO JOAO DA LAGOA": "São João da Lagoa",
    "SAO JOAO DA MATA": "São João da Mata",
    "SAO JOAO DA PONTE": "São João da Ponte",
    "SAO JOAO DAS MISSOES": "São João das Missões",
    "SAO JOAO DEL REI": "São João del Rei",
    "SAO JOAO DO MANHUACU": "São João do Manhuaçu",
    "SAO JOAO DO MANTENINHA": "São João do Manteninha",
    "SAO JOAO DO ORIENTE": "São João do Oriente",
    "SAO JOAO DO PACUI": "São João do Pacuí",
    "SAO JOAO DO PARAISO": "São João do Paraíso",
    "SAO JOAO EVANGELISTA": "São João Evangelista",
    "SAO JOAO NEPOMUCENO": "São João Nepomuceno",
    "SAO JOAQUIM DE BICAS": "São Joaquim de Bicas",
    "SAO JOSE DA BARRA": "São José da Barra",
    "SAO JOSE DA LAPA": "São José da Lapa",
    "SAO JOSE DA SAFIRA": "São José da Safira",
    "SAO JOSE DA VARGINHA": "São José da Varginha",
    "SAO JOSE DO ALEGRE": "São José do Alegre",
    "SAO JOSE DO DIVINO": "São José do Divino",
    "SAO JOSE DO GOIABAL": "São José do Goiabal",
    "SAO JOSE DO JACURI": "São José do Jacuri",
    "SAO JOSE DO MANTIMENTO": "São José do Mantimento",
    "SAO LOURENCO": "São Lourenço",
    "SAO MIGUEL DO ANTA": "São Miguel do Anta",
    "SAO PEDRO DA UNIAO": "São Pedro da União",
    "SAO PEDRO DO SUACUI": "São Pedro do Suaçuí",
    "SAO PEDRO DOS FERROS": "São Pedro dos Ferros",
    "SAO ROMAO": "São Romão",
    "SAO ROQUE DE MINAS": "São Roque de Minas",
    "SAO SEBASTIAO DA BELA VISTA": "São Sebastião da Bela Vista",
    "SAO SEBASTIAO DA VARGEM ALEGRE": "São Sebastião da Vargem Alegre",
    "SAO SEBASTIAO DO ANTA": "São Sebastião do Anta",
    "SAO SEBASTIAO DO MARANHAO": "São Sebastião do Maranhão",
    "SAO SEBASTIAO DO OESTE": "São Sebastião do Oeste",
    "SAO SEBASTIAO DO PARAISO": "São Sebastião do Paraíso",
    "SAO SEBASTIAO DO RIO PRETO": "São Sebastião do Rio Preto",
    "SAO SEBASTIAO DO RIO VERDE": "São Sebastião do Rio Verde",
    "SAO THOME DAS LETRAS": "São Tiago",
    "SAO TIAGO": "São Tomás de Aquino",
    "SAO TOMAS DE AQUINO": "São Tomé das Letras",
    "SAO VICENTE DE MINAS": "São Vicente de Minas",
    "SAPUCAI-MIRIM": "Sapucaí-Mirim",
    "SARDOA": "Sardoá",
    "SARZEDO": "Sarzedo",
    "SEM-PEIXE": "Sem-Peixe",
    "SENADOR AMARAL": "Senador Amaral",
    "SENADOR CORTES": "Senador Cortes",
    "SENADOR FIRMINO": "Senador Firmino",
    "SENADOR JOSE BENTO": "Senador José Bento",
    "SENADOR MODESTINO GONCALVES": "Senador Modestino Gonçalves",
    "SENHORA DE OLIVEIRA": "Senhora de Oliveira",
    "SENHORA DO PORTO": "Senhora do Porto",
    "SENHORA DOS REMEDIOS": "Senhora dos Remédios",
    "SERICITA": "Sericita",
    "SERITINGA": "Seritinga",
    "SERRA AZUL DE MINAS": "Serra Azul de Minas",
    "SERRA DA SAUDADE": "Serra da Saudade",
    "SERRA DO SALITRE": "Serra do Salitre",
    "SERRA DOS AIMORES": "Serra dos Aimorés",
    "SERRANIA": "Serrania",
    "SERRANOPOLIS DE MINAS": "Serranópolis de Minas",
    "SERRANOS": "Serranos",
    "SERRO": "Serro",
    "SETE LAGOAS": "Sete Lagoas",
    "SETUBINHA": "Setubinha",
    "SILVEIRANIA": "Silveirânia",
    "SILVIANOPOLIS": "Silvianópolis",
    "SIMAO PEREIRA": "Simão Pereira",
    "SIMONESIA": "Simonésia",
    "SOBRALIA": "Sobrália",
    "SOLEDADE DE MINAS": "Soledade de Minas",
    "TABULEIRO": "Tabuleiro",
    "TAIOBEIRAS": "Taiobeiras",
    "TAPARUBA": "Taparuba",
    "TAPIRA": "Tapira",
    "TAPIRAI": "Tapiraí",
    "TAQUARACU DE MINAS": "Taquaraçu de Minas",
    "TARUMIRIM": "Tarumirim",
    "TEIXEIRAS": "Teixeiras",
    "TEOFILO OTONI": "Teófilo Otoni",
    "TIMOTEO": "Timóteo",
    "TIRADENTES": "Tiradentes",
    "TIROS": "Tiros",
    "TOCANTINS": "Tocantins",
    "TOCOS DO MOJI": "Tocos do Moji",
    "TOLEDO": "Toledo",
    "TOMBOS": "Tombos",
    "TRES CORACOES": "Três Corações",
    "TRES MARIAS": "Três Marias",
    "TRES PONTAS": "Três Pontas",
    "TUMIRITINGA": "Tumiritinga",
    "TUPACIGUARA": "Tupaciguara",
    "TURMALINA": "Turmalina",
    "TURVOLANDIA": "Turvolândia",
    "UBA": "Ubá",
    "UBAI": "Ubaí",
    "UBAPORANGA": "Ubaporanga",
    "UBERABA": "Uberaba",
    "UBERLANDIA": "Uberlândia",
    "UMBURATIBA": "Umburatiba",
    "UNAI": "Unaí",
    "UNIAO DE MINAS": "União de Minas",
    "URUANA DE MINAS": "Uruana de Minas",
    "URUCANIA": "Urucânia",
    "URUCUIA": "Urucuia",
    "VARGEM ALEGRE": "Vargem Alegre",
    "VARGEM BONITA": "Vargem Bonita",
    "VARGEM GRANDE DO RIO PARDO": "Vargem Grande do Rio Pardo",
    "VARGINHA": "Varginha",
    "VARJAO DE MINAS": "Varjão de Minas",
    "VARZEA DA PALMA": "Várzea da Palma",
    "VARZELANDIA": "Varzelândia",
    "VAZANTE": "Vazante",
    "VERDELANDIA": "Verdelândia",
    "VEREDINHA": "Veredinha",
    "VERISSIMO": "Veríssimo",
    "VERMELHO NOVO": "Vermelho Novo",
    "VESPASIANO": "Vespasiano",
    "VICOSA": "Viçosa",
    "VIEIRAS": "Vieiras",
    "VIRGEM DA LAPA": "Virgem da Lapa",
    "VIRGINIA": "Virgínia",
    "VIRGINOPOLIS": "Virginópolis",
    "VIRGOLANDIA": "Virgolândia",
    "VISCONDE DO RIO BRANCO": "Visconde do Rio Branco",
    "VOLTA GRANDE": "Volta Grande",
    "WENCESLAU BRAZ": "Wenceslau Braz"
}

# Loop por município
for municipio in lista_municipios:
    municipio_upper = municipio.upper()
    municipio_formatado = dict_municipios.get(
        municipio_upper,
        municipio.title()
    )

    texto = ""
    texto2 = ""
    
    # Parte 1: Comparação 2024 x 2025 (ano completo)
    df_mun = df_cv_final[df_cv_final['municipio'] == municipio_upper]
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
            natureza_formatada = idx.lower().capitalize()
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{natureza_formatada} (passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

        texto += f"{municipio_formatado.upper()}\n"
        texto += f"Em uma análise de Janeiro a Dezembro de 2025 frente ao mesmo período de 2024, o município de {municipio_formatado} "

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
        (df_cv_final['municipio'] == municipio_upper) &
        (df_cv_final['ano fato'].isin([2025, 2026])) &
        (df_cv_final['mes'] == datas.mes_ref)
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
            natureza_formatada = idx.lower().capitalize()
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{natureza_formatada} (passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {datas.mes_ref_nome} de 2026 frente ao mesmo período de 2025, o município de {municipio_formatado} "

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
        caminhos_saida = [
            caminho_saida_paper_municipios,
            caminho_saida_paper_municipios_one_drive
        ]
        
        for caminho_saida in caminhos_saida:
            caminho_txt = os.path.join(
                caminho_saida,
                f"{municipio_formatado.upper()}.txt"
            )
        
            with open(caminho_txt, "w", encoding="utf-8") as f:
                f.write(texto.strip() + "\n\n" + texto2.strip())

        print(f"Relatório gerado para: {municipio_formatado}")
    else:
        print(f"Sem dados para: {municipio_formatado}")
        
# A
# T
# E
# N         A partir daqui, o código exporta os arquivos txt para o paper das RISPs
# Ç
# Ã
# O

# Pastas de destino dos papers de RISP
caminho_saida_paper_risps = (
    f"{paths.paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"RISP"
)

caminho_saida_paper_risps_one_drive = (
    f"{paths.onedrive_paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
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
            natureza_formatada = idx.lower().capitalize()
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            risp25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{natureza_formatada} (passando de {q24} para {q25} {risp25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {risp25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {risp25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

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
        (df_cv_final['mes'] == datas.mes_ref)
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
            natureza_formatada = idx.lower().capitalize()
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            risp26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{natureza_formatada} (passando de {q25} para {q26} {risp26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {risp26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {risp26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {datas.mes_ref_nome} de 2026 frente ao mesmo período de 2025, a {risp.title()} "

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
            caminhos_saida = [
                caminho_saida_paper_risps,
                caminho_saida_paper_risps_one_drive
            ]
            
            for caminho_saida in caminhos_saida:
                caminho_txt = os.path.join(
                    caminho_saida,
                    f"{risp_upper.upper()}.txt"
                )
            
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
    f"{paths.paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
    f"Mesorregiões"
)

caminho_saida_paper_mesorregioes_one_drive = (
    f"{paths.onedrive_paper_dir}/"
    f"{datas.ano_ref}/"
    f"{datas.mes_ref_num_str} - {datas.mes_ref_nome}/"
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
            natureza_formatada = idx.lower().capitalize()
            q24, q25 = int(row[2024]), int(row[2025])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg25_txt = "registro" if q25 == 1 else "registros"

            if q24 == 0 and q25 > 0:
                frase = f"{natureza_formatada} (passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q24} para {q25} {reg25_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

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
        (df_cv_final['mes'] == datas.mes_ref)
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
            natureza_formatada = idx.lower().capitalize()
            q25, q26 = int(row[2025]), int(row[2026])
            perc = row['Percentual']
            perc_fmt = f"{perc:.2f}".replace(".", ",")
            reg26_txt = "registro" if q26 == 1 else "registros"

            if q25 == 0 and q26 > 0:
                frase = f"{natureza_formatada} (passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc > 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                aumento.append(frase)
            elif perc < 0:
                frase = f"{natureza_formatada} ({perc_fmt}%, passando de {q25} para {q26} {reg26_txt})"
                reducao.append(frase)
            else:
                sem_variacao.append(natureza_formatada)

        texto2 += f"\nJá em uma análise dos meses de Janeiro a {datas.mes_ref_nome} de 2026 frente ao mesmo período de 2025, a mesorregião de {regiao.title()} "

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
                caminhos_saida = [
                    caminho_saida_paper_mesorregioes,
                    caminho_saida_paper_mesorregioes_one_drive
                ]
                
                for caminho_saida in caminhos_saida:
                    caminho_txt = os.path.join(
                        caminho_saida,
                        f"{regiao_upper.upper()}.txt"
                    )
                
                    with open(caminho_txt, "w", encoding="utf-8") as f:
                        f.write(texto.strip() + "\n\n" + texto2.strip())
        
                print(f"Relatório gerado para: {regiao}")
            else:
                print(f"Sem dados para: {regiao}")