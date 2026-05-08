from codigos.bases_completas.xlsx.main import executar as executar_xlsx
from codigos.bases_completas.csv.main import executar as executar_csv

def executar():

    print("\n==============================")
    print("INICIANDO BASES COMPLETAS")
    print("==============================")

    # Primeiro XLSX
    executar_xlsx()

    # Depois CSV
    executar_csv()

    print("\nOk - BASES COMPLETAS FINALIZADAS")