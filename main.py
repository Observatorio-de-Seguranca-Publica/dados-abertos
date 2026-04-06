from config.datas import (
    ano_ref,
    mes_ref
)

def main():

    print("=== EXECUÇÃO DO PROJETO DADOS ABERTOS ===")

    print(f"Ano referência: {ano_ref}")
    print(f"Mês referência: {mes_ref}")

    # Aqui depois vamos chamar os scripts
    # exemplo:
    # gerar_base_cv()
    # gerar_base_homicidios()


if __name__ == "__main__":
    main()