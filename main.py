from config.datas import (
    ano_ref,
    mes_ref
)

# Importar os módulos principais
from codigos.bases_completas.main import executar as executar_bases_completas
from codigos.bases_agrupadas.main import executar as executar_bases_agrupadas
from codigos.BDHC.main import executar as executar_bdhc
from codigos.produtividade.main import executar as executar_produtividade
from codigos.tabela_matriz.main import executar as executar_tabela_matriz


def executar_etapa(nome, funcao):

    print(f"\n========== {nome} ==========")

    try:

        funcao()

        print(f"✅ {nome} FINALIZADO")

    except Exception as e:

        print(f"❌ ERRO EM {nome}")

        raise e


def main():

    print("=== EXECUÇÃO DO PROJETO DADOS ABERTOS ===")

    print(f"Ano referência: {ano_ref}")
    print(f"Mês referência: {mes_ref}")

    # 🔧 CONTROLE DAS ETAPAS
    RODAR_BASES_COMPLETAS = True
    RODAR_BASES_AGRUPADAS = True
    RODAR_BDHC = True
    RODAR_PRODUTIVIDADE = True
    RODAR_TABELA_MATRIZ = True

    if RODAR_BASES_COMPLETAS:
        executar_etapa(
            "BASES COMPLETAS",
            executar_bases_completas
        )

    if RODAR_BASES_AGRUPADAS:
        executar_etapa(
            "BASES AGRUPADAS",
            executar_bases_agrupadas
        )

    if RODAR_BDHC:
        executar_etapa(
            "BDHC",
            executar_bdhc
        )

    if RODAR_PRODUTIVIDADE:
        executar_etapa(
            "PRODUTIVIDADE",
            executar_produtividade
        )

    if RODAR_TABELA_MATRIZ:
        executar_etapa(
            "TABELA MATRIZ",
            executar_tabela_matriz
        )

    print("\n=== EXECUÇÃO FINALIZADA ===")


if __name__ == "__main__":
    main()