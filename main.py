from config.datas import (
    ano_ref,
    mes_ref
)

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
        print(e)

        raise  # interrompe execução


def main():

    print("=== EXECUÇÃO DO PROJETO DADOS ABERTOS ===")

    print(f"Ano referência: {ano_ref}")
    print(f"Mês referência: {mes_ref}")

    executar_etapa("BASES COMPLETAS", executar_bases_completas)

    executar_etapa("BASES AGRUPADAS", executar_bases_agrupadas)

    executar_etapa("BDHC", executar_bdhc)

    executar_etapa("PRODUTIVIDADE", executar_produtividade)

    executar_etapa("TABELA MATRIZ", executar_tabela_matriz)

    print("\n=== EXECUÇÃO FINALIZADA ===")


if __name__ == "__main__":
    main()