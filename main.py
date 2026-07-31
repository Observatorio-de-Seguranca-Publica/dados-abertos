import time
from datetime import datetime
from config import datas

# Importar os módulos principais
from codigos.bases_completas.main import executar as executar_bases_completas
from codigos.bases_agrupadas.main import executar as executar_bases_agrupadas
from codigos.BDHC.main import executar as executar_bdhc
from codigos.produtividade.main import executar as executar_produtividade
from codigos.tabela_matriz.main import executar as executar_tabela_matriz
from codigos.paper.main import executar as executar_paper



def executar_etapa(nome, funcao):

    print(
        f"\n[{datetime.now():%d/%m/%Y %H:%M:%S}] "
        f"========== {nome} =========="
    )

    inicio = time.time()

    try:

        funcao()

        duracao = round(time.time() - inicio, 2)

        print(
            f"[{datetime.now():%d/%m/%Y %H:%M:%S}] "
            f"Ok - {nome} FINALIZADO "
            f"({duracao}s)"
        )

    except Exception as e:

        duracao = round(time.time() - inicio, 2)
        
        print(
            f"[{datetime.now():%d/%m/%Y %H:%M:%S}] "
            f" ERRO EM {nome} "
            f"({duracao}s)"
        )

        raise


def main():

    print(
        f"\n[{datetime.now():%d/%m/%Y %H:%M:%S}] "
        f"=== EXECUÇÃO DO PROJETO DADOS ABERTOS ==="
    )

    print(f"Ano referência: {datas.ano_ref}")
    print(f"Mês referência: {datas.mes_ref}")

    # 🔧 CONTROLE DAS ETAPAS
    RODAR_BASES_COMPLETAS = True
    RODAR_BASES_AGRUPADAS = True
    RODAR_BDHC = True
    RODAR_PRODUTIVIDADE = True
    RODAR_TABELA_MATRIZ = True
    RODAR_PAPER = True

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
    if RODAR_PAPER:
        executar_etapa(
            "PAPER",
            executar_paper
        )

    print(
        f"\n[{datetime.now():%d/%m/%Y %H:%M:%S}] "
        f"=== EXECUÇÃO FINALIZADA ==="
    )


if __name__ == "__main__":
    main()