import os
import subprocess
import sys

def executar():

    print("\n=== Iniciando tabela matriz ===")

    PASTA = os.path.dirname(__file__)
    este_arquivo = os.path.basename(__file__)

    # ⭐ RAIZ DO PROJETO
    RAIZ = os.path.abspath(
        os.path.join(PASTA, "..", "..")
    )

    arquivos = sorted(os.listdir(PASTA))

    for arquivo in arquivos:

        if (
            arquivo.endswith(".py")
            and arquivo != este_arquivo
            and arquivo != "__init__.py"
        ):

            caminho_script = os.path.join(PASTA, arquivo)

            print(f"\n--- Executando Tabela Matriz: {arquivo} ---")

            # ⭐ ADICIONA RAIZ AO PYTHONPATH
            env = os.environ.copy()
            env["PYTHONPATH"] = RAIZ

            subprocess.run(
                [sys.executable, caminho_script],
                check=True,
                cwd=RAIZ,
                env=env   # ⭐ ESSENCIAL
            )

    print("\nOk - Tabela Matriz finalizada!")