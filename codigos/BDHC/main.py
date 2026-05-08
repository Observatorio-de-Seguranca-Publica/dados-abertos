import os
import subprocess
import sys

def executar():

    print("\n==============================")
    print("INICIANDO BDHC")
    print("==============================")

    PASTA = os.path.dirname(__file__)

    # ⭐ RAIZ DO PROJETO
    RAIZ = os.path.abspath(
        os.path.join(PASTA, "..", "..")
    )

    # ⭐ PYTHONPATH (ESSENCIAL)
    env = os.environ.copy()
    env["PYTHONPATH"] = RAIZ

    # Ordem manual dos scripts
    scripts = [

        "bdhc_automatico_vitimas.py",

        "bdhc_automatico_registros.py",

        "agrupado_vitimas_homicidio_consumado.py",

        "agrupado_registros_homicidio_consumado.py",

        "juncao_bdhc_cv.py",

        "juncao_bdhc_cv_csv.py"
    ]

    for script in scripts:

        caminho_script = os.path.join(PASTA, script)

        # Verifica se arquivo existe
        if not os.path.exists(caminho_script):

            raise FileNotFoundError(
                f"Script não encontrado: {caminho_script}"
            )

        print(f"\n--- Executando BDHC: {script} ---")

        subprocess.run(
            [sys.executable, caminho_script],
            check=True,
            cwd=RAIZ,
            env=env   # ⭐ ESSENCIAL
        )

    print("\nOk - BDHC FINALIZADO")