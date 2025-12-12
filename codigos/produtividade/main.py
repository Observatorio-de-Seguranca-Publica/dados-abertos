import os
import subprocess

# Caminho da pasta onde estão os scripts
#PASTA = "C:/Users/x15501492/Documents/02 - Publicações/Códigos/Agrupados"
PASTA = "C:/Users/x15501492/Documents/Sejusp/DIS/Dados abertos/Automatização/Produtividade"
este_arquivo = os.path.basename(__file__)  # nome do script atual


for arquivo in os.listdir(PASTA):
    if arquivo.endswith(".py") and arquivo != este_arquivo:
        caminho_script = os.path.join(PASTA, arquivo)
        print(f"\n--- Executando: {arquivo} ---")
        subprocess.run(["python", caminho_script], check=True)

print("\n✅ Finalizado com sucesso!")