warning: in the working copy of 'codigos/bases_agrupadas/agrupado_alvos_roubo.py', LF will be replaced by CRLF the next time Git touches it
warning: in the working copy of 'codigos/bases_agrupadas/agrupado_alvos_roubo_veiculos.py', LF will be replaced by CRLF the next time Git touches it
warning: in the working copy of 'codigos/bases_agrupadas/agrupado_furto.py', LF will be replaced by CRLF the next time Git touches it
[1mdiff --git a/codigos/BDHC/agrupado_registros_homicidio_consumado.py b/codigos/BDHC/agrupado_registros_homicidio_consumado.py[m
[1mindex 6ae2134..dc3e8cc 100644[m
[1m--- a/codigos/BDHC/agrupado_registros_homicidio_consumado.py[m
[1m+++ b/codigos/BDHC/agrupado_registros_homicidio_consumado.py[m
[36m@@ -133,7 +133,7 @@[m [mres = res[["Registros", "Natureza", "Município", "Cód. IBGE", "Mês", "Ano Fat[m
 res = res.sort_values(["Ano Fato", "Mês", "Natureza", "Município"]).reset_index(drop=True)[m
 [m
 # 10. Transforma coluna código IBGE em número[m
[31m-res["Cod. IBGE"] = pd.to_numeric(res["Cod. IBGE"], errors="coerce").astype("Int64")[m
[32m+[m[32mres["Cód. IBGE"] = pd.to_numeric(res["Cód. IBGE"], errors="coerce").astype("Int64")[m
 [m
 # 11. Exportar para Excel[m
 saida = "C:/Users/x15501492/Documents/02 - Publicações/11 - Publicação SESP - Site/2026/02 - Fevereiro/Excel/agrupado_registros_homicidio_consumado.xlsx" [m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_alvos_furto.py b/codigos/bases_agrupadas/agrupado_alvos_furto.py[m
[1mindex 11ebcce..39df359 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_alvos_furto.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_alvos_furto.py[m
[36m@@ -200,6 +200,7 @@[m [mcaminho_excel = ([m
     f"Excel/"[m
     f"agrupado_alvos_furto.xlsx"[m
 )[m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
 [m
 # A[m
 # T[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_alvos_roubo.py b/codigos/bases_agrupadas/agrupado_alvos_roubo.py[m
[1mindex f4498b0..4e35105 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_alvos_roubo.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_alvos_roubo.py[m
[36m@@ -201,6 +201,8 @@[m [mcaminho_excel = ([m
     f"agrupado_alvos_roubo.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_alvos_roubo_veiculos.py b/codigos/bases_agrupadas/agrupado_alvos_roubo_veiculos.py[m
[1mindex 3b1265b..8a826f6 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_alvos_roubo_veiculos.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_alvos_roubo_veiculos.py[m
[36m@@ -180,6 +180,8 @@[m [mcaminho_excel = ([m
     f"agrupado_roubo_veiculos.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_crimes_violentos_12_18.py b/codigos/bases_agrupadas/agrupado_crimes_violentos_12_18.py[m
[1mindex c44a2a5..fd80f1e 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_crimes_violentos_12_18.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_crimes_violentos_12_18.py[m
[36m@@ -213,6 +213,8 @@[m [mcaminho_excel = ([m
     f"12_18_agrupado_crimes_violentos.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_crimes_violentos_19_24.py b/codigos/bases_agrupadas/agrupado_crimes_violentos_19_24.py[m
[1mindex 182092f..cdbe373 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_crimes_violentos_19_24.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_crimes_violentos_19_24.py[m
[36m@@ -215,6 +215,8 @@[m [mcaminho_excel = ([m
     f"19_24_agrupado_crimes_violentos.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_crimes_violentos_25_26.py b/codigos/bases_agrupadas/agrupado_crimes_violentos_25_26.py[m
[1mindex 4d08da7..cd4a508 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_crimes_violentos_25_26.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_crimes_violentos_25_26.py[m
[36m@@ -215,6 +215,8 @@[m [mcaminho_excel = ([m
     f"25_26_agrupado_crimes_violentos.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_furto.py b/codigos/bases_agrupadas/agrupado_furto.py[m
[1mindex 8b57d49..57c977e 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_furto.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_furto.py[m
[36m@@ -169,6 +169,8 @@[m [mcaminho_excel = ([m
     f"agrupado_furto.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/agrupado_lesao_corporal.py b/codigos/bases_agrupadas/agrupado_lesao_corporal.py[m
[1mindex 71296e7..6f31822 100644[m
[1m--- a/codigos/bases_agrupadas/agrupado_lesao_corporal.py[m
[1m+++ b/codigos/bases_agrupadas/agrupado_lesao_corporal.py[m
[36m@@ -169,6 +169,8 @@[m [mcaminho_excel = ([m
     f"agrupado_lesao_corporal.xlsx"[m
 )[m
 [m
[32m+[m[32mdf.to_excel(caminho_excel, index=False)[m
[32m+[m
 # A[m
 # T[m
 # E[m
[1mdiff --git a/codigos/bases_agrupadas/main.py b/codigos/bases_agrupadas/main.py[m
[1mindex 7605b08..f3a0ec7 100644[m
[1m--- a/codigos/bases_agrupadas/main.py[m
[1m+++ b/codigos/bases_agrupadas/main.py[m
[36m@@ -11,7 +11,7 @@[m [mdef executar():[m
 [m
     # ⭐ RAIZ DO PROJETO[m
     RAIZ = os.path.abspath([m
[31m-        os.path.join(PASTA, "..", "..", "..")[m
[32m+[m[32m        os.path.join(PASTA, "..", "..")[m
     )[m
 [m
     arquivos = sorted(os.listdir(PASTA))[m
[1mdiff --git a/codigos/produtividade/main.py b/codigos/produtividade/main.py[m
[1mindex 286d0f8..b8190a6 100644[m
[1m--- a/codigos/produtividade/main.py[m
[1m+++ b/codigos/produtividade/main.py[m
[36m@@ -11,7 +11,7 @@[m [mdef executar():[m
 [m
     # ⭐ RAIZ DO PROJETO[m
     RAIZ = os.path.abspath([m
[31m-        os.path.join(PASTA, "..", "..", "..")[m
[32m+[m[32m        os.path.join(PASTA, "..", "..")[m
     )[m
 [m
     arquivos = sorted(os.listdir(PASTA))[m
[1mdiff --git a/codigos/tabela_matriz/main.py b/codigos/tabela_matriz/main.py[m
[1mindex e804311..6aa5b02 100644[m
[1m--- a/codigos/tabela_matriz/main.py[m
[1m+++ b/codigos/tabela_matriz/main.py[m
[36m@@ -11,7 +11,7 @@[m [mdef executar():[m
 [m
     # ⭐ RAIZ DO PROJETO[m
     RAIZ = os.path.abspath([m
[31m-        os.path.join(PASTA, "..", "..", "..")[m
[32m+[m[32m        os.path.join(PASTA, "..", "..")[m
     )[m
 [m
     arquivos = sorted(os.listdir(PASTA))[m
[1mdiff --git a/main.py b/main.py[m
[1mindex 10d3136..bb23f23 100644[m
[1m--- a/main.py[m
[1m+++ b/main.py[m
[36m@@ -3,6 +3,7 @@[m [mfrom config.datas import ([m
     mes_ref[m
 )[m
 [m
[32m+[m[32m# Importar os módulos principais[m
 from codigos.bases_completas.main import executar as executar_bases_completas[m
 from codigos.bases_agrupadas.main import executar as executar_bases_agrupadas[m
 from codigos.BDHC.main import executar as executar_bdhc[m
[36m@@ -23,9 +24,8 @@[m [mdef executar_etapa(nome, funcao):[m
     except Exception as e:[m
 [m
         print(f"❌ ERRO EM {nome}")[m
[31m-        print(e)[m
 [m
[31m-        raise  # interrompe execução[m
[32m+[m[32m        raise e[m
 [m
 [m
 def main():[m
[36m@@ -35,15 +35,42 @@[m [mdef main():[m
     print(f"Ano referência: {ano_ref}")[m
     print(f"Mês referência: {mes_ref}")[m
 [m
[31m-    executar_etapa("BASES COMPLETAS", executar_bases_completas)[m
[31m-[m
[31m-    executar_etapa("BASES AGRUPADAS", executar_bases_agrupadas)[m
[31m-[m
[31m-    executar_etapa("BDHC", executar_bdhc)[m
[31m-[m
[31m-    executar_etapa("PRODUTIVIDADE", executar_produtividade)[m
[31m-[m
[31m-    executar_etapa("TABELA MATRIZ", executar_tabela_matriz)[m
[32m+[m[32m    # 🔧 CONTROLE DAS ETAPAS[m
[32m+[m[32m    RODAR_BASES_COMPLETAS = False[m
[32m+[m[32m    RODAR_BASES_AGRUPADAS = False[m
[32m+[m[32m    RODAR_BDHC = True[m
[32m+[m[32m    RODAR_PRODUTIVIDADE = True[m
[32m+[m[32m    RODAR_TABELA_MATRIZ = True[m
[32m+[m
[32m+[m[32m    if RODAR_BASES_COMPLETAS:[m
[32m+[m[32m        executar_etapa([m
[32m+[m[32m            "BASES COMPLETAS",[m
[32m+[m[32m            executar_bases_completas[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m    if RODAR_BASES_AGRUPADAS:[m
[32m+[m[32m        executar_etapa([m
[32m+[m[32m            "BASES AGRUPADAS",[m
[32m+[m[32m            executar_bases_agrupadas[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m    if RODAR_BDHC:[m
[32m+[m[32m        executar_etapa([m
[32m+[m[32m            "BDHC",[m
[32m+[m[32m            executar_bdhc[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m    if RODAR_PRODUTIVIDADE:[m
[32m+[m[32m        executar_etapa([m
[32m+[m[32m            "PRODUTIVIDADE",[m
[32m+[m[32m            executar_produtividade[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m    if RODAR_TABELA_MATRIZ:[m
[32m+[m[32m        executar_etapa([m
[32m+[m[32m            "TABELA MATRIZ",[m
[32m+[m[32m            executar_tabela_matriz[m
[32m+[m[32m        )[m
 [m
     print("\n=== EXECUÇÃO FINALIZADA ===")[m
 [m
