from pathlib import Path

# Diretório raiz do projeto
base_dir = Path(__file__).resolve().parent.parent

# Diretórios principais
config_dir = base_dir / "config"
codigos_dir = base_dir / "codigos"
logs_dir = base_dir / "logs"
memorando_dir = base_dir / "memorando_suint"

# Diretórios que poderão ser usados futuramente
input_dir = base_dir / "input"
output_dir = base_dir / "output"
temp_dir = base_dir / "temp"

# Arquivos do projeto
municipios_mg = config_dir / "municipios_mg.xlsx"

for pasta in [logs_dir, input_dir, output_dir, temp_dir]:
    pasta.mkdir(parents=True, exist_ok=True)
    
# Diretório OneDrive
onedrive_dir = (
    Path.home()
    / "OneDrive - CAMG"
)

# Diretório da publicação das bases agrupadas
publicacoes_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "11 - Publicação SESP - Site"
)

# Diretório da publicação das bases completas
completas_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "Bases completas"
)

# Diretório da publicação das bases de produtividade
produtividade_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "08 - Produtividade"
)

# Diretório da publicação das bases de tabela matriz
matriz_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "06 - Monitoramento SIGPLAN"
)

# Diretório downloads
downloads_dir = (
    Path.home()
    / "Downloads"
)

# Caminho bases CTE
grupo_local_imediato = config_dir / "grupo_local_imediato_com_codigo.xlsx"
alvo_corrigido = config_dir / "alvo_corrigido.xlsx"