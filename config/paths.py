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
    
# Diretório das publicações agrupadas
publicacoes_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "11 - Publicação SESP - Site"
)

completas_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "Bases completas"
)