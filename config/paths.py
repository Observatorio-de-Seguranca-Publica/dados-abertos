from pathlib import Path

# Diretório raiz do projeto
base_dir = Path(__file__).resolve().parent.parent

# Diretórios principais
config_dir = base_dir / "config"
codigos_dir = base_dir / "codigos"
logs_dir = base_dir / "logs"
memorando_dir = base_dir / "memorando_suint"

# Arquivos do projeto
municipios_mg = config_dir / "municipios_mg.xlsx"
grupo_local_imediato = config_dir / "grupo_local_imediato_com_codigo.xlsx"
alvo_corrigido = config_dir / "alvo_corrigido.xlsx"

# Credenciamento Python
credenciais_db = Path.home() / "Downloads" / "Credenciamento Python.txt"

# Diretório local da publicação
publicacoes_dir = (
    base_dir
    / "publicacoes"
)

# Diretório local da publicação das bases agrupadas
agrupadas_dir = (
    base_dir
    / "publicacoes"
    / "agrupadas"
)

# Diretório local da publicação das bases completas
completas_dir = (
    base_dir
    / "publicacoes"
    / "completas"
)

# Diretório local da publicação das bases de produtividade
produtividade_dir = (
    base_dir
    / "publicacoes"
    / "produtividade"
)

# Diretório local da publicação das bases de tabela matriz
matriz_dir = (
    base_dir
    / "publicacoes"
    / "imagens"
)

# Diretório local da publicação do bdhc
bdhc_dir = (
    base_dir
    / "publicacoes"
    / "bdhc"
)

# Caminho bases CTE
grupo_local_imediato = config_dir / "grupo_local_imediato.xlsx"
alvo_corrigido = config_dir / "alvo_corrigido.xlsx"

# Diretório local da publicação dos papers
paper_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "Paper"
)

# Diretórios OneDrive
onedrive_dir = (
    Path.home()
    / "OneDrive - CAMG"
)

onedrive_publicacao_dir = (
    onedrive_dir
    / "DIS_SOSP_SUINT_SEJUSP - DADOS ABERTOS"
    / "Arquivos da Publicação"
)

onedrive_imagens_dir = (
    onedrive_publicacao_dir
    / "Arquivos para Ascom"
    / "Imagens de monitoramento"
)

onedrive_produtividade_dir = (
    onedrive_publicacao_dir
    / "Arquivos para Ascom"
    / "Produtividade policial"
)

onedrive_paper_dir = (
    onedrive_publicacao_dir
    / "Paper"
)

onedrive_agrupadas_dir = (
    onedrive_publicacao_dir
    / "Arquivos Site"
    / "Quantitativo de eventos por município e risp"
)

onedrive_completas_externo_dir = (
    onedrive_publicacao_dir
    / "Bases de Dados Completas"
    / "Uso Externo"
)

onedrive_completas_interno_dir = (
    onedrive_publicacao_dir
    / "Bases de Dados Completas"
    / "Uso interno"
)
