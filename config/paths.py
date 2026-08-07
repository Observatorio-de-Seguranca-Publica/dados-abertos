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

# Credenciamento Python
credenciais_db = Path.home() / "Downloads" / "Credenciamento Python.txt"

# Diretóri local o da publicação das bases agrupadas
publicacoes_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "11 - Publicação SESP - Site"
)

# Diretório local da publicação das bases completas
completas_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "Bases completas"
)

# Diretório local da publicação das bases de produtividade
produtividade_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "08 - Produtividade"
)

# Diretório local da publicação das bases de tabela matriz
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
grupo_local_imediato = config_dir / "grupo_local_imediato.xlsx"
alvo_corrigido = config_dir / "alvo_corrigido.xlsx"

# Diretório local da publicação dos papers
paper_dir = (
    Path.home()
    / "Documents"
    / "02 - Publicações"
    / "Paper"
)
