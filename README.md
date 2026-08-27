# Dados Abertos

## 1. Sobre o projeto
Esse repositório é destinado à automatização da publicação da rotina de dados abertos no site da Secretaria de Estado de Segurança Pública de Minas Gerais (SEJUSP).

## 2. Pré-requisitos
- Python versão 3.13.12 ou superior
- Conta no GitHub com participação na organização do Observatório
- Acesso à Bisp

## 3. Instalação
### 3.1 Clone
Para clonar este repositório, utilize o comando a seguir no terminal (bash)
```
git clone https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos.git
```

### 3.2 Ambiente virtual
Para criar e ativar o ambiente virtual, utilize os seguintes comandos:

Criação
```
python -m venv venv
```

Ativação
```
source venv/Scripts/Activate
```

Desativação, caso precise
```
deactivate
```

Obs: o nome do ambiente virtual pode ficar como "venv" ou ".venv", veja como está o seu. Seguindo esse passo a passo, fica somente "venv"

### 3.3 Dependências
As dependências do projeto estão detalhadas no arquivo "requirements.txt".

Para instalá-las, use o código a seguir:
```
pip install -r requirements.txt
```

## 4. Configuração
### 4.1 Credenciamento Python

### 4.2 Configuração de datas
As datas utilizadas na execução do projeto são relativas e automáticas, isto é, a cada execução (mensal) o código capta a data corrente e já executa com base nesta data, referenciando o mês anterior. Importante ter atenção na virada de ano, que pode ter alguma quebra não visualizada.

### 4.3 BDHC
Aqui tem um ponto sensível do projeto, o BDHC é gerado manualmente e disponibilizado no canal do Teams. Antes da execução da rotina mensal, é necessário pegar o arquivo disponibilizado, salvá-lo como bdhc.xlsx e colocá-lo na pasta "publicacoes/bdhc" dentro desse repositório.

### 4.4 OneDrive
Outro ponto sensível do projeto são as saídas direto no OneDrive. Pelo projeto, as bases são colocadas diretamente no OneDrive. Para isso acontecer, é necessário que o OneDrive esteja ativado no PC e o canal "DIS_SOSP_SUINT_SEJUSP - DADOS ABERTOS" esteja acoplado no explorador de arquivos do usuário, como uma pasta sincronizada.

## 5. Estrutura do projeto
Esse projeto está dividido em 5 pastas:
- codigos: é onde os códigos estão armazenados, o coração do projeto, divididos por pastas referentes aos produtos gerados (bases agrupadas, completas, etc).
- config: possui códigos e arquivos importantes para o projeto, como o setup de acesso à Bisp, arquivo de datas, caminhos e  planilhas de apoio.
- logs: verificação das rodagens.
- memorando_suint: arquivo jupyter para elaboração do memo advindo dos dados abertos.
- publicacoes: pasta de saída para os produtos, as bases ficam armazenadas nela. Os arquivos não vão para o GitHub.
- arquivo main.py centralizado para execução do projeto.
- execucao.bat para execução automática da rotina.

## 6. Execução
### 6.1 Execução manual
Para executar o projeto manualmente (o que pode ser necessário em testes, primeiras rodagens ou reexecução em caso de erro), o projeto precisa estar com o ambiente virtual (venv) ativo, localizado na pasta raiz (code/observatorio/dados-abertos (main)) e deve ser executado o seguinte comando:
```
python -m main
```
Além disso, é possível controlar as etapas no arquivo main. (Ex: bases completas rodou, mas travou no BDHC, é possível colocar false na etapa 1 para evitar que rode tudo de novo)

### 6.2 Arquivo BAT
O arquivo BAT é utilizado para execução automática da rotina. Quando ele é disparado, o main.py é chamado e inicia a execução.

### 6.3 Agendador de Tarefas
A execução do projeto pode ser agendada para o início de cada mês por meio do Agendador de Tarefas do Windows.

## 7. Arquivos gerados

## 8. Solução de problemas

# todo
Repositório destinado ao armazenamento de códigos utilizados para publicação da rotina de dados abertos no site da Secretaria de Estado de Segurança Pública de Minas Gerais (SEJUSP).

O que devo resolver:
caminho relativo do credenciamento
caminho relativo do bat

O que devo detalhar: 
- Credenciamento Python
- One Drive
- arquivo bat
- agendador de tarefas
- saídas/publicacao

