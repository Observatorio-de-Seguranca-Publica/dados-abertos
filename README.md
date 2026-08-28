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
O credenciamento Python é o arquivo que contém senha e usuário da Bisp. Aqui neste projeto, ele está referenciado dessa forma:
```python
# Credenciamento Python
credenciais_db = Path.home() / "Downloads" / "Credenciamento Python.txt"
```

Ou seja, ele está indicando para a pasta de Downloads do computador. Dessa forma, se seu arquivo não está armazenado em Downloads, mova-o para lá ou troque o caminho relativo no arquivo paths.py deste repo.

### 4.2 Configuração de datas
As datas utilizadas na execução do projeto são relativas e automáticas, isto é, a cada execução (mensal) o código capta a data corrente e já executa com base nesta data, referenciando o mês anterior. Importante ter atenção na virada de ano, que pode ter alguma quebra não visualizada.

### 4.3 BDHC
Aqui tem um ponto sensível do projeto, o BDHC é gerado manualmente e disponibilizado no canal do Teams. Antes da execução da rotina mensal, é necessário pegar o arquivo disponibilizado, salvá-lo como bdhc.xlsx e colocá-lo na pasta "publicacoes/bdhc" dentro desse repositório.

### 4.4 OneDrive
Outro ponto sensível do projeto são as saídas direto no OneDrive. Pelo projeto, as bases são colocadas diretamente no OneDrive. Para isso acontecer, é necessário que o OneDrive esteja ativado no PC e o canal "DIS_SOSP_SUINT_SEJUSP - DADOS ABERTOS" esteja acoplado no explorador de arquivos do usuário, como uma pasta sincronizada.

Aqui vai um passo a passo de como fazer o setup:
1) Vá até a barra de canais no teams:
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/barra_teams.png

2) Dentro da equipe DIS, selecione "Ver todos os canais":
![equipe_dis](https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/equipe_dis.png)

3) Selecione o canal dados abertos e vá na aba compartilhado:
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/canal_dados_abertos.png

4) Confira se a sincronização do OneDrive está ativa:
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/sincronizacao_2.png

5) É possível verificá-la pelo Teams também:
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/sincronizacao_1.png

6) Pelo teams, selecione "Adicionar atalho ao OneDrive":
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/atalho_onedrive.png

7) No explorador de arquivos, o canal deve aparecer à esquerda dentro de OneDrive:
https://github.com/Observatorio-de-Seguranca-Publica/dados-abertos/blob/main/config/assets/barra_tarefas.png

Casa não apareça, ver se está com outro nome, como Sharepoint, CAMG ou similar.

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
Os arquivos produtos dessa rotina ficam armazenados na pasta "publicacoes". Eles estão destacados no gitignore, portanto, jamais irão subir ao GitHub. Além disso, conforme dito anteriormente, parte dos produtos também vai para o OneDrive. Quando o usuário faz o clone, a pasta com os anos e meses vem vazia e pronta para usos futuros.

Depois disso, é necessário subir os arquivos no Google Drive da Diretoria para publicação pela Ascom.

# todo
Repositório destinado ao armazenamento de códigos utilizados para publicação da rotina de dados abertos no site da Secretaria de Estado de Segurança Pública de Minas Gerais (SEJUSP).


O que devo detalhar: 
- One Drive
- arquivo bat
- agendador de tarefas