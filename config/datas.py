from datetime import datetime

# Obtém data atual
hoje = datetime.today()

# Variáveis principais
ano_atual = hoje.year
mes_atual = hoje.month
mes_num_str = f"{mes_atual:02d}"

if mes_atual == 1:
    mes_ref = 12
    ano_ref = ano_atual - 1
else:
    mes_ref = mes_atual - 1
    ano_ref = ano_atual

mes_ref_num_str = f"{mes_ref:02d}"

# Dicionário meses
dict_meses = {
    1: ("Janeiro", "Jan"),
    2: ("Fevereiro", "Fev"),
    3: ("Março", "Mar"),
    4: ("Abril", "Abr"),
    5: ("Maio", "Mai"),
    6: ("Junho", "Jun"),
    7: ("Julho", "Jul"),
    8: ("Agosto", "Ago"),
    9: ("Setembro", "Set"),
    10: ("Outubro", "Out"),
    11: ("Novembro", "Nov"),
    12: ("Dezembro", "Dez"),
}

mes_ref_nome, mes_ref_abrev = dict_meses[mes_ref]

if __name__ == "__main__":
    print(ano_ref)
    print(mes_ref)
    print(mes_ref_num_str)
    print(mes_ref_abrev)
    print(mes_ref_nome)