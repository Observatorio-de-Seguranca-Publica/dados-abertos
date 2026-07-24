import pandas as pd
from impala.dbapi import connect
from config.paths import credenciais_db
from config.database import (
    get_conn_and_cursor,
    executa_query_retorna_df,
    tabelas,
    bancos_de_dados,
)