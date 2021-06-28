import bth.db_connector as db

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql_exercicios = db.get_consulta(params_exec, 'config_organograma.sql')
    for exerc in db.consulta_sql(sql_exercicios, index_col='chave_dsk1').to_dict('records'):
        print(exerc)
