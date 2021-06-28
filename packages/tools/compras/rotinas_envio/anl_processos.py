import bth.db_connector as db

tipo_registro = 'anl_processo_correcao'


def iniciar_processo_busca(param_exec, ano):
    entidade_dsk = str(input('Entidade do desktop : '))
    sql = f'''SELECT  i_anl_processo as id , i_anl_processo, i_processo, i_ano_proc
              FROM compras.anl_processos ap
              WHERE i_responsaveis_atos IS NULL
                AND i_entidades = {entidade_dsk}
              ORDER BY i_ano_proc, i_processo, i_anl_processo'''

    # x = db.consulta_sql(sql, index_col='i_anl_processo')
    # print(x)
    for x in db.consulta_sql(sql, index_col='id').to_dict('records'):
        print(f"Anulação {x['i_anl_processo']} do processo {x['i_processo']}/{x['i_ano_proc']}")


    correcao = str(input('Realizar a correção automática ? '))

    if correcao in 'sSyYSIMsimYESyes1':
        query = db.get_consulta(param_exec, f'{tipo_registro}.sql')
        db.execute_sql(query)
    elif correcao in 'nNnaoNAOnãoNÃO0':
        return 'x'
