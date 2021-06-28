import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'busca_proposta_situacao_invalida.sql')
    lista_processos = []
    for x in db.consulta_sql(sql, index_col='tipo_registro').to_dict('records'):
        lista_processos.append(x)
        resultado = f"A proposta do fornecedor {x['chave_dsk4']} para o item {x['chave_dsk5']} está com situação incorreta"
        print(resultado)
        arquivo = open(
           f"packages/tools/compras/relatorios_incosistencias/entidade{params_exec['entidade']}_proposta_processo.txt", 'a')
        arquivo.writelines(f'{resultado} \n')

    if input('Realizar a correção ? ').upper() in 'SIMYES1':
        print('\n:: Realizando a correção das propostas do fornecedores com situção incorreta')
        for x in lista_processos:
            sql = f"""
                CALL bethadba.pg_habilitartriggers('off');
                UPDATE compras.participantes SET situacao = 1, ordem_clas = 0 
                WHERE i_entidades = {x['chave_dsk1']}
                    AND i_ano_proc = {x['chave_dsk2']}
                    AND i_processo = {x['chave_dsk3']}
                    AND i_credores = {x['chave_dsk4']}
                    AND i_item = {x['chave_dsk5']}
            """
            db.execute_sql(sql)
