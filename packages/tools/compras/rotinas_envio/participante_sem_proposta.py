import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    processos = []
    itens = []
    ls_propostas = []
    sql = """SELECT DISTINCT tipo_registro, i_chave_dsk1 entidade, 
                i_chave_dsk2 ano_processo, 
                i_chave_dsk3 processo
             FROM bethadba.controle_migracao_registro
             WHERE tipo_registro like 'processos-administrativo' and i_chave_dsk1 = 1 and i_chave_dsk2 = 2020 and i_chave_dsk3 in (19,20)
             ORDER by i_chave_dsk2 desc, i_chave_dsk3"""
    for x in db.consulta_sql(sql, index_col='tipo_registro').to_dict('records'):
        processos.append(x)

    # print(processos)
    for pos_prox, qtd in enumerate(processos):
        sql_busca_qtd_item = f"""SELECT i_item, i_material FROM compras.itens_processo ip
                                WHERE i_ano_proc = {qtd['ano_processo']}
                                    and i_processo = {qtd['processo']}
                                    and i_entidades = {qtd['entidade']}
                                    ORDER BY  i_item DESC"""

        for pos, item in enumerate(db.consulta_sql(sql_busca_qtd_item, index_col='i_material').to_dict('records')):
            print(qtd)
            print(item)
            itens[pos] = item
        # print(itens)
        # processos
        # itens.clear()

    # print(processos)
    #
    #
    # for credor in processos:
    #     sql_busca_propostas = f"""SELECT i_credores credor, i_item item_proposta, qtde_cotada from compras.participantes p
    #                                         WHERE i_ano_proc = {credor['ano_processo']}
    #                                             and i_processo = {credor['processo']}
    #                                             and i_entidades = {credor['entidade']}
    #                                         GROUP BY i_item ,i_credores, qtde_cotada
    #                                         ORDER BY i_credores , i_item """
    #     for prop in db.consulta_sql(sql_busca_propostas, index_col='qtde_cotada').to_dict('records'):
    #         if str(prop['item_proposta']) not in credor['itens']:
    #             print(prop['item_proposta'])
    #
    #         else:
    #             print(f"Credor {prop['credor']} não possui proposta para o item {str(credor['itens']).replace(str(prop['item_proposta']), '')}")
    #             # print(f"Credor {prop['credor']} não possui proposta para o item {str(credor['itens']).replace(str(prop['item_proposta']), '')}")
    #             print('não ta')







    # for credor in processos:
    #     sql_busca_propostas = f"""SELECT i_credores credor, i_item item_proposta, qtde_cotada from compras.participantes p
    #                                 WHERE i_ano_proc = {credor['ano_processo']}
    #                                     and i_processo = {credor['processo']}
    #                                     and i_entidades = {credor['entidade']}
    #                                 GROUP BY i_item ,i_credores, qtde_cotada
    #                                 ORDER BY i_credores , i_item """
    #     propostas = db.consulta_sql(sql_busca_propostas, index_col='qtde_cotada').to_dict('records')
    #
        # for prop in db.consulta_sql(sql_busca_propostas, index_col='qtde_cotada').to_dict('records'):
        #     print('Item da Proposta : ', str(prop['item_proposta']))
        #     print('itens do Proccesso : ', credor['itens'])




