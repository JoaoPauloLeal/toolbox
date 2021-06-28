import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano):
    sql = db.get_consulta(params_exec, f'busca_propostas_faltando.sql')

    for x in db.consulta_sql(sql, index_col='entidade').to_dict('records'):
        print(f"{x['processo']}/{x['ano']} o fornecedor {x['credor']} está com {x['total']} propostas faltando")
        arquivo = open(
            f"packages/tools/compras/relatorios_incosistencias/entidade{params_exec['entidade']}_fornecedores_sem_proposta.txt", 'a')
        arquivo.writelines(f"{x['processo']}/{x['ano']} o fornecedor {x['credor']} está com {x['total']} propostas faltando\n")