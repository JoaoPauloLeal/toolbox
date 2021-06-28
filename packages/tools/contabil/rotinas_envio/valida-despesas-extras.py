import bth.cloud_connector as cloud
import bth.db_connector as db


def isNaN(num):
    return num != num


def iniciar_processo_busca(params_exec, *args, **kwargs):
    corrigir = str(input('Realizar correção ?'))
    if corrigir.upper() in ('SYSIMYES'):
        fornecedor = str(input('Utilizar qual Fornecedor como Padrão ?'))
    conn = db.conectar(params_exec)
    db.execute_sql(f"insert into sapo.conexoes_usr (i_conexoes,usuario,ano_exerc,i_entidades,i_pontos,i_mov_diaria,i_sistema,i_plano_contas,eh_4320,ctrl_trigger) on existing skip select @@spid as i_conexoes, current user usuario, 2021 exerc ,{params_exec['entidade']} entid, null i_pontos,null i_mov_diaria, 1 sistema, 4,3,0 from dummy",
        conn=conn, params_exec=params_exec)
    for x in db.consulta_sql(db.get_consulta(params_exec,'valida-despesas-extras.sql'), params_exec=params_exec, index_col='i_entidades').to_dict('records'):
        print(f"Despesa Extra {x['i_despexs']} sem Credor")
        if x['ee'] != 0:
            if corrigir.upper() in ('SYSIMYES'):
                db.execute_sql(f"update sapo.despexs set i_credores = {x['ee']} where i_entidades = {x['entidades']} and i_despexs = {x['i_despexs']} and ano_exerc = {x['ano_exerc']}",conn=conn, params_exec=params_exec)
        else:
            print(f"update sapo.despexs set i_credores = {fornecedor} where i_entidades = {int(x['entidades'])} and i_despexs = {x['i_despexs']} and ano_exerc = {x['ano_exerc']}")
            if corrigir.upper() in ('SYSIMYES'):
                db.execute_sql(f"update sapo.despexs set i_credores = {fornecedor} where i_entidades = {int(x['entidades'])} and i_despexs = {x['i_despexs']} and ano_exerc = {x['ano_exerc']}", conn=conn, params_exec=params_exec)