# Verifica o cadastro de fornecedores, executa o mesmo caminho que o arqjob para verificar os registros.
# Corrige o cadastro com as informações que serão solicitadas.
# Para executar é necessario informar corretamente a conexão do banco de dados no Settings.


import bth.db_connector as db

def iniciar_processo_busca(params_exec, *args, **kwargs):
    correcao = str(input('Realizar correção ?'))
    cep = None
    bairro = None
    rua = None
    if correcao.upper() in ('SYSIMYES'):
        cep = str(input('Qual CEP utilizar ?'))
    if correcao.upper() in ('SYSIMYES'):
        rua = str(input('Qual Rua utilizar ?'))
    if correcao.upper() in ('SYSIMYES'):
        bairro = str(input('Qual Bairro utilizar ?'))
    conn = db.conectar(params_exec)
    # db.execute_sql(f"insert into sapo.conexoes_usr (i_conexoes,usuario,ano_exerc,i_entidades,i_pontos,i_mov_diaria,i_sistema,i_plano_contas,eh_4320,ctrl_trigger) on existing skip select @@spid as i_conexoes, current user usuario, 2021 exerc ,{params_exec['entidade']} entid, null i_pontos,null i_mov_diaria, 1 sistema, 4,3,0 from dummy", conn)
    db.execute_sql(f"call bethadba.pg_setoption('fire_triggers','off');", conn=conn, params_exec=params_exec)
    db.execute_sql(f"call bethadba.pg_setoption('wait_for_commit','on');", conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp1.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp2.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp3.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp4.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp5.sql'), conn=conn, params_exec=params_exec)

    for x in db.consulta_sql(db.get_consulta(params_exec,'valida-endereco-tab-temp6.sql'), conn=conn, params_exec=params_exec, index_col='tipo_registro').to_dict('records'):
        mostra = False
        arquivo = open('packages/tools/contabil/relatorios/comprovantes.txt', 'a')
        resultado = f"Registro {x['registro']} :"
        if str(x['registro'])[0:9] == 'do Credor':
            credor = str(x['registro'][10:])
            if x['cep_rua'] == None:
                resultado += ', cep'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
                if correcao.upper() in ('SYSIMYES'):
                    db.execute_sql(f"""update bethadba.pessoas_enderecos set cep = '{cep}'  where
                                    i_pessoas = (select e.i_pessoas from sapo.credores wh JOIN bethadba.pessoas_enderecos as e ON (wh.i_pessoas = e.i_pessoas)
                                    where wh.i_credores = {credor} and wh.i_entidades = {params_exec['entidade']})""", conn=conn, params_exec=params_exec)
                    db.execute_sql(f"""update bethadba.pessoas_enderecos a set nome_rua = (select first wh.endereco from sapo.credores wh where wh.i_credores = 101280) 
                                    where a.i_pessoas = (select first i_pessoas from sapo.credores wh where wh.i_credores = {credor})""", conn=conn, params_exec=params_exec)
                    print('Corrigido..')
            # if x['numero'] == None:
            #     resultado += ', numero'
            #     mostra = True
            #     arquivo.writelines(f'{resultado} \n')
            if x['nome_rua'] == None:
                resultado += ', nome da rua'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
                if correcao.upper() in ('SYSIMYES'):
                    db.execute_sql(f"""update bethadba.pessoas_enderecos set nome_rua = '{rua}'  where
                                    i_pessoas = (select e.i_pessoas from sapo.credores wh JOIN bethadba.pessoas_enderecos as e ON (wh.i_pessoas = e.i_pessoas)
                                    where wh.i_credores = {credor} and wh.i_entidades = {params_exec['entidade']})""", conn=conn, params_exec=params_exec)
                    print('Corrigido..')
            if x['nome_bairro'] == None:
                resultado += ', nome do bairro'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
                if correcao.upper() in ('SYSIMYES'):
                    db.execute_sql(f"""update bethadba.pessoas_enderecos set nome_bairro = '{bairro}'  where
                                    i_pessoas = (select e.i_pessoas from sapo.credores wh JOIN bethadba.pessoas_enderecos as e ON (wh.i_pessoas = e.i_pessoas)
                                    where wh.i_credores = {credor} and wh.i_entidades = {params_exec['entidade']})""", conn=conn, params_exec=params_exec)
                    print('Corrigido..')
            if x['nome_cidade'] == None:
                resultado += ', nome da cidade'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
            if x['nome_estado'] == None:
                resultado += ', nome do estado'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
            if x['nome_pais'] == None:
                resultado += ', nome do pais'
                mostra = True
                arquivo.writelines(f'{resultado} \n')
            if mostra:
                print(resultado)
    # db.execute_sql('drop table #tabTempEnderecos', conn)
    db.execute_sql('drop table #tabTempMovDividas', conn=conn, params_exec=params_exec)
    db.execute_sql('drop table #tabTempMovCredores', conn=conn, params_exec=params_exec)
    db.execute_sql('drop table #tabTempCredores', conn=conn, params_exec=params_exec)
    db.execute_sql('drop table #tabTempContasBancarias', conn=conn, params_exec=params_exec)
    db.execute_sql(f"call bethadba.pg_setoption('fire_triggers','on');", conn=conn, params_exec=params_exec)
    db.execute_sql(f"call bethadba.pg_setoption('wait_for_commit','off');", conn=conn, params_exec=params_exec)
    db.execute_sql('drop table #TempTiposLogradouros', conn=conn, params_exec=params_exec)