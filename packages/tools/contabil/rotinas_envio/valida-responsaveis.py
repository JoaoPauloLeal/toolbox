import bth.db_connector as db
import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, *args, **kwargs):
    conn = db.conectar(params_exec)

    db.execute_sql(f"insert into sapo.conexoes_usr (i_conexoes,usuario,ano_exerc,i_entidades,i_pontos,i_mov_diaria,i_sistema,i_plano_contas,eh_4320,ctrl_trigger) on existing skip select @@spid as i_conexoes, current user usuario, 2021 exerc ,{params_exec['entidade']} entid, null i_pontos,null i_mov_diaria, 1 sistema, 4,3,0 from dummy", conn=conn, params_exec=params_exec)
    db.execute_sql(f"CALL bethadba.pg_habilitartriggers('off');", conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp1.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp2.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-endereco-tab-temp4.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-responsaveis-temp1.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-responsaveis-temp2.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-responsaveis-temp3.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-responsaveis-temp4.sql'), conn=conn, params_exec=params_exec)
    db.execute_sql(db.get_consulta(params_exec, 'valida-responsaveis-temp5.sql'), conn=conn, params_exec=params_exec)

    correcao = str(input('Realizar correção ? '))
    bairro = None
    logradouro = None
    cep = None
    if correcao.upper() in ('SYSIMYES'):
        logradouro = str(input('Qual logradouro padrão ? '))
    if correcao.upper() in ('SYSIMYES'):
        bairro = str(input('Qual bairro padrão ? '))
    if correcao.upper() in ('SYSIMYES'):
        cep = str(input('Qual cep padrão ? '))
    for x in db.consulta_sql("""select 2 as rest, isnull(chave_dsk1,'') as chs,
                                             * 
                                        from #tabTempResponsaveis 
                                       where chs is not null""", conn=conn, params_exec=params_exec, index_col='rest').to_dict('records'):
        if x['cpf'] is None:
            print(f"Responsavel {str(x['pessoa'])} sem CPF")
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"update bethadba.pessoas_fisicas set cpf = '{cloud.geraCpf()}' where i_pessoas = {str(x['pessoa'])}", conn=conn, params_exec=params_exec)

    for x in db.consulta_sql("""select idMunicipio, 
                                                   idLogradouro, 
                                                   idBairro,    
                                                   numero,
                                                   responsavel,
                                                   cast(pessoa as varchar),
                                                   isnull(pessoa,'') p,
                                                   pessoa e,
                                                   nome_cidade,
                                                   nome_rua,
                                                   nome_bairro
                                              from #tabTempEnderecos""", conn=conn, params_exec=params_exec, index_col='e').to_dict('records'):
        if x['p'] != 0:
            if db.consulta_sql(f"""select * from bethadba.pessoas_enderecos pe where i_pessoas = {str(x['pessoa'])}""", conn=conn, params_exec=params_exec).get('i_pessoas').count() != 0:
                if x['idBairro'] == '':
                    print(f"Responsavel {str(x['pessoa'])} sem bairro")
                    if correcao.upper() in ('SYSIMYES'):
                        db.execute_sql(f"update bethadba.pessoas_enderecos set nome_bairro = '{bairro}' where i_pessoas = {str(x['pessoa'])}", conn=conn, params_exec=params_exec)
                if x['nome_rua'] == '':
                    print(f"Responsavel {str(x['pessoa'])} sem rua")
            else:
                if x['idLogradouro'] == '':
                    print(f"Responsavel {str(x['pessoa'])} sem logradouro")
                    if correcao.upper() in ('SYSIMYES'):
                        db.execute_sql(f"insert into bethadba.pessoas_enderecos (i_pessoas,tipo_endereco,nome_rua,cep) values ({str(x['pessoa'])},'P','{logradouro}','{cep}')", conn=conn, params_exec=params_exec)