import bth.db_connector as db
import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'busca_comissao_licitacao.sql')
    correcao = str(input('Realizar correção ?'))
    grava = str(input('Gravar resultado ?'))
    for x in db.consulta_sql(sql, index_col='tipo_registro').to_dict('records'):
        resultado = f"Comissão {x['chave_dsk2']} da entidade {x['chave_dsk1']} : "
        print(x)
        if x['ato'] == '':
            resultado += "Ato, "

        if x['dataExoneracao'] == '':
            resultado += "Data de Exoneração, "
            if correcao.upper() in ('SYSIMYES'):
                if x['dataExpiracao'] != '':
                    db.execute_sql(f"""
                               CALL bethadba.pg_habilitartriggers('off');
                               update compras.responsaveis set data_exonera = data_expira 
                               where  i_responsavel = {x['chave_dsk2']}
                                   and i_entidades = {x['chave_dsk1']}
                           """)
                elif x['dataCriacao'] != '':
                    db.execute_sql(f"""
                        CALL bethadba.pg_habilitartriggers('off');
                        update compras.responsaveis 
                            set data_exonera = CAST(DATEADD(YEAR,1,data_publ) as date), 
                                data_expira = CAST(DATEADD(YEAR,1,data_publ) 
                        where  i_responsavel = {x['chave_dsk2']}
                            and i_entidades = {x['chave_dsk1']}""")
                else:
                    db.execute_sql(f"""
                                            CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis 
                                                set data_publ = data_cadastro_tce,
                                                    data_exonera = CAST(DATEADD(YEAR,1,data_cadastro_tce) as date),
                                                    data_expira = CAST(DATEADD(YEAR,1,data_cadastro_tce) as date)
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}""")


        if x['dataExpiracao'] == '':
            resultado += "Data de Expiração, "
            if correcao.upper() in ('SYSIMYES'):
                # if x['dataExpiracao']
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                    update compras.responsaveis set data_exonera = data_expira 
                    where  i_responsavel = {x['chave_dsk2']}
                        and i_entidades = {x['chave_dsk1']}
                """)

        if x['dataCriacao'] == '':
            resultado += "Data de Criação, "
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                               CALL bethadba.pg_habilitartriggers('off');
                               update compras.responsaveis set data_publ = data_cadastro_tce 
                               where  i_responsavel = {x['chave_dsk2']}
                                   and i_entidades = {x['chave_dsk1']}
                           """)

        if x['cpf_autoridade'] == '' or x['tamCpfAutoridade'] > 11:
            resultado += "CPF Autoridade, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                    update compras.responsaveis set cpf_titular = '{cpf}' 
                    where  i_responsavel = {x['chave_dsk2']}
                        and i_entidades = {x['chave_dsk1']}
                """)

        if x['cpf_presid'] == '' or x['tamCpf'] > 11:
            resultado += "CPF Presidente, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                update compras.responsaveis set cpf_presid = '{cpf}' 
                                where  i_responsavel = {x['chave_dsk2']}
                                    and i_entidades = {x['chave_dsk1']}
                            """)

        if x['responsavel1'] != '' and len(x['cpf_membro1']) > 11:
            resultado += "CPF Membro 1, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis set cpf_membro1 = '{cpf}' 
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}
                                        """)

        if x['responsavel2'] != '' and len(x['cpf_membro2']) > 11:
            resultado += "CPF Membro 2, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis set cpf_membro2 = '{cpf}' 
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}
                                        """)

        if x['responsavel3'] != '' and len(x['cpf_membro3']) > 11:
            resultado += "CPF Membro 3, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis set cpf_membro3 = '{cpf}' 
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}
                                        """)

        if x['responsavel4'] != '' and len(x['cpf_membro4']) > 11:
                resultado += "CPF Membro 4, "
        if correcao.upper() in ('SYSIMYES'):
            cpf = cloud.geraCpf()
            db.execute_sql(f"""
                CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis set cpf_membro4 = '{cpf}' 
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}
                                        """)

        if x['responsavel5'] != '' and len(x['cpf_membro5']) > 11:
            resultado += "CPF Membro 5, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                            update compras.responsaveis set cpf_membro5 = '{cpf}' 
                                            where  i_responsavel = {x['chave_dsk2']}
                                                and i_entidades = {x['chave_dsk1']}
                                        """)

        if x['responsavel6'] != '' and len(x['cpf_membro6']) > 11:
            resultado += "CPF Membro 6, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                                update compras.responsaveis set cpf_membro6 = '{cpf}'
                                                where  i_responsavel = {x['chave_dsk2']}
                                                    and i_entidades = {x['chave_dsk1']}
                                            """)

        if x['responsavel7'] != '' and len(x['cpf_membro7']) > 11:
            resultado += "CPF Membro 7, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                                update compras.responsaveis set cpf_membro7 = '{cpf}'
                                                where  i_responsavel = {x['chave_dsk2']}
                                                    and i_entidades = {x['chave_dsk1']}
                                            """)

        if x['responsavel8'] != '' and len(x['cpf_membro8']) > 11:
            resultado += "CPF Membro 8, "
            if correcao.upper() in ('SYSIMYES'):
                cpf = cloud.geraCpf()
                db.execute_sql(f"""
                    CALL bethadba.pg_habilitartriggers('off');
                                                update compras.responsaveis set cpf_membro8 = '{cpf}' 
                                                where  i_responsavel = {x['chave_dsk2']}
                                                    and i_entidades = {x['chave_dsk1']}
                                            """)

        if grava .upper() in ('SYSIMYES'):
            arquivo = open(
            f"packages/tools/compras/relatorios_incosistencias/entidade{params_exec['entidade']}_comissoes.txt", 'a')
            arquivo.writelines(f'{resultado} \n')