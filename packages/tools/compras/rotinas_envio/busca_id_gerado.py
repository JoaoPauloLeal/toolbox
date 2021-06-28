import bth.db_connector as db
import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    # sql = db.get_consulta(params_exec, f'busca_comissao_licitacao.sql')
    num_entidade_dsk = input('Entidade Desktop : ')
    sql = f"""select id_gerado as id_proc_adm ,
                i_chave_dsk1, 
                i_chave_dsk2 as exercicio
            from bethadba.controle_migracao_registro 
            where tipo_registro = 'processos-administrativo' 
                and i_chave_dsk1 = {num_entidade_dsk}"""
    id_entidade_cloud = db.consulta_sql(f"""select id_gerado, i_chave_dsk1
                                        from bethadba.controle_migracao_registro   
                                        where tipo_registro = 'entidades' 
                                            and i_chave_dsk1 = {num_entidade_dsk}""", index_col='i_chave_dsk1').to_dict('records')
    # print(id_entidade_cloud[0].get('id_gerado'))

    for processos in db.consulta_sql(sql, index_col='i_chave_dsk1').to_dict('records'):
        # print(processos)
        url_consulta = f"https://compras.betha.cloud/compras-services/api/exercicios/{processos['exercicio']}/processos-administrativo/{processos['id_proc_adm']}/entidades"
        print(url_consulta)
        resp = cloud.get_service_layer(params_exec, url=url_consulta)
        print(resp)