import bth.db_connector as db


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    sql = db.get_consulta(params_exec, f'analisa_fornecedor.sql')
    correcao = str(input('Realizar correção ?'))
    print(db.consulta_sql(sql, index_col='tipo_registro').to_dict('records'))
    for x in db.consulta_sql(sql, index_col='tipo_registro').to_dict('records'):
        mostra = False
        resultado = f"Fornecedor: {x['chave_dsk2']}"
        if x['nome'] == '':
            resultado += ', Nome'
            mostra = True
        if x['cpfCnpj'] == '':
            resultado += ', CPF/CNPJ'
            mostra = True

        if x['estado'] == '':
            resultado += ', Estado'
            mostra = True
        if x['unidadeFederacao'] == '':
            resultado += ', Sigla Estado'
            mostra = True
        if x['cod_cidade'] == '0':
            resultado += ', Cidade'
            mostra = True
        if x['municipio'] == '':
            resultado += ', Municipio'
            mostra = True
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                                          update compras.credores 
                                          set cidade = 'Migracao'
                                          where i_credores = {x['chave_dsk2']}
                                              and i_entidades = {int(params_exec['entidade'])}""")
        if x['bairro'] == '':
            resultado += ', Bairro'
            mostra = True
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                                          update compras.credores 
                                          set bairro = 'centro'
                                          where i_credores = {x['chave_dsk2']}
                                              and i_entidades = {int(params_exec['entidade'])}""")

        if x['logradouro'] == '' or str(x['logradouro'])[0] == ',':
            resultado += ', Logradouro'
            mostra = True
            if correcao.upper() in ('SYSIMYES'):
                db.execute_sql(f"""
                               update compras.credores 
                               set endereco = 'Rua Geral, 00'
                               where i_credores = {x['chave_dsk2']}
                                   and i_entidades = {int(params_exec['entidade'])}""")
        # if x['id_gerado'] == '':
        #     resultado += ', Id Gerado'
        #     mostra = True

        if mostra:
            print(resultado)

        arquivo = open(f"packages/tools/compras/relatorios_incosistencias/entidade{params_exec['entidade']}_fornecedores.txt", 'a')
        arquivo.writelines(f'{resultado} \n')


def iniciar_processo_correcao(params_exec):
    sql = """
        UPDATE * FROM COMPRAS.CREDORES
    """
