import bth.db_connector as db
import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, *args, **kwargs):
    # Execução
    conta = 0
    identidade = db.busca_id_entidade_migracao(params_exec)
    url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/empenhos'
    sql = f"select * from bethadba.controle_migracao_registro where tipo_registro = 'empenhos' and i_chave_dsk1 = {params_exec['entidade']} and id_gerado is null"
    data = db.consulta_sql(sql=sql, params_exec=params_exec).to_dict('records')
    total = len(data)
    for x in data:
        print(x)
        hash = x['hash_chave_dsk']
        exe_empenho = x['i_chave_dsk2']
        num_empenho = x['i_chave_dsk3']

        criterio = f'entidade.id = {db.busca_id_entidade_migracao(params_exec)} and numero = {num_empenho} and exercicio.ano = {exe_empenho}'
        print(criterio)
        fields = 'exercicio.ano,numeroCadastro.numero'
        for retorna in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            conta += 1
            qslq = f"update bethadba.controle_migracao_registro set id_gerado = {retorna.get('id')} where hash_chave_dsk = '{hash}'"
            print(qslq)
            # db.execute_sql(sql=qslq)
            print(f'Executando: {conta}/{total}')