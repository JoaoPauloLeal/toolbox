import bth.cloud_connector as cloud
import bth.db_connector as db
servico = 'entidades'


def iniciar_processo_busca(params_exe, ano, *args, **kwargs):
    url = cloud.retorna_url_servico_compras(servico=servico)
    cnpj = cloud.get_service_layer(url=url, params_exec=params_exe)
    print(cnpj)
    # print(f"""
    #         update compras.entidades set cnpj = {cnpj[0].get('cnpj')}
    #         where i_entidades = {params_exe['entidade']}
    #     """)
    # db.execute_sql(f"""
    #     update compras.entidades set cnpj = {cnpj[0].get('cnpj')}
    #     where i_entidades = {params_exe['entidade']}
    # """)
    print('Alterado CNPJ da entidade')