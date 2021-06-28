import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, *args, **kwargs):
    for x in cloud.buscaFonte(
            url='https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/entidades',
            token=params_exec['token']):
        print(x['content'].get('nome'))
        print(x['content'].get('cnpj'))