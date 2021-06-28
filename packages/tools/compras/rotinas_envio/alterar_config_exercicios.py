import bth.db_connector as db
import bth.cloud_connector as cloud
import json


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    numLicit = input('Numeração das Licitações por exercicio e modalidade ?')
    retorno = cloud.get_service_layer(params_exec=params_exec, url='https://compras.betha.cloud/compras-services/api/parametros-exercicios-compras')
    # print(retorno)
    if numLicit.upper() in 'SIMYES1':
        for x in retorno:
            print(x)
            json_envio = {
                'numeroLicitacao': {
                    'valor': 'EXERCICIO_MODALIDADE',
                    'descricao': 'Sequencial único por modalidade e exercício'
                }
            }
            cloud.requisicao_service_layer(x.get('links')[0].get('href'))
            # print(x.get('links')[0].get('href'))
            # print(json_envio)
            # print(cloud.envia_registro(url=x.get('links')[0].get('href'), body=json.dumps(json_envio)))
