import bth.interacao_cloud as interacao_cloud
import requests

tipo_registro = 'comprovantes'
sistema = 1
limite_lote = 1000
url = "https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/comprovantes"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    lista_dados_enviar = []
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)

    token = params_exec.get('token')
    headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}

    for item in req_res:
        idGerado = item['idGerado']
        content = item['content']
        lista_conteudo_retorno.append({
            'idGerado': idGerado['id']
        })
        contador += 1

    for item in lista_conteudo_retorno:
        dict_dados = {
            "idIntegracao": str(item['idGerado']),
            "idGerado": {
                "id": item['idGerado']
            },
            "content": {
                "exercicio": 2021
            }
        }
        contador += 1
        lista_dados_enviar.append(dict_dados)
        urlDelete = str(url)
        print(str(dict_dados).replace("\'", "\""))
        retorno_req = requests.delete(urlDelete, headers=headers, data=str(dict_dados).replace("\'", "\""))
        print(retorno_req.text)
        print(retorno_req.status_code)

    print(lista_dados_enviar)
    print('- Busca de dados finalizado.')
