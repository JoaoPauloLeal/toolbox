import bth.interacao_cloud as interacao_cloud
import requests

tipo_registro = 'solicitacoes-delete'
sistema = 305
limite_lote = 1000


def iniciar_processo_envio(params_exec, *args, **kwargs):
    print('- Iniciando busca das contratações via SL.')
    '''
    *lista_contratacoes* = LISTA DOS ID GERADOS DAS CONTRATAÇÕES
    OBS.: PODE SER ALTERADO MANUALMENTE
    '''
    contador = 0
    '''
    METODO CASO SEJA NECESSARIO DELETAR TODAS AS SOLICITAÇÕES DE FORNECIMENTO
    IRA REALIZAR BUSCA DOS IDS GERADOS DE TODAS AS CONTRATAÇÕES
    '''
    lista_contratacoes = []

    url = f"https://contratos.betha.cloud/contratos/api/contratacoes/0/solicitacoesfornecimento?filter&gestaosolicitacoes=true&limit=1000&offset=0&sort=data+desc,+numeroSolicitacao+desc"
    req_res_one = interacao_cloud.busca_dados_cloud_api_tela(params_exec, url=url)

    for item in req_res_one:
        contratacao = item['contratacao']
        numeroAno = item['numeroAno']
        cont_string = 0
        for sub_item in numeroAno:
            if sub_item == '/':
                ano_solicitacao = numeroAno[cont_string + 1:cont_string + 5]
            cont_string += 1
        lista_contratacoes.append({
            "idsolicitacao": item['id'],
            "idcontratacao": contratacao['id'],
            "ano_solicitacao": ano_solicitacao
        })
        contador += 1

    print('- Busca das contratações finalizada.')
    for item in lista_contratacoes:
        headers = {'authorization': f'bearer {params_exec["token"]}'}
        urlDeleteSolicitacao = f"https://contratos.betha.cloud/contratacao-services/api/exercicios/{item['ano_solicitacao']}/contratacoes/{item['idcontratacao']}/solicitacoes/{item['idsolicitacao']}"
        # print(urlDeleteSolicitacao)
        r_del = requests.delete(url=urlDeleteSolicitacao, headers=headers, allow_redirects=True)
        print(f"DELETE solicitação id = {item['idsolicitacao']} | retorno = {r_del}")


print('- Delete de dados finalizado.')
