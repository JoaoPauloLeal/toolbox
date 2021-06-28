import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import requests
from datetime import datetime

tipo_registro = 'fornecedores'
sistema = 305
limite_lote = 1000
url = "https://contratos.betha.cloud/contratacao-services/api/materiais/"


def iniciar_processo_envio(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                          url=url,
                                                          tipo_registro=tipo_registro,
                                                          tamanho_lote=limite_lote)

    # print(req_res)

    token = params_exec.get('token')
    headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}

    for item in req_res:
        idExcluir = item['id']
        lista_conteudo_retorno.append(idExcluir)
        contador += 1
    for item in lista_conteudo_retorno:
        urlDelete = str(url) + str(item)
        print(urlDelete)
        retorno_req = requests.delete(urlDelete, headers=headers)
        print(str(item), retorno_req.status_code)
    # print(lista_controle_migracao)
    # model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_conteudo_retorno)
    print(contador)
    print('- Busca de dados finalizado.')
