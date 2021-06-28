import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import requests
from datetime import datetime

tipo_registro = 'materiais'
sistema = 305
limite_lote = 20
url = "https://compras.betha.cloud/compras-services/api/materiais/"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)
    token = params_exec.get('token')
    headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}

    for item in req_res:
        idUpdate = item['id']
        estocavel = item['estocavel']
        lista_conteudo_retorno.append({
            'idUpdate': idUpdate,
            'estocavel': estocavel,
            'conteudo': item
        })
        # contador += 1

    for item in lista_conteudo_retorno:
        if item['estocavel'] is True:
            item_atualizado = item['conteudo']
            item_atualizado['estocavel'] = False
            print(item_atualizado)
            contador += 1
    print("ENCONTROU -------")
    print(contador)
    print('- Busca de dados finalizado.')
