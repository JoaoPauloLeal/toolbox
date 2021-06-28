import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import requests
from datetime import datetime

tipo_registro = 'processos-administrativo'
sistema = 305
limite_lote = 1000
url = "https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{id}"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    for ano in range(2017, 2021 + 1):
        url_parcial = f"https://compras.betha.cloud/compras-services/api/exercicios/{int(ano)}/processos-administrativo"
        print('- Iniciando busca dos dados de dados.')
        print(url_parcial)
        lista_conteudo_retorno = []
        hoje = datetime.now().strftime("%Y-%m-%d")
        contador = 0
        req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                              url=url_parcial,
                                                              tipo_registro=tipo_registro,
                                                              tamanho_lote=limite_lote)
        token = params_exec.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}

        for item in req_res:
            idExcluir = item['id']
            lista_conteudo_retorno.append(idExcluir)
            contador += 1

        for item in lista_conteudo_retorno:
            url_parcial_two = f"https://compras.betha.cloud/compras-services/api/exercicios/{int(ano)}/processos-administrativo/{int(item)}/atos-finais"

            req_res_atos = interacao_cloud.busca_dados_cloud(params_exec,
                                                        url=url_parcial_two,
                                                        tipo_registro=tipo_registro,
                                                        tamanho_lote=limite_lote)
            if req_res_atos:
                lista_conteudo_retorno_ato = []
                for sub_item in req_res_atos:
                    idExcluir = sub_item['id']
                    lista_conteudo_retorno_ato.append(idExcluir)
                    contador += 1

                for sub_item_item in lista_conteudo_retorno_ato:
                    url_parcial_two_delete = f"https://compras.betha.cloud/compras-services/api/exercicios/{int(ano)}/processos-administrativo/{int(item)}/atos-finais/{int(sub_item_item)}"
                    print(url_parcial_two_delete)
                    retorno_req_ato = requests.delete(url_parcial_two_delete, headers=headers)
                    print(str(item), retorno_req_ato.status_code)
        print(contador)
        print('- Busca de dados finalizado.')
