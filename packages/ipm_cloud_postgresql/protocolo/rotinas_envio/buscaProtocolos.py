import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'protocolos'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/protocolos"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Protocolos.')
    lista_controle_migracao = []
    token = params_exec['token']
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    conta_situacao = 0
    conta_aux = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)

    dados_update = []

    for item in req_res:
        if 'protocoladoPor' in item:
            protocolado_por = item['protocoladoPor']
            if protocolado_por == 'Migracao Betha':
                idGerado = item['id']
                hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, idGerado)
                dict_dados = {
                    "idIntegracao": hash_chaves,
                    "idGerado": idGerado,
                    "conteudo": {
                        "id": idGerado,
                        "protocoladoPor": 'lisandrobv'
                    }
                }

                conta_situacao += 1
                # dados_update.append(dict_dados)
                # conta_aux += 1
                # if conta_aux == 2000:
                #     print(dados_update)
        contador += 1

    print(contador)
    print(conta_situacao)
    print(f'Dados gerados ({conta_situacao}): ')
    # print(dados_update)

    req_res = interacao_cloud.preparar_requisicao_put(lista_dados=dados_update,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Busca de dados finalizado.')
