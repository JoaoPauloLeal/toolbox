import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'processos'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processos"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Processos.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    conta_situacao = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)

    dados_update = []

    for item in req_res:
        if 'situacao' in item:
            situacao = item['situacao']

            if situacao == 'PENDENTE':
                idGerado = item['id']
                idAssunto = item['assunto']['id']
                idProtocolo = item['protocolo']['id']
                idRequerente = item['requerente']['id']
                hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, idGerado)
                dict_dados = {
                    "idIntegracao": hash_chaves,
                    "idGerado": idGerado,
                    "conteudo": {
                        "id": idGerado,
                        "protocolo": {
                            "id": idProtocolo
                        },
                        "assunto": {
                            "id": idAssunto
                        },
                        "requerente": {
                            "id": idRequerente
                        },
                        "situacao": 'EM_ANALISE'
                    }
                }

                conta_situacao += 1
                dados_update.append(dict_dados)
        contador += 1

    print(contador)
    print(conta_situacao)
    print(f'Dados gerados ({conta_situacao}): ', dados_update)
    print('- Busca de dados finalizado.')
