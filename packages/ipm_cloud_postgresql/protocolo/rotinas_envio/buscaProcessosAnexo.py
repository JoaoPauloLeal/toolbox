import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import base64
from datetime import datetime


tipo_registro = 'processos-anexos'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processos-anexo"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Processos Documentos Anexos.')
    lista_controle_migracao = []
    token = params_exec['token']
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    conta_aux = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)

    dados_update = []

    for item in req_res:
        idGerado = item['id']
        data_criado = item['createdIn']

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, idGerado)
        dict_dados = {
            "idIntegracao": hash_chaves,
            "idGerado": idGerado,
            "conteudo": {
                "id": idGerado
            }
        }

        contador += 1
        dados_update.append(dict_dados)
        # conta_aux += 1
        # if conta_aux == 2000:
        #     print('Lotes para enviar >>>>> ')
        #     conta_aux = 0
        #     print(dados_update)

    print(contador)
        # print(f'idCloud {idGerado} - {data_criado}')
    # print(dados_update)

    req_res = interacao_cloud.preparar_requisicao_delete(lista_dados=dados_update,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)

    print('- Busca de dados finalizado.')


"""
1st step - convert image into binary
"""
# with open("BACC53DE5724B4177923E909EC366962F57440EE", "rb") as original_file:
#     encoded_string = base64.b64encode(original_file.read())
#
# print(encoded_string)
# xmzWowsfJbpGwCe0DTveqwvos7Mf0lcVNe/Q+G1hO/p+UNPd/stUse8AhP/3fDixf8HI3No67nvhlYAAAAASUVORK5CYII='

# print(type(encoded_string))
# <class 'bytes'>

"""
2nd step - create new image using the encoded string
"""
# with open("novodocto.pdf", "wb") as new_file:
#     new_file.write(base64.decodebytes(encoded_string))