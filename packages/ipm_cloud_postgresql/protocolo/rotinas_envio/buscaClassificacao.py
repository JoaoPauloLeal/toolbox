import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'classificacao'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/classificacao"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Classificação.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    print(req_res)
    dados_update = []

    for item in req_res:
        if 'id' in item:
            idGerado = item['id']
            chave_dsk1 = item['descricao'].upper()
            print(f'idCloud {idGerado} - {chave_dsk1}')

        # keys_update = [key for key in item.keys() if key == "id"]
        #
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1)
        # item.update({"idIntegracao": hash_chaves, "idGerado": idGerado, "conteudo": {"id": idGerado, "descricao": item['descricao']}})
        # [item.pop(key) for key in keys_update]
        #
        # keys_update2 = [key2 for key2 in item.keys() if key2 == "descricao"]
        # item.update('')
        # [item.pop(key2) for key2 in keys_update2]
        # print(item)
        contador += 1

        # dados_update.append(item)

        # print(dados_update)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de Classificação',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1
        })

    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    print(contador)
    print('- Busca de dados finalizado.')
