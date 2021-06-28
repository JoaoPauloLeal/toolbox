import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'assuntoDocumento'
sistema = 304
limite_lote = 100
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/assuntoDocumento"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Assuntos dos Documentos.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    print(req_res)
    dados_update = []
    hash_ant = ''
    for item in req_res:
        if 'id' in item:
            idGerado = item['id']
            chave_dsk1 = item['assunto']['descricao'].upper()
            chave_dsk2 = item['documento']['descricao'].upper()
            print(f'idCloud {idGerado} - {chave_dsk1} - {chave_dsk2}')

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)
        if hash_chaves == hash_ant:
            print(f'hash iguais {hash_chaves}')
        hash_ant = hash_chaves
        contador += 1

        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de Documento',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2
        })

    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    print(contador)
    print('- Busca de dados finalizado.')

