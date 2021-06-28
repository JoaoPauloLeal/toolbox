import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'usuarios'
sistema = 304
limite_lote = 500
url = "https://plataforma-autorizacoes.betha.cloud/user-accounts/v0.1/api/users"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Usuarios.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud_app(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    # print(req_res)


    for item in req_res:
        if 'id' in item:
            idGerado = item['id']
            chave_aux = item['name']
            if chave_aux == 'clesiofranzoi':
                chave_aux = 'CLESIO HENRIQUE FRANZOI'
            elif chave_aux == 'leo inacio lohn':
                chave_aux = 'LEO INÁCIO LOHN'
            elif chave_aux == 'Marina Petri':
                chave_aux = 'MARINA PETRI CORREA'
            else:
                chave_aux = item['name']

            chave_dsk1 = chave_aux.upper()
            # chave_dsk2 = item['iAssuntos']
            # print(f'idCloud {idGerado} - {chave_dsk1} - {chave_dsk2}')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1 )
        # print(hash_chaves)

        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de Usuarios',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1
        })
        contador += 1
    # print(lista_controle_migracao)
    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    print(contador)
    print('- Busca de dados finalizado.')
