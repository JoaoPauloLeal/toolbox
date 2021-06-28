import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 304
tipo_registro = 'processos-busca-fonte-dados'
url = 'https://api.dados.protocolo.betha.cloud/protocolo/processos/dados/api/processos'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'processoOrganogramasUsuarios(organograma(nome, id, mascara, mascaraFormatada), idUsuario), id, numeroProcesso'
    criterio = "previstoPara > '2021-01-01T00:00:00' and previstoPara < '2021-12-31T23:59:59'"
    ordenacao = "id asc"
    lista_dados = []
    # registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, ordenacao=ordenacao)
    registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, criterio=criterio)
    contador = 0

    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_cloud:
        # print(item)
        # cont_item_null = 0
        # for item_nulo in item:
        #     if item_nulo is None:
        #         print("ITEM VAZIO", item[cont_item_null-1])
        #     cont_item_null += 1
        print(item)
        # ano_processo = str(item['numeroProcesso']).split('/')

        tamanho_total = len(str(item['numeroProcesso']))
        ano_processo = str(item['numeroProcesso'])[tamanho_total-4:tamanho_total]
        # print(ano_processo)

        # ano_processo = str(ano_processo).split('.')
        idGerado = str(item['id'])
        processoOrganogramasUsuarios = item['processoOrganogramasUsuarios']
        chave_dsk1 = str(processoOrganogramasUsuarios[0]['idUsuario'])
        organograma = processoOrganogramasUsuarios[0]['organograma']
        chave_dsk2 = str(organograma['nome'])
        chave_dsk3 = str(item['numeroProcesso'])
        chave_dsk4 = str(organograma['id'])
        chave_dsk5 = str(organograma['mascara'])
        chave_dsk6 = str(organograma['mascaraFormatada'])
        chave_dsk7 = str(ano_processo[1])
        chave_dsk8 = str(processoOrganogramasUsuarios[0]['id'])
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk3), str(idGerado), str(contador+1))
        # print(idGerado)
        lista_dados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de processos no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2,
            'i_chave_dsk3': chave_dsk3,
            'i_chave_dsk4': chave_dsk4,
            'i_chave_dsk5': chave_dsk5,
            'i_chave_dsk6': chave_dsk6,
            'i_chave_dsk7': chave_dsk7,
            'i_chave_dsk8': chave_dsk8,
        })
        contador += 1
    # print(lista_dados)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_dados)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path
