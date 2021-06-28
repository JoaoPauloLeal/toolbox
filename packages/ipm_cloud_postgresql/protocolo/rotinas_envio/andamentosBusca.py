import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 304
tipo_registro = 'andamentos-fonte-dados-atual'
url = 'https://api.dados.protocolo.betha.cloud/protocolo/processos/dados/api/andamentos'


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'id, dataAndamento, processo(id, numeroProcesso), idUsuarioDest, situacaoAndamento'
    criterio = "dataAndamento > '2021-01-01T00:00:00' and dataAndamento < '2021-12-16T23:59:59'"
    # criterio = "id = 4317337"
    ordenacao = 'id asc'
    lista_dados = []
    # dict_dados = []
    registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, ordenacao=ordenacao, criterio=criterio)
    # registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, ordenacao=ordenacao)
    contador = 0

    # for item in registro_cloud:
    #     dict_dados = item
    #     lista_dados.append(dict_dados)
    #     contador += 1
    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_cloud:
        # print(item)
        idGerado = item['id']
        chave_dsk1 = item['dataAndamento']
        processo = item['processo']
        chave_dsk2 = processo['id']
        chave_dsk3 = processo['numeroProcesso']
        chave_dsk4 = item['idUsuarioDest']
        chave_dsk5 = item['situacaoAndamento']
        # print(item['idUsuarioDest'])
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk3), str(idGerado))
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
        })
        contador += 1
    # print(lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_dados)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")

    # with open(get_path(f'retorno_fonte.json'), "w", encoding='utf-8') as f:
    #     f.write(str(lista_dados))
    #     f.close()

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path