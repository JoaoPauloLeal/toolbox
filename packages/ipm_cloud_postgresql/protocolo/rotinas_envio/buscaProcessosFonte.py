import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 304
tipo_registro = 'processos-fonte-dados'
url = 'https://api.dados.protocolo.betha.cloud/protocolo/processos/dados/api/processos'


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'processoOrganogramasUsuarios(id, idUsuario), id, numeroProcesso'
    # criterio = "dataAndamento > '2021-04-08T00:00:00' and dataAndamento < '2021-04-08T23:59:59' and idOrganogramaDest = 545"
    lista_dados = []
    dict_dados = []
    # registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, criterio=criterio)
    registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, campos=campos)
    contador = 0

    # for item in registro_cloud:
    #     dict_dados = item
    #     lista_dados.append(dict_dados)
    #     contador += 1
    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_cloud:
        
        print(contador)
        idGerado = item['id']
        
        chave_dsk1 = item['numeroProcesso']
        
        chave_dsk2 = item['processoOrganogramasUsuarios']
        # 
        # # chave_dsk2 = str(processo_usuario['id'])
        # 
        # # chave_dsk3 = processo_usuario['idUsuario']
        # 

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk1), str(idGerado))
        # print(idGerado)
        print("EMPACOU 1.3")
        lista_dados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de processos fonte de dados no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk1': str(chave_dsk2)
        })
        print("EMPACOU 1.5")
        contador += 1
    
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_dados)
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