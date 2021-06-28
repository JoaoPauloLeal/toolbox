import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'busca-contratacao'
url = 'https://contratos.betha.cloud/contratacao-services/api/exercicios/2020/contratacoes'


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    # dados_assunto = coletar_dados(params_exec, ano)
    # dados_enviar = pre_validar(params_exec, dados_assunto)
    dados_enviar = []
    busca_dados_cloud(params_exec, dados_enviar)


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def coletar_dados(params_exec, ano):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, 'busca-contratacao.sql', ano)
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def busca_dados_cloud(params_exec, dados):
    lista_id_gerados = []
    # for lista_busca_fonte in dados:
    #     lista_id_gerados.append(lista_busca_fonte['numero'])
    # print(lista_id_gerados)
    print('- Iniciando busca de dados no cloud.')
    campos = 'id'

    lista_att = str(lista_id_gerados).replace('[', '')
    lista_att2 = str(lista_att).replace(']', '')

    # criterio = f'id not in ({str(lista_att2)})'
    lista_dados = []

    # registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, criterio=criterio)
    registro_cloud = interacao_cloud.busca_api_fonte_dados_compras(params_exec, url=url, campos=campos)
    contador = 0
    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_cloud:
        idGerado = item['id']
        # if item['id'] == 943603:
        #     print("----------------------")
        #     print(item)
        #     print("----------------------")
        if item['ano'] is None:
            chave_dsk1 = "None"
        else:
            chave_dsk1 = item['ano']
        chave_dsk2 = item['observacao']
        chave_dsk3 = item['sequencial']
        chave_dsk4 = item['dataAssinatura']
        # chave_dsk4 = item
        # print(item['idUsuarioDest'])
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, '2016', '2020', )
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['clicodigo'], item['ano_contrato'],
                                              item['identificador_contrato'])

        # print(idGerado)
        lista_dados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de contratos no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2,
            'i_chave_dsk3': chave_dsk3,
            'i_chave_dsk4': chave_dsk4,
        })
        contador += 1
    # print(lista_dados)
    model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_dados)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path
