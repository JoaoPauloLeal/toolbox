import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 304
tipo_registro = 'andamentos-busca-fonte-dados'
url = 'https://api.dados.protocolo.betha.cloud/protocolo/processos/dados/api/andamentos'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    busca_dados_cloud(params_exec)


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
        query = model.get_consulta(params_exec, 'andamentosBuscaDiferente.sql', ano)
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    campos = 'id, dataAndamento, processo(id, numeroProcesso), idUsuarioDest, situacaoAndamento, idOrganogramaDest, idOrganogramaOrig'
    criterio = "dataAndamento > '2018-01-01T00:00:00' and dataAndamento < '2018-12-31T23:59:59'"
    lista_dados = []
    registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos, criterio=criterio)
    # registro_cloud = interacao_cloud.busca_api_fonte_dados_barra(params_exec, url=url, campos=campos)
    contador = 0

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
        chave_dsk6 = item['idOrganogramaDest']
        chave_dsk7 = item['idOrganogramaOrig']
        # print(item['idUsuarioDest'])
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk3), str(idGerado), str(chave_dsk5), str(chave_dsk6), str(contador+1))
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


def isNaN(num):
    return num != num