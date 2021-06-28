import requests
import json
import re
import getpass
import settings
import math
import time
import sys
import logging
from tqdm import tqdm
from time import sleep
from datetime import datetime


def verifica_token(token):
    r = {"expired": True}
    print(f'\n:: Iniciando validação do token {token}')
    try:
        url = "https://oauth.cloud.betha.com.br/auth/oauth2/tokeninfo"
        params = {'access_token': token}
        req = requests.get(url=url, params=params)
        data = req.json()

        if 'error' in data.keys():
            r['error'] = data['error']
        if 'user' in data:
            dados_json = data['user']['attributes']['singleAccess']
            r['entityId'] = re.search("(?<=entityId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
            r['databaseId'] = re.search("(?<=databaseId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
        if 'expired' in data:
            r['expired'] = data['expired']

        if not r['expired']:
            print(f'- Token ativo. \n- Database: {r["databaseId"]} \n- Entidade: {r["entityId"]}')
        else:
            print('- Token inválido. Execução será finalizada.')

    except Exception as error:
        print(f'Erro ao executar função "verifica_token". {error}')


def get_dados_token(token):
    r = {}
    try:
        url = "https://oauth.cloud.betha.com.br/auth/oauth2/tokeninfo"
        params = {'access_token': token}
        req = requests.get(url=url, params=params)
        data = req.json()

        if 'user' in data:
            dados_json = data['user']['attributes']['singleAccess']
            r['entityId'] = re.search("(?<=entityId\" : \")(\\d+)(?!=\")", str(dados_json)).group()
            r['databaseId'] = re.search("(?<=databaseId\" : \")(\\d+)(?!=\")", str(dados_json)).group()

    except Exception as error:
        print(f'Erro ao executar função "get_dados_token". {error}')

    finally:
        return r


def preparar_requisicao_sem_lote(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    lista_retorno = []
    dh_inicio = datetime.now()
    lotes_enviados = 0
    total_lotes = len(lista_dados)
    total_erros = 0
    try:
        for i in lista_dados:
            lotes_enviados += 1
            print(f'\r- Dados enviados: {lotes_enviados}/{total_lotes}', '\n' if lotes_enviados == total_lotes else '',
                  end='')
            dict_envio = i
            hash_chaves = None

            if 'idIntegracao' in dict_envio:
                hash_chaves = dict_envio['idIntegracao']
                del dict_envio['idIntegracao']

            if 'url' in dict_envio:
                url = dict_envio['url']
                del dict_envio['url']
            else:
                url = kwargs.get('url')

            json_envio = json.dumps(dict_envio)
            retorno_requisicao = {
                'hash_chave': hash_chaves,
                'id_gerado': None,
                'mensagem': None
            }
            headers = {'authorization': f'bearer {kwargs.get("token")}', 'content-type': 'application/json'}
            retorno_req = requests.post(url, headers=headers, data=json_envio)

            if retorno_req.ok:
                retorno_requisicao['id_gerado'] = int(retorno_req.text)
                lista_retorno.append(retorno_requisicao)
            else:
                retorno_json = retorno_req.json()
                if 'message' in retorno_json:
                    retorno_requisicao['mensagem'] = retorno_json['message']
                    # print('Erro: ', retorno_json['message'])

            if retorno_requisicao['id_gerado'] is None:
                total_erros += 1

            lista_retorno.append(retorno_requisicao)
        print(f'- Envio finalizado. {total_erros} registro(s) retornaram inconsistência.')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')
    finally:
        return lista_retorno


def preparar_requisicao(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote(lote_envio,
                                        url=kwargs.get('url'),
                                        token=kwargs.get('token'),
                                        tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['idLote'] is not None:
                    retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio,
                                    url=kwargs.get('url'),
                                    token=kwargs.get('token'),
                                    tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['idLote'] is not None:
                retorno_requisicao.append(ret_envio)

        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')

        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')

    finally:
        return retorno_requisicao


def preparar_requisicao_frotas(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote_frotas(lote_envio,
                                               url=kwargs.get('url'),
                                               token=kwargs.get('token'),
                                               tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['idLote'] is not None:
                    retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote_frotas(lote_envio,
                                           url=kwargs.get('url'),
                                           token=kwargs.get('token'),
                                           tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['idLote'] is not None:
                retorno_requisicao.append(ret_envio)

        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')

        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')

    finally:
        return retorno_requisicao


def enviar_lote(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '304',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'idLote': None,
        'conteudo_json': json_envio_lote
    }
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        retorno_req = requests.post(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['id']
                elif 'idLote' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['idLote']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                elif settings.SISTEMA_ORIGEM == 'protocolo':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['idLote']
                elif settings.SISTEMA_ORIGEM == 'livro_eletronico':
                    retorno_requisicao['url_consulta'] = url + '/' + retorno_requisicao['idLote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def enviar_lote_frotas(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '306',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'idLote': None,
        'conteudo_json': json_envio_lote
    }
    # print(json_envio_lote)
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        retorno_req = requests.post(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['id']
                elif 'idLote' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['idLote']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                elif settings.SISTEMA_ORIGEM == 'protocolo':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['idLote']
                elif settings.SISTEMA_ORIGEM == 'frotas':
                    retorno_requisicao[
                        'url_consulta'] = 'https://frotas.betha.cloud/frotas-services/api/conversoes/lotes/' + \
                                          retorno_requisicao['idLote']
                elif settings.SISTEMA_ORIGEM == 'livro_eletronico':
                    retorno_requisicao['url_consulta'] = url + '/' + retorno_requisicao['idLote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def enviar_cadastro_frotas_unico(dado, **kwargs):
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'Content-type': 'application/json'}
        json_envio = json.dumps(dado)
        retorno_req = requests.post(url=url, headers=headers, data=json_envio)
        if retorno_req.status_code != 201:
            print("Erro durante a execução da função enviar_cadastro_frotas_unico.", retorno_req.text)
            print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.status_code == 201:
            print("Sucesso ao realizar post do cadastro: ", dado)
    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_req


def busca_dados_cloud(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 100
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        dados_coletados.append(i)
                elif 'retorno' in retorno_json:
                    for i in retorno_json['retorno']:
                        dados_coletados.append(i)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def search_data_cloud_any_where(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 100
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                has_next = retorno_json['hasNext']
                dados_coletados.append(retorno_json)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_dados_cloud_api_tela(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 1000
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token_tela"]}',
               'app-context': f'{params_exec["app-context"]}',
               'user-access': f'{params_exec["user-access"]}',
               }

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)
            if r.status_code == 401:
                print("----- Token de tela expirou -----")
                new_token = input("Insira o novo token de tela: ")
                headers = {
                    'authorization': f'bearer {new_token}',
                    'app-context': f'{params_exec["app-context"]}',
                    'user-access': f'{params_exec["user-access"]}',
                }
            if r.ok:
                retorno_json = r.json()
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        dados_coletados.append(i)
                elif 'retorno' in retorno_json:
                    for i in retorno_json['retorno']:
                        dados_coletados.append(i)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)
            if rodada_busca == 4:
                print("parada manual")
                has_next = False

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_dados_cloud_alterado(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 1000
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        dados_coletados.append(i)
                elif 'retorno' in retorno_json:
                    for i in retorno_json['retorno']:
                        dados_coletados.append(i)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            # if offset >= 10000:
            #     has_next = False

            if erros_consecutivos >= 3:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                # has_next = False

            if erros_consecutivos >= 1:
                rodada_busca -= 1
                offset -= limit
                retornou = True
                while retornou:
                    print("Refazendo busca:", offset)
                    r = requests.get(url=url, params=params, headers=headers)
                    if r.ok:
                        retorno_json = r.json()
                        has_next = retorno_json['hasNext']
                        if 'content' in retorno_json:
                            for i in retorno_json['content']:
                                dados_coletados.append(i)
                        elif 'retorno' in retorno_json:
                            for i in retorno_json['retorno']:
                                dados_coletados.append(i)
                        rodada_busca += 1
                        offset += limit
                        erros_consecutivos = 0
                        retornou = False
            # while r is None:
            #     r = requests.get(url=url, params=params, headers=headers)
            #     print("BUSCANDO NOVAMENTE offset: ", offset)
            #     if r.ok:
            #         # rodada_busca -= 1
            #         # offset -= limit
            #         retorno_json = r.json()
            #         has_next = retorno_json['hasNext']
            #         if 'content' in retorno_json:
            #             for i in retorno_json['content']:
            #                 dados_coletados.append(i)
            #         elif 'retorno' in retorno_json:
            #             for i in retorno_json['retorno']:
            #                 dados_coletados.append(i)
            #         rodada_busca += 1
            #         offset += limit
            #         erros_consecutivos = 0

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_dados_cad_unico_cloud(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 100
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'iniciaEm': offset, 'nRegistros': limit}
            r = requests.get(url=url, params=params, headers=headers)
            if r.ok:
                retorno_json = r.json()
                # print(retorno_json)
                # print(retorno_json['maisPaginas'])
                has_next = bool(retorno_json['maisPaginas'])
                if 'conteudo' in retorno_json:
                    for i in retorno_json['conteudo']:
                        dados_coletados.append(i)
                # elif 'retorno' in retorno_json:
                #     for i in retorno_json['retorno']:
                #         dados_coletados.append(i)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)
            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False
            # if rodada_busca > 100:
            #     print(offset)
            #     break

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_dados_cloud_app(params_exec, **kwargs):
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    limit = 500
    offset = 0
    token_app = 'e244d7bd-f966-455f-9adb-910d0ffca75d'
    content_type = 'application/json'
    user_access = 'mDc5FZggvOU='
    erros_consecutivos = 0
    rodada_busca = 1
    headers = {'authorization': f'bearer {token_app}', 'Content-Type': f'{content_type}',
               'User-Access': f'{user_access}'}

    try:
        while has_next:
            print(f'\r- Realizando busca na página {rodada_busca}', end='')
            params = {'offset': offset, 'limit': limit}
            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        dados_coletados.append(i)
                elif 'retorno' in retorno_json:
                    for i in retorno_json['retorno']:
                        dados_coletados.append(i)

                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False

        print('\n- Busca de páginas finalizada.')
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def enviar_lote_put(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '304',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'idLote': None,
        'conteudo_json': json_envio_lote
    }
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        retorno_req = requests.put(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['id']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                if settings.SISTEMA_ORIGEM == 'protocolo':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['idLote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def enviar_lote_delete(lote, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '304',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'idLote': None,
        'conteudo_json': json_envio_lote
    }
    try:
        url = kwargs.get('url')
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        retorno_req = requests.delete(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['id']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                if settings.SISTEMA_ORIGEM == 'protocolo':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['idLote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def enviar_lote_delete_compras(lote, url, *args, **kwargs):
    json_envio_lote = json.dumps(lote)
    retorno_requisicao = {
        'sistema': '305',
        'tipo_registro': kwargs.get('tipo_registro'),
        'data_hora_envio': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'usuario': f'(bthMigracao) {getpass.getuser()}',
        'url_consulta': None,
        'status': 1,
        'idLote': None,
        'conteudo_json': json_envio_lote
    }
    try:
        url = url
        token = kwargs.get('token')
        headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
        retorno_req = requests.delete(url, headers=headers, data=json_envio_lote)
        # print("DEBUG - Tempo requisição: ", retorno_req.elapsed.total_seconds(), ' segundos.')
        if retorno_req.ok:
            if 'json' in retorno_req.headers.get('Content-Type'):
                retorno_json = retorno_req.json()
                if 'id_lote' in retorno_json:
                    retorno_requisicao['id_lote'] = retorno_json['idLote']
                elif 'id' in retorno_json:
                    retorno_requisicao['idLote'] = retorno_json['id']
                else:
                    print('DEBUG - retorno_json: ', retorno_json)
                    retorno_requisicao['id_lote'] = None
                # print('DEBUG - Lote enviado: ', retorno_requisicao['id_lote'])
                if settings.SISTEMA_ORIGEM == 'folha':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['id_lote']
                if settings.SISTEMA_ORIGEM == 'protocolo':
                    retorno_requisicao['url_consulta'] = url + '/lotes/' + retorno_requisicao['idLote']
                else:
                    retorno_requisicao['url_consulta'] = re.sub('\w+$', f'lotes/{retorno_requisicao["idLote"]}', url)
            else:
                print('Retorno não JSON:', retorno_req.status_code, retorno_req.text)

    except Exception as error:
        print(f'Erro durante a execução da função enviar_lote. {error}')
    finally:
        return retorno_requisicao


def preparar_requisicao_put(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote_put(lote_envio,
                                            url=kwargs.get('url'),
                                            token=kwargs.get('token'),
                                            tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['idLote'] is not None:
                    retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio,
                                    url=kwargs.get('url'),
                                    token=kwargs.get('token'),
                                    tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['idLote'] is not None:
                retorno_requisicao.append(ret_envio)

        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')

        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')

    finally:
        return retorno_requisicao


def preparar_requisicao_delete(lista_dados, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote_delete(lote_envio,
                                               url=kwargs.get('url'),
                                               token=kwargs.get('token'),
                                               tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['idLote'] is not None:
                    retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio,
                                    url=kwargs.get('url'),
                                    token=kwargs.get('token'),
                                    tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['idLote'] is not None:
                retorno_requisicao.append(ret_envio)

        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')

        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')

    finally:
        return retorno_requisicao


def preparar_requisicao_delete_compras(lista_dados, url, *args, **kwargs):
    print('- Iniciando montagem e envio de lotes.')
    dh_inicio = datetime.now()
    retorno_requisicao = []
    lote_envio = []
    lotes_enviados = 0
    tamanho_lote = 1 if 'tamanho_lote' not in kwargs else kwargs.get('tamanho_lote')
    total_lotes = math.ceil(len(lista_dados) / tamanho_lote)
    try:
        for i in lista_dados:
            lote_envio.append(i)
            if len(lote_envio) >= tamanho_lote:
                ret_envio = enviar_lote_delete_compras(lote_envio,
                                                       url=url,
                                                       token=kwargs.get('token'),
                                                       tipo_registro=kwargs.get('tipo_registro'))
                if ret_envio['idLote'] is not None:
                    retorno_requisicao.append(ret_envio)

                lotes_enviados += 1
                print(f'\r- Lotes enviados: {lotes_enviados}/{total_lotes}', end='')
                lote_envio = []
        if len(lote_envio) != 0:
            ret_envio = enviar_lote(lote_envio,
                                    url=kwargs.get('url'),
                                    token=kwargs.get('token'),
                                    tipo_registro=kwargs.get('tipo_registro'))
            if ret_envio['idLote'] is not None:
                retorno_requisicao.append(ret_envio)

        if tamanho_lote != total_lotes:
            print(f'\r- Lotes enviados: {total_lotes}/{total_lotes}', end='')

        print(f'\n- Envio de lotes finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')

    except Exception as error:
        print(f'Erro durante a execução da função preparar_requisicao. {error}')

    finally:
        return retorno_requisicao


def busca_api_fonte_dados(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 500 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token"]}',
               'app-context': f'{params_exec["app-context"]}',
               'user-access': f'{params_exec["user-access"]}'}
    try:
        while has_next:
            params = {'offset': offset, 'limit': limit, 'fields': campos}

            if criterio is not None:
                params.update({'filter': criterio})

            if ordenacao is not None:
                params.update({'sort': ordenacao})

            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                # print('retorno_json', retorno_json)
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        # print('DEBUG', i)
                        dados_coletados.append(i)
                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            # if rodada_busca == 4:
            #     print('Parada manual')
            #     has_next = False

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_api_fonte_dados_compras(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 500 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token"]}'}
    try:
        while has_next:
            params = {'offset': offset, 'limit': limit, 'fields': campos}

            if criterio is not None:
                params.update({'filter': criterio})

            if ordenacao is not None:
                params.update({'sort': ordenacao})

            r = requests.get(url=url, params=params, headers=headers)

            if r.ok:
                retorno_json = r.json()
                # print('retorno_json', retorno_json)
                has_next = retorno_json['hasNext']
                if 'content' in retorno_json:
                    for i in retorno_json['content']:
                        # print('DEBUG', i)
                        dados_coletados.append(i)
                rodada_busca += 1
                offset += limit
                erros_consecutivos = 0
            else:
                erros_consecutivos += 1
                print('\nErro ao realizar requisição.', r.status_code)

            # if rodada_busca == 4:
            #     print('Parada manual')
            #     has_next = False

            if erros_consecutivos >= 10:
                print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_api_fonte_dados_barra(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 1000 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token-tela"]}',
               'app-context': f'{params_exec["app-context"]}',
               'user-access': f'{params_exec["user-access"]}'}

    max_range = busca_total_fonte(offset, limit, campos, criterio, ordenacao, headers, url)
    # print(max_range)
    max_range = (int(max_range) / int(limit))
    # print(max_range)
    max_range = format(round(max_range + 0.5))
    # print(max_range)
    # print("{0:.0f}".format(round(max_range, 0)))
    try:
        while has_next:
            for tq in tqdm(range(int(max_range)), desc='Realizando busca'):
                params = {'offset': offset, 'limit': limit, 'fields': campos}

                if criterio is not None:
                    params.update({'filter': criterio})
                if ordenacao is not None:
                    params.update({'sort': ordenacao})

                r = requests.get(url=url, params=params, headers=headers)
                if r.status_code == 401:
                    print("----- Token de tela expirou -----")
                    new_token = input("Insira o novo token de tela: ")
                    headers = {
                        'authorization': f'bearer {new_token}',
                        'app-context': f'{params_exec["app-context"]}',
                        'user-access': f'{params_exec["user-access"]}',
                    }
                    r = requests.get(url=url, params=params, headers=headers)
                if r.ok:
                    retorno_json = r.json()
                    # print('retorno_json', retorno_json)
                    has_next = retorno_json['hasNext']
                    if 'content' in retorno_json:
                        for i in retorno_json['content']:
                            # print('DEBUG', i)
                            dados_coletados.append(i)
                    rodada_busca += 1
                    offset += limit
                    erros_consecutivos = 0
                    # max_range = retorno_json['total']
                else:
                    erros_consecutivos += 1
                    print('\nErro ao realizar requisição.', r.status_code)

                if erros_consecutivos >= 10:
                    print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                    has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_api_fonte_dados_barra_compras(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 1000 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    max_range = busca_total_fonte(offset, limit, campos, criterio, ordenacao, headers, url)
    # print(max_range)
    max_range = (int(max_range) / int(limit))
    # print(max_range)
    max_range = format(round(max_range + 0.5))
    # print(max_range)
    # print("{0:.0f}".format(round(max_range, 0)))
    try:
        while has_next:
            for tq in tqdm(range(int(max_range)), desc='Realizando busca'):
                params = {'offset': offset, 'limit': limit, 'fields': campos}

                if criterio is not None:
                    params.update({'filter': criterio})
                if ordenacao is not None:
                    params.update({'sort': ordenacao})

                r = requests.get(url=url, params=params, headers=headers)

                if r.ok:
                    retorno_json = r.json()
                    # print('retorno_json', retorno_json)
                    has_next = retorno_json['hasNext']
                    if 'content' in retorno_json:
                        for i in retorno_json['content']:
                            # print('DEBUG', i)
                            dados_coletados.append(i)
                    rodada_busca += 1
                    offset += limit
                    erros_consecutivos = 0
                    # max_range = retorno_json['total']
                else:
                    erros_consecutivos += 1
                    print('\nErro ao realizar requisição.', r.status_code)

                if erros_consecutivos >= 10:
                    print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                    has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_api_fonte_dados_contabil_unica_barra(params_exec, **kwargs):
    """
    Função para realizar a busca através das API's de fonte de dados Betha
    :param params_exec: Parâmetros de contexto da execução
    :param kwargs: Parâmetros
    :param campos: Listagem de campos que serão retornados da fonte
    :param criterio: Filtros que serão aplicados na busca da fonte
    :param ordenacao: Ordenação dos campos que serão retornados
    :return: Retorna um objeto <List> contendo os JSON's obtidos da fonte.
    """
    dados_coletados = []
    has_next = True
    url = kwargs.get('url')
    offset = 0
    erros_consecutivos = 0
    rodada_busca = 1
    campos = 'id' if 'campos' not in kwargs else kwargs.get('campos')
    criterio = None if 'criterio' not in kwargs else kwargs.get('criterio')
    ordenacao = None if 'ordenacao' not in kwargs else kwargs.get('ordenacao')
    limit = 1000 if 'limit' not in kwargs else kwargs.get('limit')
    headers = {'authorization': f'bearer {params_exec["token"]}'}

    max_range = busca_total_fonte(offset, limit, campos, criterio, ordenacao, headers, url)
    print(max_range)
    print(limit)
    if max_range is not None:
        max_range = (int(max_range) / int(limit))
        max_range = format(round(max_range + 0.5))
    else:
        max_range = 1

    try:
        while has_next:
            for tq in tqdm(range(int(max_range)), desc='Realizando busca'):
                params = {'offset': offset, 'limit': limit, 'fields': campos}

                if criterio is not None:
                    params.update({'filter': criterio})
                if ordenacao is not None:
                    params.update({'sort': ordenacao})

                r = requests.get(url=url, params=params, headers=headers)

                if r.ok:
                    retorno_json = r.json()
                    # print('retorno_json', retorno_json)
                    has_next = retorno_json['hasNext']
                    if 'content' in retorno_json:
                        for i in retorno_json['content']:
                            # print('DEBUG', i)
                            dados_coletados.append(i)
                    rodada_busca += 1
                    offset += limit
                    erros_consecutivos = 0
                    # max_range = retorno_json['total']
                else:
                    erros_consecutivos += 1
                    print('\nErro ao realizar requisição.', r.status_code)

                if erros_consecutivos >= 10:
                    print('Diversas requisições consecutivas retornaram erro. Verificar se o servidor está ativo.')
                    has_next = False
    except Exception as error:
        print(f'Erro durante a execução da função busca_dados. {error}')

    finally:
        return dados_coletados


def busca_total_fonte(offset, limit, campos, criterio, ordenacao, headers, url):
    params = {'offset': offset, 'limit': limit, 'fields': campos}

    if criterio is not None:
        params.update({'filter': criterio})

    if ordenacao is not None:
        params.update({'sort': ordenacao})

    r = requests.get(url=url, params=params, headers=headers).json()
    return r['total']
