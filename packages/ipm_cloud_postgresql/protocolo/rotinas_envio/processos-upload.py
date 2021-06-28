import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import base64
from pathlib import Path
import logging
from datetime import datetime


tipo_registro = 'processos-upload'
sistema = 304
limite_lote = 1
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/upload/processos"
# url = "http://192.168.41.99:8089/protocolo/service-layer/v1/api/upload/processos"

def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    # model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


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


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    # token = '13d7bc50-b857-4866-8a06-c66ffef5cde6'
    token = params_exec['token']

    contador = 0
    for item in dados:
        arquivo_doc = item["arq_file"]
        # print(arquivo_doc)
        # filename = 'erro_atualiza.jpg'
        # fileObj = get_path(filename)
        # files = {'file': (None, open(fileObj, 'rb')), 'idIntegracao': (None, '154')}

        fileObj = Path(arquivo_doc)
        if fileObj.is_file():
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["key1"], item["id_codigo"], item["ano"],
                                                  item["id_doc"], item["id_seq"])
            m = MultipartEncoder(
                         fields={'file': (item["nome_arq"], open(arquivo_doc, "rb"), item["content_type"]),
                        # fields={'file': (filename, open(fileObj, "rb"), 'image/jpeg'),
                                 'idIntegracao': hash_chaves}
                         )

                    #     print(f'Arquivo {encoded_string}')
                # else:
                #     m = ''
                # print(encoded_string)

            # print(f'Dados gerados ({contador}): ', dict_dados)
            # lista_dados_enviar.append(multipart_form_data)
            lista_controle_migracao.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Upload de Anexos dos Processos',
                'id_gerado': None,
                'i_chave_dsk1': item["key1"],
                'i_chave_dsk2': item["id_codigo"],
                'i_chave_dsk3': item["ano"],
                'i_chave_dsk4': item["id_doc"],
                'i_chave_dsk5': item["id_seq"]
            })
            # print(lista_controle_migracao)
            # print(lista_dados_enviar)
            # headers = {'authorization': f'bearer {token}', 'content-type': 'multipart/form-data'}

            model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
            retorno_req = requests.post(url, headers={'content-type': m.content_type, 'Authorization': f'Bearer {token}'}, data=m)
            # print(f'passou 0 {retorno_req.request.body}')
            # print(f'Passsou {retorno_req.request.headers}')
            # print(retorno_req.text)
            model.atualiza_tabelas_controle_envio_upload(retorno_req, tipo_registro=tipo_registro,
                                                                  url_consulta=url,
                                                                  sistema=sistema)

        contador += 1

    print(f'Contador: {contador}')
    # req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
    #                                               token=token,
    #                                               url=url,
    #                                               tipo_registro=tipo_registro,
    #                                               tamanho_lote=limite_lote)
    # model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')


def get_path(nome_arquivo):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/frotas/json_default/'
    path = path_padrao + nome_arquivo
    return path