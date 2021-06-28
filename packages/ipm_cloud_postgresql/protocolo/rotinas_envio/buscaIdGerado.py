import packages.ipm_cloud_postgresql.model as model
import requests
import json
import re
import getpass
import settings
import math
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'andamentos'
sistema = 304
limite_lote = 100
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/andamentos"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_para_deletar = []
    # hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                          url=url,
                                                          tipo_registro=tipo_registro,
                                                          tamanho_lote=limite_lote)
    print('EMPACOU 1')
    # print(req_res)
    for item in req_res:
        data_migracao = datetime.strptime('2021-01-10T01:00:00', '%Y-%m-%dT%H:%M:%S')
        data_andamento = datetime.strptime(item['dataAndamento'], '%Y-%m-%dT%H:%M:%S')
        if data_andamento.date() <= data_migracao.date():
            lista_para_deletar.append(item['id'])
            contador += 1
            print(contador)
    print('--------CONTADOR----------')
    print(contador)
    # 1169997
    print('--------------------------')
    # print(lista_para_deletar)
    prepara_json_exclusao_envio_lote(lista_para_deletar)
        # idGerado = item['id']
        # chave_dsk1 = item['nome'].upper()
        # chave_dsk2 = item['cpfCnpj']
        #
        # hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)
        # # print(idGerado)
        # lista_controle_migracao.append({
        #     'sistema': sistema,
        #     'tipo_registro': tipo_registro,
        #     'hash_chave_dsk': hash_chaves,
        #     'descricao_tipo_registro': 'Busca de Fornecedores',
        #     'id_gerado': idGerado,
        #     'i_chave_dsk1': chave_dsk1,
        #     'i_chave_dsk2': chave_dsk2
        # })
        # contador += 1

    # print('- Iniciando processo de montagem de Json.')
    # dados = coletar_dados(params_exec, ano)
    #
    # # # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
    # # with open(get_path(f'id_gerado.json'), "r", encoding='utf-8') as f:
    # #     data = json.load(f)
    # #     f.close()
    #
    # lista_id_gerados = []
    # # # Ira varrer a content pegando os ids que correspondem ao idGerado para exclusão JSON
    # # for conteudo in data['content']:
    # #     lista_id_gerados.append(conteudo['id'])
    #
    # # Ira varrer a content pegando os ids que correspondem ao idGerado para exclusão SQL
    # for item in dados:
    #     lista_id_gerados.append(item['id_gerado'])  # id_gerado é o campo no banco que sera deletado no json
    #
    # # Prepara o json de exclusão
    # prepara_json_exclusao_envio(lista_id_gerados)

    print('- Criação de dados finalizado.')


def coletar_dados(params_exec, ano):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, 'id_gerado.sql', ano)
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path


def aplica_parametros(params_exec, t):
    texto_consulta = t
    try:
        for param in params_exec:
            # texto_consulta = texto_consulta.replace(('{{' + param + '}}'), str(params_exec.get(param)))
            texto_consulta = texto_consulta.find("assunto", '"id":')
        print(texto_consulta)

    except Exception as error:
        print("Erro ao executar função 'aplica_parametros'.", error)

    finally:
        return texto_consulta


def prepara_json_exclusao_envio(dados):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    contador = 0
    id_integracao = "ExclusaoIdGeradosIncoerentes"
    for item in dados:
        dict_dados = {
                "idIntegracao": id_integracao,
                "idGerado": item,
                "conteudo": {
                    "idGerado": item
                }
        }
        lista_dados_enviar.append(dict_dados)
        contador += 1
    # Caso necessario ver o json que esta sendo formado
    # print(lista_dados_enviar)
    # Sera sempre criado um novo arquivo com o conteudo de ids feitos no modelo anterior
    with open(get_path(f'template_exclude.json'), "w", encoding='utf-8') as f:
        f.write(str(lista_dados_enviar))
        f.close()
    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    # req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
    #                                               token=token,
    #                                               url=url,
    #                                               tipo_registro=tipo_registro,
    #                                               tamanho_lote=limite_lote)
    # model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')



def prepara_json_exclusao_envio_lote(dados):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    contador = 0
    id_integracao = "ExclusaoIdGeradosIncoerentes"
    print('delete1')
    for item in dados:
        dict_dados = {
                "idIntegracao": id_integracao,
                "idGerado": item,
                "conteudo": {
                    "idGerado": item
                }
        }
        lista_dados_enviar.append(dict_dados)
        contador += 1
        # if contador == 500:
        #     headers = {'authorization': f'bearer b8ddcd70-e359-4814-bdfb-6aff6f02522d', 'content-type': 'application/json'}
        #     retorno_req = requests.delete(url, headers=headers, data=lista_dados_enviar)
        #     print('----------------------------')
        #     print(retorno_req.text)
        #     print('----------------------------')
        #     lista_dados_enviar.clear()
        #     print(lista_dados_enviar)
        #     print('----------------------------')
        #     contador = 0
    print('--------CONTADOR CONTADOR DELETE----------')
    print(contador)
    print('--------------------------')
    headers = {'authorization': f'bearer b8ddcd70-e359-4814-bdfb-6aff6f02522d', 'content-type': 'application/json'}
    retorno_req = requests.delete(url, headers=headers, data=lista_dados_enviar)
    print('---------RETORNO DELETE-------------------')
    print(retorno_req.text)
    print('----------------------------')
    # lista_dados_enviar.clear()
    # print(lista_dados_enviar)
    print('----------------------------')
    contador = 0
    # Caso necessario ver o json que esta sendo formado
    # print(lista_dados_enviar)
    # Sera sempre criado um novo arquivo com o conteudo de ids feitos no modelo anterior
    # with open(get_path(f'template_exclude.json'), "w", encoding='utf-8') as f:
    #     f.write(str(lista_dados_enviar))
    #     f.close()
    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    # req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
    #                                               token=token,
    #                                               url=url,
    #                                               tipo_registro=tipo_registro,
    #                                               tamanho_lote=limite_lote)
    # model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')
