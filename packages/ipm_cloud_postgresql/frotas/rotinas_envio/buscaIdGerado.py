import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'buscaIdGerado'
sistema = 304
limite_lote = 100
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/documento"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    print('- Iniciando processo de montagem de Json.')

    # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
    with open(get_path(f'id_gerado.json'), "r", encoding='utf-8') as f:
        data = json.load(f)
        f.close()

    lista_id_gerados = []
    # Ira varrer a content pegando os ids que correspondem ao idGerado para exclusão
    for conteudo in data['content']:
        lista_id_gerados.append(conteudo['id'])

    # Prepara o json de exclusão
    prepara_json_exclusao_envio(lista_id_gerados)

    print('- Criação de dados finalizado.')


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
