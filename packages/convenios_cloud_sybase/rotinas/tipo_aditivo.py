import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'tipo_aditivo'
url = 'https://convenios.cloud.betha.com.br/convenios/api/tipos-aditivos'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados()
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados():
    dados_assunto = []
    descricao = {
        'descricao': ['Acréscimo']
    }
    for item in descricao['descricao']:
        dados_assunto.append(item)
    return dados_assunto


def iniciar_envio(params_exec, dados_assunto):
    headers = {'authorization': f'bearer {params_exec["token_tela"]}',
               'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    for item in dados_assunto:
        dict_dado = {
            "descricao": item,
            "classificacao": {
                "key": "ACRESCIMO"
            },
            "tipo": {
                "key": "PRAZO_VALOR"
            },
            "passivelDataTermino": False,
            "passivelValor": False
        }
        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado_tipo_repasse = search_exists(headers, item)
        if id_gerado_tipo_repasse is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de tipo aditivo',
                str(id_gerado_tipo_repasse),
                str(params_exec['id_entidade']),
                str(item)
            ]
            register_controle_migracao(params_exec, registro)
        print(retorno)


def search_exists(headers, dado):
    dado_search = f'filter=(descricao elike "{dado}")'
    try:
        retorno = requests.get(url=url, headers=headers, params=dado_search)
        id_gerado = retorno.json()['content'][0]['id']
    except Exception:
        id_gerado = None
    finally:
        return id_gerado


def register_controle_migracao(params_exec, registro):
    print(registro)
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)
