import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'modalidade'
url = 'https://convenios.cloud.betha.com.br/convenios/api/modalidades'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados()
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados():
    dados_assunto = []
    descricao = {
        'descricao': ['Objeto', 'Prazo', 'Valor', 'Prazo/Valor', 'Objeto/Valor', 'Objeto/Prazo', 'Objeto/Prazo/Valor',
                      'Outros',
                      'Principal', 'Acréscimo', 'Decréscimo', 'Nenhum', 'Termo de Colaboração', 'Termo de Fomento',
                      'Termo de Convênio',
                      'Termo de Parceria', 'Contrato de Gestão', 'Termo de Responsabilidade',
                      'Termo de Cooperação Técnica', 'Outras transferências voluntárias)']
    }
    identificador = {
        'identificador': ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', '10', '11', '12', '13', '14',
                          '15', '16', '17']
    }
    for item, sub_item in zip(descricao['descricao'], identificador['identificador']):
        dados_assunto.append({
            'descricao': item,
            'identificador': sub_item
        })
    return dados_assunto


def iniciar_envio(params_exec, dados_assunto):
    headers = {'authorization': f'bearer {params_exec["token_tela"]}',
               'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    for item in dados_assunto:
        dict_dado = {
            "descricao": item['descricao']
        }
        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado = search_exists(headers, item['descricao'])
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de modalidade',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['descricao']),
                str(item['identificador'])
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
