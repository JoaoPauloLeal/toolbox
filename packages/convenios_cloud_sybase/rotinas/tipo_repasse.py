import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'tipo_repasse'
url_tipos_repasses = 'https://convenios.cloud.betha.com.br/convenios/api/tipos-repasses'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados()
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados():
    dados_assunto = []
    tipo_repasse_descricao = {
        'descricao': ['Anual', 'Bimestral', 'Diária', 'Mensal', 'Quinzenal', 'Sem prestação', 'Semanal', 'Semestral',
                      'Por medição/execução de serviços']
    }
    tipo_repasse_descricao_id = {
        'descricao_id': ['1', '2', '3', '4', '5', '6', '7', '8', '12']
    }
    tipo_repasse_classificacao = {
        'classificacao': ['Auxílio', 'Contribuição', 'Subvenção', 'Outros']
    }
    tipo_repasse_classificacao_id = {
        'classificacao_id': ['4', '6', '5', '7']
    }
    for item, item_two in zip(tipo_repasse_descricao['descricao'], tipo_repasse_descricao_id['descricao_id']):
        for sub_item, sub_item_two in zip(tipo_repasse_classificacao['classificacao'], tipo_repasse_classificacao_id['classificacao_id']):
            if str(sub_item).upper() == 'AUXÍLIO':
                key = 'AUXILIO'
                value = 'A'
            elif str(sub_item).upper() == 'CONTRIBUIÇÃO':
                key = 'CONTRIBUICAO'
                value = 'C'
            elif str(sub_item).upper() == 'SUBVENÇÃO':
                key = 'SUBVENCAO'
                value = 'S'
            else:
                key = 'OUTROS'
                value = 'O'
            dict_tipo_repassase = {
                'descricao': item,
                'descricao_id': item_two,
                'classificacao_id': sub_item_two,
                'classificacao': {
                    "key": key,
                    "value": value,
                    "description": str(sub_item).upper()
                }
            }
            dados_assunto.append(dict_tipo_repassase)
    return dados_assunto


def iniciar_envio(params_exec, dados_assunto):
    headers = {'authorization': f'bearer {params_exec["token_tela"]}',
               'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    for item in dados_assunto:
        descricao = str(item['descricao'] + ' | ' + str(item['classificacao']['description']).lower())
        dict_dado = {
            "descricao": descricao,
            "classificacao": {
                "key": item['classificacao']['key'],
                "value": item['classificacao']['value'],
                "description": item['classificacao']['description']
            }
        }
        retorno = requests.post(url=url_tipos_repasses, headers=headers, data=json.dumps(dict_dado))
        id_gerado_tipo_repasse = search_exists(headers, descricao)
        if id_gerado_tipo_repasse is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de tipo repasse',
                str(id_gerado_tipo_repasse),
                str(params_exec['id_entidade']),
                str(item['descricao']),
                str(item['classificacao']['description']),
                str(item['descricao_id']),
                str(item['classificacao_id'])
            ]
            register_controle_migracao(params_exec, registro)
        print(retorno)


def search_exists(headers, tipo_repasse):
    tipo_repasse_search = f'filter=(descricao elike "%25{tipo_repasse}%25")'
    try:
        retorno = requests.get(url=url_tipos_repasses, headers=headers, params=tipo_repasse_search)
        id_gerado_tipo_repasse = retorno.json()['content'][0]['id']
    except Exception:
        id_gerado_tipo_repasse = None
    finally:
        return id_gerado_tipo_repasse


def register_controle_migracao(params_exec, registro):
    print(registro)
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)
