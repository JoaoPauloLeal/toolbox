import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'natureza_texto_juridico'
url = 'https://convenios.cloud.betha.com.br/convenios/api/naturezas-texto-juridico-ato'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = db.get_consulta_conv(params_exec, f'{tipo_registro}.sql')
        df = db.consulta_sql(query, params_exec)
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    headers = {'authorization': f'bearer {params_exec["token_tela"]}',
               'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    for item in dados_assunto:
        dict_dado = {
            "descricao": item['descricao']
        }
        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado_tipo_repasse = search_exists(headers, item['descricao'])
        if id_gerado_tipo_repasse is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de natureza do texto juridico',
                str(id_gerado_tipo_repasse),
                str(params_exec['id_entidade']),
                str(item['descricao'])
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
