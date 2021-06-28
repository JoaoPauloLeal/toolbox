import time

import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime, date

sistema = 1
tipo_registro = 'convenio_repassado_aditivo_situacao'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    if dados_assunto is not None:
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
        url = f"https://convenios.cloud.betha.com.br/convenios/api/convenios-repassados/{item['id_gerado_convenio']}/aditivos-convenios/{item['id_gerado_convenio_aditivo']}/situacoes"
        dict_dado = {
            "tipoSituacao": {
                "id": int(item['id_gerado_tipo'])
            },
            "data": str(item['data_situacao']),
            "aditivoConvenio": {
                "id": int(item['id_gerado_convenio_aditivo'])
            }
        }

        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado = search_exists(headers, url)
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de convenio repassado aditivo situacao',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['id_gerado_convenio_aditivo']),
                str(item['id_gerado_tipo']),
                str(item['data_situacao'])
            ]
            register_controle_migracao(params_exec, registro)
        print(retorno)


def search_exists(headers, url):
    try:
        retorno = requests.get(url=url, headers=headers)
        if not retorno.ok:
            time.sleep(3)
            check_registro(url, headers)
        id_gerado = retorno.json()['content'][0]['id']
    except Exception:
        id_gerado = None
    finally:
        return id_gerado


def check_registro(url_check, headers):
    cadastrado = True
    while not cadastrado:
        retorno = requests.get(url=url_check, headers=headers)
        if retorno.ok:
            cadastrado = False
    return retorno


def register_controle_migracao(params_exec, registro):
    print(registro)
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)
