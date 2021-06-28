import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'responsavel'
url = 'https://convenios.cloud.betha.com.br/convenios/api/responsaveis'


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
    headers = {'authorization': f'bearer {params_exec["token_tela"]}', 'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    for item in dados_assunto:
        register_dado(headers, item)
        id_gerado = search_exists(headers, item['identificacao_advogado'])
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de responsavel',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['advogado']),
                str(item['identificacao_advogado'])
            ]
            register_controle_migracao(params_exec, registro)


def register_controle_migracao(params_exec, registro):
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)


def register_dado(headers, dado):
    dict_concedente = {
        "nome": str(dado['advogado']),
        "cpf": str(dado['identificacao_advogado']),
        "tipo": {
            "id": int(dado['tipo_responsavel'])
        }
    }
    retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_concedente))
    print(retorno)


def search_exists(headers, dado):
    try:
        par_search = f'filter=cpf = "{dado}"&limit=20&offset=0&sort'
        retorno = requests.get(url=url, headers=headers, params=par_search)
        id_gerado = retorno.json()['content'][0]['id']
    except Exception:
        id_gerado = None
    finally:
        return id_gerado
