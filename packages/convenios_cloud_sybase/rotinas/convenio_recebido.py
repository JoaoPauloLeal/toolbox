import simplejson as json
import requests
import bth.db_connector as db
import time
from datetime import datetime, date

sistema = 1
tipo_registro = 'convenio_recebido'
url = 'https://convenios.cloud.betha.com.br/convenios/api/convenios-recebidos'


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
        dict_dado = {
            "numeroConvenio": item['numero_convenio'],
            "valorRepasse": item['valor_repassado'],
            "valorContrapartida": item['valor_contrapartida'],
            "valorGlobal": item['valor_global'],
            "objeto": item['objeto'],
            "dataInicio": str(item['periodo_inicio']),
            "dataTermino": str(item['periodo_fim']),
            "dataAssinatura": str(item['data_assinatura']),
            "modalidade": {
                "id": item['modalidade']
            },
            "concedente": {
                "id": item['concedente']
            }
        }
        print(dict_dado)
        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado = search_exists(headers, item)
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de convenio recebido',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['numero_convenio']),
                str(item['objeto']),
                str(item['periodo_inicio']),
                str(item['periodo_fim']),
                str(item['data_assinatura']),
                str(item['concedente']),
                str(item['modalidade'])
            ]
            register_controle_migracao(params_exec, registro)
        print(retorno)


def search_exists(headers, dado):
    url_convenios_recebidos_consolidados = 'https://convenios.cloud.betha.com.br/convenios/api/convenios-recebidos-consolidados'
    tipo_repasse_search = f'filter=(numeroConvenio elike "{str(dado["numero_convenio"])}" and descricao elike "{str(dado["objeto"])}")'
    try:
        retorno = requests.get(url=url_convenios_recebidos_consolidados, headers=headers, params=tipo_repasse_search)
        if retorno.status_code == 400:
            tipo_repasse_search = f'filter=(numeroConvenio elike "{str(dado["numero_convenio"])}")'
            retorno = requests.get(url=url_convenios_recebidos_consolidados, headers=headers,
                                   params=tipo_repasse_search)
        if not retorno.ok:
            time.sleep(3)
            check_registro(url_convenios_recebidos_consolidados, headers, tipo_repasse_search)
        id_gerado = retorno.json()['content'][0]['idConvenio']
    except Exception:
        id_gerado = None
    finally:
        return id_gerado


def check_registro(url_check, headers, search):
    cadastrado = True
    while not cadastrado:
        retorno = requests.get(url=url_check, headers=headers, params=search)
        if retorno.ok:
            cadastrado = False
    return retorno


def register_controle_migracao(params_exec, registro):
    print(registro)
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)
