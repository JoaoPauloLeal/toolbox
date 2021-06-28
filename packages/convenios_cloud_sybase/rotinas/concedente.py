import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime

sistema = 1
tipo_registro = 'concedente'
url = 'https://convenios.cloud.betha.com.br/convenios/api/concedentes'


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
        id_gerado = search_exists(headers, item['cnpj_cpf_concedente'])
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de concedente',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['cnpj_cpf_concedente']),
                str(item['orgao_concedente'])
            ]
            register_controle_migracao(params_exec, registro)


def register_controle_migracao(params_exec, registro):
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)


def register_dado(headers, dado):
    if int(dado['esfera']) == 1:
        esfera = 'ESTADUAL'
    elif int(dado['esfera']) == 2:
        esfera = 'FEDERAL'
    elif int(dado['esfera']) == 3:
        esfera = 'MUNICIPAL'
    elif int(dado['esfera']) == 4:
        esfera = 'NAO_GOVERNAMENTAL'
    else:
        esfera = 'NAO_GOVERNAMENTAL'
    dict_concedente = {
        "nome": str(dado['orgao_concedente']),
        "esferaAdministrativa": {"key": esfera}
    }
    if int(len(dado['cnpj_cpf_concedente'])) == 14:
        dict_concedente.update({
            "cnpj": str(dado['cnpj_cpf_concedente']),
            "tipo": {"key": "JURIDICA"}
        })
    else:
        dict_concedente.update({
            "cpf": str(dado['cnpj_cpf_concedente']),
            "tipo": {"key": "FISICA"},
        })
    retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_concedente))
    print(retorno)


def search_exists(headers, concedente):
    try:
        if int(len(concedente)) == 14:
            par_search = f'filter=cnpj = "{concedente}"&limit=20&offset=0&sort'
        else:
            par_search = f'filter=cpf = "{concedente}"&limit=20&offset=0&sort'
        url_fixa = 'https://convenios.cloud.betha.com.br/convenios/api/concedentes'
        retorno = requests.get(url=url_fixa, headers=headers, params=par_search)
        id_gerado_concedente = retorno.json()['content'][0]['id']
    except Exception:
        id_gerado_concedente = None
    finally:
        return id_gerado_concedente
