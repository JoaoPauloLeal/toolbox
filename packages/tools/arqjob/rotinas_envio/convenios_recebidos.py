from decimal import Decimal

import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
# import json
import time
import simplejson as json
import requests
from packages.ipm_cloud_postgresql.model import new_connection as conn_desktop

sistema = 667
tipo_registro = 'convenios_recebidos'
url_concedente = 'https://convenios.cloud.betha.com.br/convenios/api/concedentes'
url_concedentes = 'https://convenios.cloud.betha.com.br/convenios/api/concedentes?filter=(cnpj+%3D+%2201227588000183%22)&limit=20&offset=0&sort='


def iniciar_processo_busca(params_exec, *args, **kwargs):
    headers = {'authorization': f'bearer {params_exec["token-tela"]}', 'user-access': f'{params_exec["user-access"]}',
               'content-type': 'application/json'}
    convenios_recebidos = colect_convenios_recebidos(params_exec)
    for item in convenios_recebidos:
        # COMEÇO CONCEDENTE
        if not search_concedente_exists(headers, item[59]):
            print("NÃO ACHOU")
            register_concedente(headers, item)
            id_concedente = search_concedente_exists(headers, item[59])
            print(id_concedente)
        else:
            id_concedente = search_concedente_exists(headers, item[59])
            print(f"Info: Cpf ou Cnpj: {item[59]}, de concedente já cadastrado, id gerado: {id_concedente}.")
        # FIM CONCEDENTE


def register_concedente(headers, convenios_recebido):
    if int(convenios_recebido[19]) == 2:
        esfera = 'FEDERAL'
    dict_concedente = {
        "nome": str(convenios_recebido[21]),
        "esferaAdministrativa": {"key": esfera}
    }
    if int(len(convenios_recebido[59])) == 14:
        dict_concedente.update({
            "cnpj": str(convenios_recebido[59]),
            "tipo": {"key": "JURIDICA"}
        })
    else:
        dict_concedente.update({
            "cpf": str(convenios_recebido[59]),
            "tipo": {"key": "FISICA"},
        })
    dado = json.dumps(dict_concedente)
    retorno = requests.post(url=url_concedente, headers=headers, data=dado)
    print(retorno)


def search_concedente_exists(headers, concedente):
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


def colect_convenios_recebidos(params_exec):
    print('- Iniciando a consulta dos dados no banco Sybase.')
    contador = 0
    try:
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        dados_sybase = []

        with conn_desktop(dbname=params_exec['db_name']) as connSybase:
            cursor_sybase = connSybase.cursor()
            dados = cursor_sybase.execute(query).fetchall()
            # print(cursor_sybase)
            for item in dados:
                dict_dados = item
                contador += 1
                dados_sybase.append(dict_dados)

        print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return dados_sybase
