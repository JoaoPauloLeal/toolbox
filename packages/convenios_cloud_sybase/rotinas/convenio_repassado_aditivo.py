import simplejson as json
import requests
import bth.db_connector as db
from datetime import datetime, date
import re

sistema = 1
tipo_registro = 'convenio_repassado_aditivo'


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
        numero_convenio = re.sub(u'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ: ]', '', str(item['numero_convenio']))
        dict_dado = {
            "aditivo": str(numero_convenio),
            "dataTermino": str(item['periodo_fim']),
            "justificativa": str(item['justificativa']),
            "tipoAditivo": {
                "id": int(item['tipo_aditivo'])
            },
            "valorContrapartida": float(item['valor_contrapartida']),
            "valorRepasse": float(item['valor_repassado'])
        }
        if item['data_assinatura'] is not None:
            dict_dado.update({
                "dataAssinatura": str(item['data_assinatura'])
            })
        if item['responsavel'] is not None:
            dict_dado.update({
                "atoAutorizativo": {
                    "id": int(item['ato_autorizativo'])
                }
            })
        print(item['id_gerado_convenio'])
        url = f"https://convenios.cloud.betha.com.br/convenios/api/convenios-repassados/{item['id_gerado_convenio']}/aditivos-convenios"
        print(json.dumps(dict_dado))
        retorno = requests.post(url=url, headers=headers, data=json.dumps(dict_dado))
        id_gerado = search_exists(headers, item)
        if id_gerado is not None:
            registro = [
                str(params_exec['sistema']),
                tipo_registro,
                'Cadastro de convenio repassado aditivo',
                str(id_gerado),
                str(params_exec['id_entidade']),
                str(item['numero_convenio']),
                str(item['justificativa']),
                str(item['periodo_inicio']),
                str(item['periodo_fim'])
            ]
            register_controle_migracao(params_exec, registro)
        print(retorno)


def search_exists(headers, dado):
    url_convenios_repassados_consolidados_aditivo = f"https://convenios.cloud.betha.com.br/convenios/api/convenios-repassados/{dado['id_gerado_convenio']}/aditivos-convenios/"
    try:
        retorno = requests.get(url=url_convenios_repassados_consolidados_aditivo, headers=headers)
        for item in retorno.json():
            if str(item['convenio']['id']) == str(dado['id_gerado_convenio']) and str(item['dataTermino']) == str(
                    dado['periodo_fim']) and float(item['valorRepasse']) == float(dado['valor_repassado']):
                id_gerado = item['id']
            else:
                id_gerado = None
    except Exception:
        id_gerado = None
    finally:
        return id_gerado


def register_controle_migracao(params_exec, registro):
    conn = db.conectar(params_exec)
    db.regista_controle_migracao(conn, registro)
