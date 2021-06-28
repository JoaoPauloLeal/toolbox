import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'iniciar-obras'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        # print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
        #       f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        dict_enviar = {
            "id": None,
            "obra": {
                "id": i['id_obra']
            },
            "usuario": None,
            "dhHistorico": i['dtHistorico'] + ' 12:00:00',
            "situacaoNova": None,
            "situacaoNovaDesc": None,
            "idOrigemHistorico": None
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(params_exec, tipo_registro, params_exec.get('entidade'), str(i['i_obras'] + '/' + i['dtHistorico'])):
            id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/iniciar', json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Inicio de Obra',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_obras'] + '/' + i['dtHistorico'])
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')

