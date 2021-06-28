import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime
import requests

tipo_registro = 'medicoes'

def procura_contrato(obra, num_contrato):
    headers = {
        'authorization': f'Bearer {settings.USER_TOKEN}',
        'app-context': settings.APP_CONTEXT,
        'user-access': settings.USER_ACCESS,
    }

    r = requests.get(url=f'https://obras.betha.cloud/obras/api/obras/{obra}/contratosobras', headers=headers, params={
        'filter': f'(nroAnoContrato like "{num_contrato}%")'
    })

    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))
        if len(retorno['content']) > 0:
            return retorno['content'][0]

    return None

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
        conn = db.conectar()
        df = db.consulta_sql(conn, query, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto.to_dict('records')
    for i in dict_dados:
        contrato = procura_contrato(i['id_obra'], i['numero_contrato'])
        dict_enviar = {
            "arquivos": [],
            "dataInicial": i['data_medicao'],
            "dataFinal": i['data_medicao'],
            "percentFisico": i['percentual_fisico'],
            "responsavelTecnico": {
                "id": i['id_responsavel']
            },
            "dataMedicao": i['data_medicao'],
            "tipoMedicao": {
                "id": i['id_tipo_medicao']
            },
            "obraContratacao": {
                "id": contrato['id'] if contrato else None
            },
            "observacao": i['observacao']
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(tipo_registro, str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras'])):
            id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/medir', json_envio, True, 'medir')
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(settings.SISTEMA),
                    tipo_registro,
                    'Cadastro de Medições',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras'])
                ]
                conn = db.conectar()
                db.regista_controle_migracao(conn, registro)
            else:
                save_path = os.path.realpath('logs')
                path = os.path.join(save_path, f"{tipo_registro}.txt")
                f = open(path, "a")
                f.write(f"ID OBRA: {i['id_obra']}\nID DSK: {i['i_obras']}\nJSON - {json_envio}\nErro - {mensagem_erro}\n\n")
                f.close()