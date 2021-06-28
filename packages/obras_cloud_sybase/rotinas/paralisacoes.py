import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
import requests
from datetime import datetime

tipo_registro = 'paralisacoes'

headers = {
    'authorization': f'Bearer {settings.USER_TOKEN}',
    'app-context': settings.APP_CONTEXT,
    'user-access': settings.USER_ACCESS,
    'Content-Type': 'application/json'
}

def is_obra_paralisada(id_obra):
    r = requests.get(url=f'https://obras.betha.cloud/obras/api/obras/{id_obra}/', headers=headers)

    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))
        if retorno['situacao'] == 'PARALISADA':
            return True

    return False

def reinicia_obra_paralisada(id_obra, data_medicao):
    print(f'id obra: {id_obra}')
    print(f'data: {data_medicao}')
    r = requests.post(url=f'https://obras.betha.cloud/obras/api/obras/{id_obra}/reiniciar', headers=headers, data={
        'dataReinicio': data_medicao
    })

    if r.ok:
        return True
    else:
        save_path = os.path.realpath('logs')
        path = os.path.join(save_path, f"{tipo_registro}.txt")
        f = open(path, "a")
        f.write(f"ID OBRA: {id_obra}: Erro ao reiniciar obra paralisada\nErro: {r.content}")
        f.close()
        return False

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
        df = db.consulta_sql(query, index_col='id')
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
        dict_enviar = {
            "dataObraParalisacao": i['data_medicao'],
            "responsavelTecnico": {
                "id": i['id_responsavel']
            },
            "motivoParalisacao": {
                "id": i['id_motivo']
            },
            "observacao": i['observacao']
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(tipo_registro, str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras'])):
            # if is_obra_paralisada(i['id_obra']):
            #     if not reinicia_obra_paralisada(i['id_obra'], i['data_medicao']):
            #         print('Um erro critico ocorreu')
            #         return

            id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/paralisarobra', json_envio)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Paralisações',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_medicao_acompanhamento']) + '/' + str(i['i_obras'])
                ]
                conn = db.conectar()
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')

