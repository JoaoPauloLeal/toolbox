import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'obra-matriculas-cei'

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
        dict_enviar = {
            "dataMatriculaObra": i['data_cadastro'],
            "matriculaCei": {
                "id": i['id_matricula']
            }
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(tipo_registro, str(i['i_obras'] + '/' + i['i_matricula_cei'])):
            id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["id_obra"]}/matriculasobras', json_envio)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(settings.SISTEMA),
                    tipo_registro,
                    'Cadastro de Matricula CEI',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_obras'] + '/' + i['i_matricula_cei'])
                ]
                conn = db.conectar()
                db.regista_controle_migracao(conn, registro)
            else:
                save_path = os.path.realpath('logs')
                path = os.path.join(save_path, f"{tipo_registro}.txt")
                f = open(path, "a")
                f.write(f"JSON - {json_envio}\nErro - {mensagem_erro}\n\n")
                f.close()
