import bth.db_connector as db
import bth.cloud_connector as cloud
from datetime import datetime
import json
import settings
import os

tipo_registro = 'tipos-obra'
url = 'https://obras.betha.cloud/obras/api/tiposobras'

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        # print(query)
        # conn = db.conectar(params_exec)
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        # print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).'
        #       f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        dict_enviar = {
            'campoAdicional': None,
            'descricao': i['nome'],
            'classificacoes': None
        }

        if i['i_classificacao_obra']:
            dict_enviar |= {
                'classificacao': [{
                    'id': i['i_classificacao_obra'],
                    'descricao': i['classificacao_obra_desc']
                }]
            }

        json_envio = json.dumps(dict_enviar)
        if not db.checa_existe_registro(params_exec, tipo_registro,'null', str(i['i_tipo_obras'])):
            id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(1),
                    tipo_registro,
                    'Cadastro de Tipos de Obra',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_tipo_obras']),
                    str(i['nome']).replace("'",'.')
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')
