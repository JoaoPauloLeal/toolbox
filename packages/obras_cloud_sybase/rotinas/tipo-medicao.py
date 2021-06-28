import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'tipo-medicao'
url = 'https://obras.betha.cloud/obras/api/tiposmedicao'

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = {
        '1': 'Execução Indireta-Contrato',
        '2': 'Execução Indireta-Aditivo',
        '3': 'Execução Direta',
        'M': 'Medição',
        'C': 'Termo de conclusão',
        'P': 'Paralisação',
        'R': 'Reinício'
    }

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)

def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for key, desc in dict_dados.items():
        dict_enviar = {
            'descricao': desc,
            'id': None
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(params_exec, tipo_registro,'null', str(key)):
            id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Tipo de Medição',
                    str(id_registro),
                    str(params_exec['entidade']),
                    str(key),
                    desc
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')



