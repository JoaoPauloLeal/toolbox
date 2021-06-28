import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'categoria-obra'
url = 'https://obras.betha.cloud/obras/api/categoriasobras'

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = {
        '0': 'Outros',
        '1': 'Ampliação',
        '2': 'Construção',
        '3': 'Recuperação ou Manutencao',
        '4': 'Reforma',
        '5': 'Obra de infra-estrutura',
        '6': 'Obra rodoviaria',
        '7': 'Obra de arte Rodoviária',
        '8': 'Pavimentação',
        '9': 'Projeto Completo',
        'A': 'Projeto Arquitetônico',
        'B': 'Projeto estrutural',
        'C': 'Projeto Elétrico Telefonia Lógico',
        'D': 'Projeto Hidro-Sanitário',
        'E': 'Ampliação e Recuperação',
        'F': 'Ampliação e Reforma',
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

        if not db.checa_existe_registro(params_exec, tipo_registro, 'null', str(key)):
            id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(1),
                    tipo_registro,
                    'Cadastro de Categorias de Obras',
                    str(id_registro),
                    str(params_exec['entidade']),
                    str(key),
                    desc
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')



