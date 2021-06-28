import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'classificacao-obra'
url = 'https://obras.betha.cloud/obras/api/classificacaoobras'

def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = [
        'Nenhum',
        'Abatedouro',
        'Barracão',
        'Creche',
        'Edifício Administrativo',
        'Escola / Colégio',
        'Hospital',
        'Posto de Saúde',
        'Unidade Habitacional',
        'Outros Edifícios',
        'Malha Viária Urbana',
        'Estrada Municipal',
        'Estrada Rural',
        'Obra de Arte Especial',
        'Abastecimento de Água',
        'Aterro Sanitário',
        'Canalização de Rio',
        'Cemitério',
        'Dragagem',
        'Esgoto',
        'Fundo de Vale',
        'Galeria Pluvial',
        'Outras Obras de Saneamento',
        'Parque ou Praça',
        'Abrigo de ônibus',
        'Iluminação Pública'
    ]

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)

def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for index, desc in enumerate(dict_dados, start=0):
        dict_enviar = {
            'descricao': desc,
            'id': None
        }

        json_envio = json.dumps(dict_enviar)

        if not db.checa_existe_registro(params_exec, tipo_registro,'null', str(index)):
            id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(1),
                    tipo_registro,
                    'Cadastro de Classificação de Obras',
                    str(id_registro),
                    str(params_exec['entidade']),
                    str(index),
                    desc
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')



