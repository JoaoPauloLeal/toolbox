import settings
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    params_exec = {
        'somente_pre_validar': False,
        'token': '',  # Token Service Layer
        'token-tela': '',  # Token oaut2 front end
        'app-context': '',  # Context oaut2 front end
        'user-access': '',  # User oaut2 front end
        'user_login': '',  # User login para pegar token do oaut2
        'user_pass': '',  # Pass login para pegar token do oaut2
        'entidade': '',  # Nome exato da entidade a qual quer pegar o token do oaut2
        'db_name': ''  # Nome do banco para conexão sybase
    }
    mensagem_inicio(params_exec)
    # interacao_cloud.verifica_token(params_exec['token'])  # Somente ativo para token do Service Layer
    # verifica_tabelas_controle()  # Verificação das tabelas do postgre se estão criadas
    """
        Nomenclatura para as rotinas que deve ser seguida : 
            'sistema+cadastro+informação adicional(caso necessidade para diferenciar')
    """
    # buscar(params_exec, 'contabil_natureza_receita')
    # buscar(params_exec, 'contabil_deducao_receita')
    # buscar(params_exec, 'convenios_recebidos')
    # buscar(params_exec, 'frotas_organogramas_replica')
    # buscar(params_exec, 'tributos_pagamentos')
    # buscar(params_exec, 'tesouraria_arrecadacao')
    buscar(params_exec, 'oaut_betha')


def buscar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_busca'], 0)
        print(modulo)
        modulo.iniciar_processo_busca(params_exec)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)


def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução ferramenta {settings.BASE_ORIGEM}, utilizando os '
          f'seguintes parâmetros: \n- {params_exec}')


def verifica_tabelas_controle():
    pgcnn = model.PostgreSQLConnection()
    pgcnn.verifica_tabelas_controle()