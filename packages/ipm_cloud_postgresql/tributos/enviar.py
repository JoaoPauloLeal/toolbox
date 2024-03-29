"""
    ROTINA PRINCIPAL QUE É CHAMADA ARA O ENVIO
"""
import settings
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud


def iniciar():
    params_exec = {
        'somente_pre_validar': False,
        'token': '',
        'ano': '',
        'clicode': ''
    }
    # Exibe mensagem inicial de início de execução
    mensagem_inicio(params_exec)

    # Realiza a validação do token informado
    interacao_cloud.verifica_token(params_exec['token'])

    # Verifica existência de tabelas e funções de controle
    verifica_tabelas_controle()

    # Inicia chamadas de rotinas de envio de dados


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do assunto {tipo_registro}')
    path_padrao = 'packages.ipm_cloud_postgresql.contabil.rotinas_envio'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)


def mensagem_inicio(params_exec):
    print(f':: Iniciando execução da migração do sistema {settings.BASE_ORIGEM} para Betha Cloud utilicando os '
          f'seguintes parâmetros: \n- {params_exec}')


def verifica_tabelas_controle():
    pgcnn = model.PostgreSQLConnection()
    pgcnn.verifica_tabelas_controle()
