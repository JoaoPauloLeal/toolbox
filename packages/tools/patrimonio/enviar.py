import settings
# import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando projeto do sistema Contabil')
    global ano_inicial
    ano_inicial = input("Ano inicial para migração: ")
    global ano_final
    ano_final = input("Ano final para migração: ")
    global ano

    for ano in range(int(ano_inicial), int(ano_final) + 1):
        print("------------- INICIO PROJETO: " + str(ano) + " --------------")
        params_exec = {
            'clicodigo': '',
            'somente_pre_validar': False,
            'token': '',  # Token Service Layer
            'token-tela': '',  # Token oaut2 front end
            'app-context': '',  # Context oaut2 front end
            'user-access': '',  # User oaut2 front end
            'user_login': '',  # User login para pegar token do oaut2
            'user_pass': '',  # Pass login para pegar token do oaut2
            'entidade': "1",  # Nome exato da entidade a qual quer pegar o token do oaut2
            'exercicio': '2021',
            'ano': str(ano)
        }
        mensagem_inicio(params_exec)
        # interacao_cloud.verifica_token(params_exec['token'])
        # verifica_tabelas_controle()
        # buscar(params_exec, 'excluir-bens', ano, entidadeCloud='870')
        # buscar(params_exec, 'excluir-baixas', ano, entidadeCloud='870')
        buscar(params_exec, 'excluir-organograma-do-banco', ano, entidadeCloud='870')

        
        print("------------- TERMINO PROJETO: " + str(ano) + " -------------")
        ano = ano + 1


def enviar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    # print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
        # print(modulo)
        modulo.iniciar_processo_envio(params_exec, ano)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)


def buscar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    print(path_padrao)
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_busca'], 0)
        # print(modulo)
        modulo.iniciar_processo_busca(params_exec, ano)
        print(f'- Rotina de {tipo_registro} finalizada. '
              f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
    except NameError:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro + str(NameError))


def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução ferramenta {settings.BASE_ORIGEM}, utilizando os '
          f'seguintes parâmetros: \n- {params_exec}')


# def verifica_tabelas_controle():
#     pgcnn = model.PostgreSQLConnection()
#     pgcnn.verifica_tabelas_controle()
