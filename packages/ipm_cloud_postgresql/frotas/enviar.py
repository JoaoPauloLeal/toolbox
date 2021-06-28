import settings
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Protocolo')
    global ano_inicial
    # ano_inicial = 2000
    ano_inicial = input("Ano inicial para migração: ")
    global ano_final
    # ano_final = 2005
    ano_final = input("Ano final para migração: ")
    global ano

    for ano in range(int(ano_inicial), int(ano_final) + 1):
        print("------------- INICIO MIGRAÇÃO DO ANO: " + str(ano) + " --------------")
        params_exec = {
            'clicodigo': '2016',
            'somente_pre_validar': False,
            'token': '',
            'token': '',  # Token base oficial biguaçu
            'ano': str(ano)
        }
        mensagem_inicio(params_exec)
        interacao_cloud.verifica_token(params_exec['token'])
        verifica_tabelas_controle()

        enviar(params_exec, 'postmultpart', ano)

        # buscar(params_exec, 'buscaPessoas', ano)
        # buscar(params_exec, 'buscaTiposVeiculoEquipamento', ano)
        # buscar(params_exec, 'buscaUnidadesMedida', ano)
        # buscar(params_exec, 'buscaMateriaisServicos', ano)
        # buscar(params_exec, 'buscaOrganogramas', ano)
        # buscar(params_exec, 'buscaMotoristas', ano)
        # buscar(params_exec, 'buscaFornecedores', ano)
        # buscar(params_exec, 'buscaMateriaisEspecificacao', ano)

        # enviar(params_exec, 'funcionario', ano)
        # enviar(params_exec, 'veiculoEquipamento', ano)
        # enviar(params_exec, 'ordemAbastecimento', ano)

        print("------------- TERMINO MIGRAÇÃO DO ANO: " + str(ano) + " -------------")
        ano = ano + 1


def enviar(params_exec, tipo_registro, ano, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    print(path_padrao)
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
    except:
        print("Erro ao executar rotina para o tipo de registro: " + tipo_registro)


def mensagem_inicio(params_exec):
    print(f'\n:: Iniciando execução ferramenta {settings.BASE_ORIGEM}, utilizando os '
          f'seguintes parâmetros: \n- {params_exec}')


def verifica_tabelas_controle():
    pgcnn = model.PostgreSQLConnection()
    pgcnn.verifica_tabelas_controle()
