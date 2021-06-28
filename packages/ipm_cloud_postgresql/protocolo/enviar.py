import settings
import packages.ipm_cloud_postgresql.model as model
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Protocolo')
    global ano_inicial
    ano_inicial = input("Ano inicial para migração: ")
    global ano_final
    ano_final = input("Ano final para migração: ")
    global ano

    for ano in range(int(ano_inicial), int(ano_final) + 1):
        print("------------- INICIO MIGRAÇÃO DO ANO: " + str(ano) + " --------------")
        params_exec = {
            'clicodigo': '2016',
            'somente_pre_validar': False,
            'token': '',  # Token Service Layer
            'token-tela': '',  # Token oaut2 front end
            'app-context': '',  # Context oaut2 front end
            'user-access': '',  # User oaut2 front end
            'user_login': '',  # User login para pegar token do oaut2
            'user_pass': '',  # Pass login para pegar token do oaut2
            'entidade': "",  # Nome exato da entidade a qual quer pegar o token do oaut2
            'ano': str(ano)
        }
        # for i in tqdm(range(1000)):
        #     sleep(0.01)
        # tqdm(range(1000))
        mensagem_inicio(params_exec)
        # interacao_cloud.verifica_token(params_exec['token'])
        verifica_tabelas_controle()

        # enviar(params_exec, 'buscaIdGerado', ano)
        # enviar(params_exec, 'documento', ano)
        # enviar(params_exec, 'assuntoDocumento', ano)  # não precisa já existe no banco
        #
        # enviar(params_exec, 'classificacao', ano)
        # enviar(params_exec, 'assunto', ano)
        # enviar(params_exec, 'organograma', ano)

        # enviar(params_exec, 'protocolos', ano)  # juntos
        # enviar(params_exec, 'processos', ano)  # juntos
        # enviar(params_exec, 'processo-organogramas-usuarios', ano)  # juntos
        # enviar(params_exec, 'andamentos', ano)
        # 
        # buscar(params_exec, 'buscaProcessosNew', ano)

        # enviar(params_exec, 'buscaProcessosFonte', ano)
        # enviar(params_exec, 'andamentosBusca', ano)
        # enviar(params_exec, 'andamentosBuscaDiferente', ano)
        # enviar(params_exec, 'andamentos_busca_fonte')
        # enviar(params_exec, 'processos_busca_fonte')
        enviar(params_exec, 'correcao_organogramas_usuario_processo')

        # enviar(params_exec, 'processosBusca', ano)
        # buscar(params_exec, 'buscaProcessosExtra', ano)
        # buscar(params_exec, 'buscaProcessosOrganogramasUsuariosExtra', ano)
        # enviar(params_exec, 'correcao_organogramas', ano)
        # 
        # enviar(params_exec, 'arquivamentos', ano)
        # # enviar(params_exec, 'arquivamentosExcecao', ano)  # ter cuidado, verificar se ainda sim o total não bate
        # # com o sql anterior
        # enviar(params_exec, 'pareceres', ano)
        # enviar(params_exec, 'entrega-documentos', ano)
        # enviar(params_exec, 'processo-parte-interessadas', ano)

        # enviar(params_exec, 'parecer-anexos', ano)  # Anexos do parecer
        # enviar(params_exec, 'processos-anexo', ano)  # Anexos do documento do processo
        print("------------- TERMINO MIGRAÇÃO DO ANO: " + str(ano) + " -------------")
        ano = ano + 1


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do serviço {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.{settings.SISTEMA_ORIGEM}.rotinas_envio'
    try:
        modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
        # print(modulo)
        modulo.iniciar_processo_envio(params_exec)
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
