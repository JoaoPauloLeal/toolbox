import settings
import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime


def iniciar():
    print(':: Iniciando migração do sistema Compras')
    global ano_inicial
    # ano_inicial = input("Ano inicial para migração: ")
    ano_inicial = 1
    global ano_final
    # ano_final = input("Ano final para migração: ")
    ano_final = 1
    global ano

    for ano in range(int(ano_inicial), int(ano_final) + 1):
        print("------------- INICIO MIGRAÇÃO DO ANO: " + str(ano) + " --------------")
        params_exec = {
            'clicodigo': '2016',
            'somente_pre_validar': False,
            'token': settings.TOKEN_MIGRACAO,  # Token Service Layer #ignoreline,
            'token-tela': '',  # Token oaut2 front end #ignoreline,
            'app-context': '',  # Context oaut2 front end #ignoreline,
            'user-access': '',  # User oaut2 front end #ignoreline,
            'user_login': '',  # User login para pegar token do oaut2 #ignoreline,
            'user_pass': '',  # Pass login para pegar token do oaut2 #ignoreline,
            'entidade': '1',  # Nome exato da entidade a qual quer pegar o token do oaut2 #ignoreline,
            'ano': str(ano),
            'exercicio': 2016

        }
        mensagem_inicio(params_exec)
        interacao_cloud.verifica_token(params_exec['token'])
        # verifica_tabelas_controle()

        # buscar(params_exec, 'alterar_config_exercicios', ano)
        # buscar(params_exec, 'busca_cnpj_entidades', ano)
        # buscar(params_exec, 'analisa_configuracao_exercicios', ano)
        # buscar(params_exec, 'analisa_fornecedor', ano)
        # buscar(params_exec, 'analisa_comissao_licitacao', ano)
        # buscar(params_exec, 'deleta_divisao_item_entidades', ano)
        # buscar(params_exec, 'busca_id_gerado', ano)
        # buscar(params_exec, 'proposta_participante_situacao', ano)
        buscar(params_exec, 'proposta_participante_processo', ano)
        # buscar(params_exec, 'participante_sem_proposta', ano)
        # buscar(params_exec, 'anl_processos', ano)
        # buscar(params_exec, 'busca_solicitacoes_compra', ano)
        # buscar(params_exec, 'busca-administrativos-two', ano)
        # buscar(params_exec, 'busca-administrativos', ano)
        # buscar(params_exec, 'busca_materiais', ano)
        # buscar(params_exec, 'anl_processos', ano)

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


# def verifica_tabelas_controle():
#     pgcnn = model.PostgreSQLConnection()
#     pgcnn.verifica_tabelas_controle()
