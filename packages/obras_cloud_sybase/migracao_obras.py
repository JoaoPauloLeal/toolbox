import settings
import bth.db_connector as db
from datetime import datetime


def iniciar():
    print('Iniciando migração do sistema obras.')
    #  db.teste_conexao()

    params_exec = {
        'entidade': 7,
        'data_inicial': '2021-01-01',
        'data_final': '2021-12-31',
        'ESTADO': 'SC',
        'token':'',
        'appcontext':'=',
        'useraccess':'',
        'db_name': 'Contabil',  # Nome do banco para conexão sybase
        'db_user': 'desbth',  # Nome do banco para conexão sybase
        'db_pw': '',  # Nome do banco para conexão sybase
        'db_host': 'localhost',  # Nome do banco para conexão sybase
        'db_port': '9002'  # Nome do banco para conexão sybase
    }

    """ Inicia envio obras """
    # Dados cadastrais
    # enviar(params_exec, 'classificacao')
    # enviar(params_exec, 'categoria')
    # enviar(params_exec, 'tipos-obras')
    # enviar(params_exec, 'tipo-responsavel')
    # enviar(params_exec, 'tipo-responsabilidade-tecnica')
    # enviar(params_exec, 'motivo-paralisacao')
    # enviar(params_exec, 'tipo-medicao')
    # enviar(params_exec, 'responsaveis')
    # enviar(params_exec, 'matriculas-cei')

    # Dados das obras
    # enviar(params_exec, 'obras')
    # enviar(params_exec, 'obra-matriculas-cei') # Não executado para SC
    # enviar(params_exec, 'contratos-obras')
    # enviar(params_exec, 'obra-art')              # Adiciona o 1° responsavel cadastrado no sapo (orçamentario) p/ SC
    # enviar(params_exec, 'iniciar-obras')
    # enviar(params_exec, 'medicoes')
    # enviar(params_exec, 'orcamentos')
    # enviar(params_exec, 'paralisacoes')
    enviar(params_exec, 'conclusoes')


def enviar(params_exec, tipo_registro, *args, **kwargs):
    if params_exec['ESTADO'] == 'SC' and tipo_registro == 'responsaveis':
        tipo_registro = 'responsaveis_sc'
    print(f'\n:: Iniciando execução do cadastro {tipo_registro}')
    tempo_inicio = datetime.now()
    modulo = __import__(f'packages.obras_cloud_sybase.rotinas.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)
    print(f'- Rotina de {tipo_registro} finalizada. '
          f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')
