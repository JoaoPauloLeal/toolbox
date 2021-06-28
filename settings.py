"""
Definição de qual packages deve ser executado
Obs.: Manter padrão para criação de novos packages :
            nome simples: somente o nome objetivo do projeto
            nome composto: 3(tres) parametros (sistema, ambiente, banco)
"""
# BASE_ORIGEM = 'ipm_cloud_postgresql'
# BASE_ORIGEM = 'obras_cloud_sybase'
# BASE_ORIGEM = 'convenios_cloud_sybase'
# BASE_ORIGEM = 'tools'
BASE_ORIGEM = 'front'


'''Definição de qual projeto a ser executado dentro do pacote'''
# SISTEMA_ORIGEM = 'betha'
# SISTEMA_ORIGEM = 'arqjob'
# SISTEMA_ORIGEM = 'protocolo'
# SISTEMA_ORIGEM = 'frotas'
# SISTEMA_ORIGEM = 'livro_eletronico'
# SISTEMA_ORIGEM = 'compras'
# SISTEMA_ORIGEM = 'contabil'
# SISTEMA_ORIGEM = 'patrimonio'
# SISTEMA_ORIGEM = 'obras'
# SISTEMA_ORIGEM = 'contratos'
# SISTEMA_ORIGEM = 'convenios'
SISTEMA_ORIGEM = 'migracao_convenios'  # Obs. para obras/convenios (sistemas com migradores proprios internos)

# Para Service Layer
# TOKEN_MIGRACAO = '85666a9e-7576-4cd0-8cdf-5f96ee4f1cd8'


def iniciar_migracao():
    # start_logging()
    if BASE_ORIGEM == 'convenios_cloud_sybase':
        path = f'packages.{BASE_ORIGEM}.{SISTEMA_ORIGEM}'
        modulo = __import__(path, globals(), locals(), ['iniciar'], 0)
        modulo.iniciar()
    elif BASE_ORIGEM == 'front':
        path = f'{BASE_ORIGEM}.app.main'
        modulo = __import__(path, globals(), locals(), ['iniciar'], 0)
        modulo.iniciar()
    else:
        path = f'packages.{BASE_ORIGEM}.{SISTEMA_ORIGEM}.enviar'
        modulo = __import__(path, globals(), locals(), ['iniciar'], 0)
        modulo.iniciar()


# def start_logging():
#     import logging
#     from datetime import datetime
#     nome_arquivo = datetime.now().strftime("%d_%m_%y_%H_%M_%S")
#     logging.basicConfig(filename=f'log/LOG_{nome_arquivo}.log',
#                         format="%(levelname)s %(asctime)s  %(message)s",
#                         level=logging.INFO)
#     logging.info('Execução iniciada.')
