import bth.get_token as bth

tipo_registro = 'contratos-obras'


def get_token_access(params_exec, *args, **kwargs):
    print('Iniciando consulta de credenciais...')
    return bth.get_token_access(params_exec)
