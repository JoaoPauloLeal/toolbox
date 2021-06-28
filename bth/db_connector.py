import pyodbc
import pandas as pd
import settings


def checa_existe_registro(params_exec, tipo_registro, chave_dsk1, chave_dsk2):
    conn = conectar(params_exec)
    sql = f"SELECT 1 as id, * FROM bethadba.controle_migracao_registro where tipo_registro='{tipo_registro}'"
    if chave_dsk1 != 'null':
        sql += f" AND i_chave_dsk1 = '{chave_dsk1}'"
    if chave_dsk2 != 'null':
        sql += f" AND i_chave_dsk2 = '{chave_dsk2}'"
    # print(sql_teste)
    df = consulta_sql(sql,params_exec, index_col='id')
    # dict_dados = df
    if len(df) > 0:
        return True
    return False


def conectar(params_exec):
    try:
        conn = pyodbc.connect(f"DSN={params_exec['db_name']};UID={ params_exec['db_user']};PWD={ params_exec['db_pw']}")
        return conn
    except pyodbc.Error as error:
        print(error)


def consulta_sql(sql, params_exec, **kwargs):
    conn = conectar(params_exec)
    dataframe = None
    try:
        if 'index_col' in kwargs:
            dataframe = pd.read_sql_query(sql, conn, index_col=kwargs['index_col'])
        else:
            dataframe = pd.read_sql_query(sql, conn)
    except Exception as error:
        print("Erro ao executar função 'SybaseConnection:consulta_sql'.", error)
    finally:
        return dataframe.to_dict('records')


def execute_sql(sql, params_exec, conn=None):
    # print(sql)
    if not conn:
        conn = conectar(params_exec)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as error:
        print(f"Erro ao executar função 'execute_sql'.{sql}", error)


def regista_controle_migracao(conn, lista_dados):
    try:
        params = '\',\''.join(lista_dados)
        sql = f"call bethadba.dbf_insere_controle_migracao_registro('{params}')"
        # print('sql', sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as error:
        print("Erro ao executar função 'PostgreSQLConnection:consulta_sql'.", error)


def registra_controle_migracao_ocor(conn, lista_dados):
    try:
        params = '\',\''.join(lista_dados)
        sql = f'call bethadba.dbf_insere_controle_migracao_registro_ocor(\'{params}\')'
        # print('sql', sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.commit()
    except Exception as error:
        print("Erro ao executar função 'PostgreSQLConnection:consulta_sql'.", error)


def get_consulta(params_exec, assunto):
    texto_consulta = None
    try:
        # logging.info(f'Iniciando busca de consulta para o assunto {assunto}.')

        # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
        # print(open(get_path(f'{assunto}'), "r", encoding='utf-8').read())
        # print(get_path(f'{assunto}'))
        texto_consulta = open(get_path(f'{assunto}'), "r", encoding='utf-8').read()
        # Aplica os parâmetros de usuário na consulta obtida
        texto_consulta = aplica_parametros(params_exec, texto_consulta)
        # print(texto_consulta)

    except Exception as error:
        print("Erro ao executar função 'get_consulta'.", error)

    finally:
        return texto_consulta


def get_consulta_conv(params_exec, assunto):
    texto_consulta = None
    try:
        # logging.info(f'Iniciando busca de consulta para o assunto {assunto}.')

        # Obtém o texto do arquivo assunto.sql na pasta 'sql_padrao'
        # print(open(get_path(f'{assunto}'), "r", encoding='utf-8').read())
        texto_consulta = open(get_path_conv(f'{assunto}'), "r", encoding='utf-8').read()
        # Aplica os parâmetros de usuário na consulta obtida
        texto_consulta = aplica_parametros(params_exec, texto_consulta)
        # print(texto_consulta)

    except Exception as error:
        print("Erro ao executar função 'get_consulta'.", error)

    finally:
        return texto_consulta


def get_path(assunto):
    path_padrao = f'packages/{settings.BASE_ORIGEM}/sql/'
    # path_custom = f'packages/{settings.BASE_ORIGEM}/{settings.SISTEMA_ORIGEM}/sql_custom/'
    # existe_customizado = os.path.isfile(path_custom + assunto)
    existe_customizado = False
    # path = (path_custom if existe_customizado else path_padrao) + assunto
    path = path_padrao + assunto
    return path


def get_path_conv(assunto):
    path_padrao = f'packages/{settings.BASE_ORIGEM}/sql/'
    path = path_padrao + assunto
    return path


def aplica_parametros(params_exec, t):
    texto_consulta = t
    try:
        for param in params_exec:
            texto_consulta = texto_consulta.replace(('{{' + param + '}}'), str(params_exec.get(param)))

    except Exception as error:
        print("Erro ao executar função 'aplica_parametros'.", error)

    finally:
        return texto_consulta


def busca_id_entidade_migracao(params_exec):
    conn = conectar(params_exec)
    sql=f"select * from bethadba.controle_migracao_registro where tipo_registro = 'entidades' and i_chave_dsk1 = {params_exec['entidade']}"
    idgerado = ''
    for x in consulta_sql(sql, params_exec, index_col='sistema'):
        idgerado = x['id_gerado']
    return idgerado