import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime
import pandas as pd
from packages.ipm_cloud_postgresql.model import new_connection as conn_desktop

tipo_registro = 'liquidacoes'
sistema = 1
limite_lote = 1000
url = "https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/liquidacoes"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    lista_dados_enviar = []
    contador = 0
    # print("EMPACOU 0")
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)

    for item in req_res:
        # print(item)
        # print("EMPACOU 0")
        idGerado = item['idGerado']
        #
        content = item['content']
        empenho = content['empenho']
        data = datetime.strptime(str(content['data']), '%Y-%m-%d')
        if str(data.year) == '2021':
            chave_dsk6 = 'LIQ'
        else:
            chave_dsk6 = 'RESTO'
        lista_conteudo_retorno.append({
            'id_gerado': idGerado['id'],
            'chave_dsk1': '8',
            'chave_dsk2': data.year,
            'chave_dsk3': empenho['id'],
            'chave_dsk4': 'liq anterior',
            'chave_dsk5': data.year,
            'chave_dsk6': chave_dsk6
        })
        contador += 1

    for item in lista_conteudo_retorno:
        try:
            # params_exec = {
            #     'i_chave_dsk1': item['chave_dsk1'],
            #     'i_chave_dsk2': item['chave_dsk2'],
            #     'i_chave_dsk3': item['chave_dsk3'],
            #     'i_chave_dsk4': item['chave_dsk4'],
            #     'i_chave_dsk5': item['chave_dsk5'],
            #     'i_chave_dsk6': item['chave_dsk6']
            # }
            params_exec = {
                'i_chave_dsk1': 3,
                'i_chave_dsk2': 2019,
                'i_chave_dsk3': 759,
                'i_chave_dsk4': 3,
                'i_chave_dsk5': 5,
                'i_chave_dsk6': 'LIQANT'
            }
            query = model.get_consulta(params_exec, 'liquidacoes.sql')
            # dataframe = None
            with conn_desktop(dbname='sapoDionisio') as connSybase:
                cursor_sybase = connSybase.cursor()
                print('EMPACOU 0')
                pd.read_sql_query(sql=query, con=connSybase)
                # cursor_sybase.execute(query)
                print('EMPACOU 1')
                # connSybase.commit()
            print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
        except Exception as error:
            print(f'Erro ao executar função {tipo_registro}. {error}')
        finally:
            return dados_sybase
        print(item)

    # print(lista_dados_enviar)
    print('- Busca de dados finalizado.')
