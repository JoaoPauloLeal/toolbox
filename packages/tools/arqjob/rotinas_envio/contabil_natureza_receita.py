import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import requests
from packages.ipm_cloud_postgresql.model import new_connection as conn_desktop

sistema = 667
tipo_registro = 'contabil_natureza_receita'
# url = 'https://api.dados.protocolo.betha.cloud/protocolo/processos/dados/api/andamentos'


def iniciar_processo_busca(params_exec, *args, **kwargs):
    dados_sybase = coletar_dados_sybase(params_exec)
    dados_update = analisa_dados_sybase(params_exec, dados_sybase)
    update_dados(params_exec, dados_update)


def coletar_dados_sybase(params_exec):
    print('- Iniciando a consulta dos dados no banco Sybase.')
    contador = 0
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql')
        dados_sybase = []

        with conn_desktop(dbname=params_exec['db_name']) as connSybase:
            cursor_sybase = connSybase.cursor()
            dados = cursor_sybase.execute(query).fetchall()
            # print(cursor_sybase)
            for item in dados:
                item2 = item
                dict_dados = {
                    "sistema": item[0],
                    "tipo_registro": item[1],
                    "hash_chave_dsk": item[2],
                    "descricao_tipo_reg": item[3],
                    "id_gerado": item[4],
                    "i_chave_dsk1": item[5],
                    "i_chave_dsk2": item[6]
                }
                contador += 1
                dados_sybase.append(dict_dados)

        print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return dados_sybase


def analisa_dados_sybase(params_exec, dados):
    url_fonte = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/planejamento/naturezas-receitas'
    campos = 'configuracao(id), numero, descricao, tipo, marcadoresNatureza'
    lista_dados = []
    cont = 0

    for item in dados:
        natureza = str(item['i_chave_dsk1'])
        cont_desc = len(str(natureza))
        for sub_item in range(int(cont_desc)):
            slice_object = slice(cont_desc)
            numero = natureza[slice_object]
            numero_teste = numero.ljust(14, '0')
            # print(numero_teste)
            criterio = f"numero = '{numero_teste}' and tipo = 'ANALITICO'"
            print(criterio)
            cont_desc -= 1
            registro_cloud = interacao_cloud.busca_api_fonte_dados_compras(params_exec, url=url_fonte,
                                                                                        campos=campos,
                                                                                        criterio=criterio)
            print(natureza[slice_object])
            print(registro_cloud)
            if len(registro_cloud):
                for item_sub in registro_cloud:
                    configuracao = item_sub['configuracao']
                    lista_dados.append({
                        'id': str(item_sub['id']),
                        'configuracaoId': configuracao['id'],
                        'numero': str(item_sub['numero']),
                        'descricao': str(item_sub['descricao']),
                        'tipo': str(item_sub['tipo'])
                    })
                cont += 1
                break
    print(f"Consultas com sucesso na fonte de dados: {cont}")
    return lista_dados


def update_dados(params_exec, dados):
    contador = 0
    dados_update = []
    urlPut = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/naturezas-receitas'

    for item in dados:
        dict_dist = {
            'idIntegracao': f"CNR{str(item['id'])}",
            'idGerado': {
                'id': int(item['id'])
            },
            'content': {
                'configuracao': {
                    'id': int(item['configuracaoId'])
                },
                'numero': str(item['numero']),
                'descricao': str(item['descricao']),
                'tipo': 'SINTETICO'
            }
        }
        dados_update.append(dict_dist)
        contador += 1

    cont_nat_rec_up = 0
    for item in dados_update:
        data_update = json.dumps(item)
        header = {'authorization': f'bearer {params_exec["token"]}', 'content-type': 'application/json'}
        retorno_req = requests.put(url=urlPut, headers=header, data=data_update)
        print("Sucesso lote:", retorno_req.text, "Status Code", retorno_req.status_code)
        cont_nat_rec_up += 1

    print(f'Realizado update de  : {cont_nat_rec_up}')
