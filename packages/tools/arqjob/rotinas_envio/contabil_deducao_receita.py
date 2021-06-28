from decimal import Decimal

import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
# import json
import time
import simplejson as json
import requests
from packages.ipm_cloud_postgresql.model import new_connection as conn_desktop

sistema = 667
tipo_registro = 'contabil_deducao_receita'
url_receita_planejamento = 'https://planejamento-sl.cloud.betha.com.br/planejamento/service-layer/v2/api/receitas-ldo'
url_natureza_contabil = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/naturezas-receitas/'
url_recurso_contabil = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/recursos/'
url_deducao_contabil = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/planejamento/deducoes-receitas'
url_lote_planejamento = 'https://planejamento-sl.cloud.betha.com.br/planejamento/service-layer/v2/api/lotes/'


def iniciar_processo_busca(params_exec, *args, **kwargs):
    analisa_dados_sybase(params_exec)


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
        print(dados_sybase)
        print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return dados_sybase


def analisa_dados_sybase(params_exec):
    registro_cloud = interacao_cloud.search_data_cloud_any_where(params_exec, url=url_receita_planejamento)
    registro_dsk = coletar_dados_sybase_controle_receitas_ldo()
    lista_deducao_pendente = []
    count = 0
    dados_coletados = []
    lista_auxiliar_contents = []
    for item in registro_cloud:
        if 'content' in item:
            for i in item['content']:
                dados_coletados.append(i)

    for item in dados_coletados:
        natureza = requests.get(url=str(url_natureza_contabil) + str(item['content']['natureza']['id']),
                                headers={'authorization': f'bearer {params_exec["token"]}'})
        if 'content' in natureza.json():
            lista_auxiliar_contents.append(
                {
                    'receita_planejamento': item['content'],
                    'natureza_contabil': natureza.json()['content'],
                    'id_gerado': dados_coletados[count]['idGerado'],
                    'posicao': count
                }
            )
        count += 1
    for item in registro_dsk:
        count = 0
        it_found = True
        while it_found:
            for sub_item in lista_auxiliar_contents:
                if str(item['i_chave_dsk1'][0:13] + '0') == str(sub_item['natureza_contabil']['numero']):
                    params_alter = {
                        "mascara": str(item['i_chave_dsk1'][0:13]),
                        "ano": int(item['i_chave_dsk5'])
                    }
                    deducoes_desk = coletar_dados_sybase_deducoes_receitas_ldo(params_alter)
                    valor = calcula_valor_deducao(deducoes_desk)
                    lista_recursos_deducao = []
                    count_rec_ded_auxiliar = 0
                    for recurso_deducao in deducoes_desk:
                        count_rec_ded = 0
                        id_recurso = sub_item['receita_planejamento']['recursos'][int(count_rec_ded)]['recurso']['id']
                        for rec_plan_rec in sub_item['receita_planejamento']['recursos']:
                            if ((recurso_deducao[13] / valor) * 100) == Decimal(rec_plan_rec['percentual']):
                                id_recurso = \
                                    sub_item['receita_planejamento']['recursos'][int(count_rec_ded)]['recurso']['id']
                            count_rec_ded += 1

                        dict_dados_recursos = {
                            "valor": recurso_deducao[13],
                            "percentual": (recurso_deducao[13] / valor) * 100,
                            "recurso": {
                                "id": id_recurso
                            }
                        }
                        lista_recursos_deducao.append(dict_dados_recursos)
                        count_rec_ded_auxiliar += 1
                    id_deducao = busca_outra_deducao(params_exec)
                    valor_receita = sub_item['receita_planejamento']['metaFinanceira']
                    if sub_item['receita_planejamento']['cenariosMacroeconomicosLdo'] is not None:
                        dict_dados_deducao = {
                            "cenariosMacroeconomicosLdo": sub_item['receita_planejamento']['cenariosMacroeconomicosLdo']
                        }
                    if sub_item['receita_planejamento']['marcadores'] is not None:
                        dict_dados_deducao = {
                            "marcadores": sub_item['receita_planejamento']['marcadores']
                        }
                    dict_dados_deducao = {
                        "idIntegracao": f"PUTCADASTRORECEITAPLAN{str(sub_item['receita_planejamento']['ldo']['id'])}",
                        "idGerado": sub_item['id_gerado'],
                        "content": {
                            "ldo": {
                                "id": sub_item['receita_planejamento']['ldo']['id']
                            },
                            "exercicio": int(item['i_chave_dsk5']),
                            "natureza": {
                                "id": sub_item['receita_planejamento']['natureza']['id']
                            },
                            "entidade": {
                                "id": sub_item['receita_planejamento']['entidade']['id']
                            },
                            "organograma": {
                                "id": sub_item['receita_planejamento']['organograma']['id']
                            },
                            "recursos": sub_item['receita_planejamento']['recursos'],
                            "deducoes": [
                                {
                                    "valor": valor,
                                    "percentual": round(Decimal((valor / Decimal(valor_receita)) * 100), 2),
                                    "deducao": {
                                        "id": id_deducao['id']
                                    },
                                    "recursos": lista_recursos_deducao
                                }
                            ],
                            "metaFinanceira": sub_item['receita_planejamento']['metaFinanceira']
                        }
                    }
                    lista_deducao_pendente.append(dict_dados_deducao)
                    lista_temp = []
                    print("DEDUCAO")
                    # print("lista_deducao_pendente", lista_deducao_pendente)
                    # CRIA RECEITA NOVA PARA PUT
                    print(json.dumps(lista_deducao_pendente))
                    header = {'authorization': f'bearer {params_exec["token"]}', 'content-type': 'application/json'}
                    retorno_req = requests.put(url=url_receita_planejamento, headers=header, data=json.dumps(lista_deducao_pendente))
                    print("Sucesso lote:", retorno_req.text, "Status Code", retorno_req.status_code)
                    # CRIAR METODO METODO DE GET PARA PEGAR O ID
                    # status = True
                    # count_requests = 0
                    # slepp_time = 0
                    # header_get = {'authorization': f'bearer {params_exec["token"]}'}
                    # while status:
                    #     teste_url = url_lote_planejamento+str(retorno_req.json()['idLote'])
                    #     retorno_lote = requests.get(url=url_lote_planejamento+str(retorno_req.json()['idLote']), header=header_get)
                    #     count_requests += 1
                    #     time.sleep(count_requests * 2)
                    #     if retorno_lote.json()['status'] == 'EXECUTADO':
                    #         status = False
                    #     if count_requests == 10:
                    #         count_requests = 0
                    # receita_gerada = requests.get(url=url_receita_planejamento+'/'+str(retorno_lote.json()['retorno']['idGerado']['id']))
                    # print(receita_gerada.json())

                    # it_found = False
                if len(sub_item) == count:
                    it_found = False
                count += 1


def coletar_dados_sybase_deducoes_receitas_ldo(params_alter):
    print('- Iniciando a consulta dos dados no banco Sybase.')
    contador = 0
    try:
        query = model.get_consulta(params_alter, 'contabil_deducao_receita.sql')
        dados_sybase = []

        with conn_desktop(dbname=params_exec['db_name']) as connSybase:
            cursor_sybase = connSybase.cursor()
            dados = cursor_sybase.execute(query).fetchall()
            # print(cursor_sybase)
            for item in dados:
                dict_dados = item
                contador += 1
                dados_sybase.append(dict_dados)

        print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return dados_sybase


def coletar_dados_sybase_controle_receitas_ldo():
    print('- Iniciando a consulta dos dados no banco Sybase.')
    contador = 0
    params_alter = {
        "teste": "teste"
    }
    try:
        query = model.get_consulta(params_alter, 'coletar_dados_sybase_controle_receitas_ldo.sql')
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
                    # "id_gerado": item[4],
                    "i_chave_dsk1": item[5],
                    "i_chave_dsk2": item[6],
                    "i_chave_dsk3": item[7],
                    "i_chave_dsk4": item[8],
                    "i_chave_dsk5": item[9]
                }
                contador += 1
                dados_sybase.append(dict_dados)

        print(f'- Consulta finalizada. {contador} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return dados_sybase


def calcula_valor_deducao(deducao):
    valor_total = 0

    for item in deducao:
        valor_total += item[13]
    return valor_total


def busca_outra_deducao(params_exec):
    campos = 'id, descricao, tipo'
    criterio = "tipo = 'OUTRAS_DEDUCOES' and descricao = 'Outras Deduções'"
    dados_cloud = interacao_cloud.busca_api_fonte_dados_contabil_unica_barra(params_exec, campos=campos,
                                                                             criterio=criterio,
                                                                             url=url_deducao_contabil)
    # print(dados_cloud)
    return dados_cloud[0]
