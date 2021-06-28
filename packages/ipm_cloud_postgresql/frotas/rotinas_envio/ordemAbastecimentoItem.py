import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import math
import packages.ipm_cloud_postgresql.frotas.rotinas_envio.veiculoEquipamento as veiculoEquipamento
import logging
from datetime import datetime

tipo_registro = 'ordem-abastecimento-item'
sistema = 306
limite_lote = 50
url = "https://frotas.betha.cloud/frotas-services/api/conversoes/lotes/ordem-abastecimento-item"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec, ano)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec, ano):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql', ano)
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    token = params_exec['token']
    contador = 0
    # print(dados)
    #
    # print(dados)
    for item in dados:
        # print("EMPACOU LOOP 1")
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(item["ordemabastecimento"]), str(item["dataordem"]), str(item["exercicio"]), str(item["numeroitem"]))
        # print("EMPACOU LOOP 1")
        # print(item)
        dict_dados = {
            "hash": hash_chaves,
            "conteudo": {
                "ordemAbastecimento": {
                    "id": int(item["ordemabastecimento"])
                },
                "material": {
                    "id": int(item["material"])
                },
                "materialEspecificacao": {
                    "id": int(item["materialespecificacao"])
                },
                "numeroItem": item["numeroitem"],
                "completarTanque": int(item["completartanque"]),
                "tipoDespesa": {
                    "id": int(item["tipodespesa"])
                }
            },
        }
        # dict_dados2 = dict_dados["conteudo"].replace("None", "null")
        # print(dict_dados)
        # print("EMPACOU LOOP 2")
        contador += 1
        # print(contador)
        # print(f'Dados gerados ({contador}): ', dict_dados)
        # print(dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de ordem de abestecimento e servico',
            'id_gerado': None,
            'i_chave_dsk1': str(item["ordemabastecimento"]),
            'i_chave_dsk3': str(item["dataordem"]),
            'i_chave_dsk4': str(item["exercicio"]),
            'i_chave_dsk5': str(item["numeroitem"])
        })
    print(lista_dados_enviar)
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    print("AQUI")
    print(lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao_frotas(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')


def isNaN(num):
    return num != num