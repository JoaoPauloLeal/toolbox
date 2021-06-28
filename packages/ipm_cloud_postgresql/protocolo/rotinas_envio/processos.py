import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import packages.ipm_cloud_postgresql.protocolo.rotinas_envio.buscaProcessos as processosBusca
import logging
from datetime import datetime


tipo_registro = 'processos'
sistema = 304
limite_lote = 35
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processos"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    # processosBusca.iniciar_processo_busca(params_exec)
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
        df = pgcnn.exec_sql(query, index_col='id')
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
    for item in dados:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["key1"], item["id_codigo"], item["ano"])
        dict_dados = {
            "idIntegracao": hash_chaves,
            "conteudo": {
                "protocolo": {
                    "id": item["id_protocolo_cloud"]
                    },
                "assunto": {
                    "id": item["id_assunto_cloud"]
                    },
                "requerente": {
                    "id": item["id_cloud_pessoa"]
                    },
                "numeroProcesso": item["numero_processo"],
                "tipoIdentificacao": item["tipo_identificacao"],
                "possuiDataPrevista": item["possui_data_prevista"],
                "previstoPara": item["previsto_para"],
                "situacao": item["situacao"],
                "procedencia": item["procedencia"],
                "prioridade": item["prioridade"],
                "tipoProcesso": item["tipo_processo"],
                "observacao": item["observacao"]
            }
        }
        # "requerente": {
        #     "id": None if "id" not in item else item["id_cloud_pessoa"]
        # },

        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Processos',
            'id_gerado': None,
            'i_chave_dsk1': item["key1"],
            'i_chave_dsk2': item["id_codigo"],
            'i_chave_dsk3': item["ano"]
        })
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')
