import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'declaracoesdf'
sistema = 999
limite_lote = 50
url = "https://livro-eletronico.cloud.betha.com.br/service-layer-livro/api/declaracoesdf"


def iniciar_processo_envio(params_exec, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql')
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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["i_declaracoes"], item["key1"])
        dict_dados = {
            "idIntegracao": hash_chaves,
            "declaracoesDf": {
                "iDeclaracoes": item["i_declaracoes"],
                "iDocumentos": item["i_documentos"],
                "iContribuintes": item["i_contribuintes"],
                "iDeclarados": item["i_declarados"],
                "dfInicial": item["df_inicial"],
                "dtEmissao": item["dt_emissao"],
                "tipo": item["tipo"],
                "status": item["status"],
                "vlDocumento": item["vl_documento"],
                "vlBaseCalculo": item["vl_base_calculo"],
                "vlServico": item["vl_servico"],
                "vlImposto": item["vl_imposto"],
                "optanteSn": item["optante_sn"],
                "origem": item["origem"],
                "vlDeducao": item["vl_deducao"],
                "situacaoTributaria": item["situacao_tributaria"],
                "naturezaOperacao": item["natureza_operacao"],
                "descontadoPrefeitura": item["descontado_prefeitura"]
                # "vlDescIncondicional": item["vl_desc_incondicional"],
                # "aliquota": item["aliquota"],
                # "vlIss": item["vl_iss"],
                # "vlTaxas": item["vl_axas"],
                # "vlDescCondicional": item["vl_desc_condicional"]
            }
        }

        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Planos de Contas',
            'id_gerado': None,
            'i_chave_dsk1': item["i_declaracoes"],
            'i_chave_dsk2': item["key1"]
        })
    # print(lista_controle_migracao)
    # print(lista_dados_enviar)
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')
