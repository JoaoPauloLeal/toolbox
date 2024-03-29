import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
from datetime import datetime

sistema = 300
tipo_registro = 'concurso'
url = 'https://pessoal.cloud.betha.com.br/service-layer/v1/api/concurso'
limite_lote = 1000


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
        dh_inicio = datetime.now()
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- {len(df.index)} registro(s) encontrado(s).',
              f'\n- Consulta finalizada. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dh_inicio = datetime.now()
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)

        print(f'- Registros validados com sucesso: {len(dados_validados)} '
              f'| Registros com advertência: {len(registro_erros)}'
              f'\n- Pré-validação finalizada. ({(datetime.now() - dh_inicio).total_seconds()}) segundos')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando processo de transformação.')
    dh_inicio = datetime.now()
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)
    contador = 0
    for item in dados:
        contador += 1
        print(f'\r- Gerando JSON: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['id_entidade'], item['numeroedital'], item['tiporecrutamento'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'conteudo': {
                'tipoRecrutamento': item['tiporecrutamento'],
                'numeroEdital': item['numeroedital'],
                'descricao': item['descricao']
            }
        }
        if 'ato' in item and item['ato'] is not None:
            dict_dados['conteudo'].update({
                'ato': {
                    'id': item['ato']
                }
            })
        if 'percentualpcd' in item and item['percentualpcd'] is not None:
            dict_dados['conteudo'].update({'percentualPcd': item['percentualpcd']})
        if 'datainicialinscricao' in item and item['datainicialinscricao'] is not None:
            dict_dados['conteudo'].update({'dataInicialInscricao': item['datainicialinscricao'].strftime("%Y-%m-%d")})
        if 'datafinalinscricao' in item and item['datafinalinscricao'] is not None:
            dict_dados['conteudo'].update({'dataFinalInscricao': item['datafinalinscricao'].strftime("%Y-%m-%d")})
        if 'dataprorrogacao' in item and item['dataprorrogacao'] is not None:
            dict_dados['conteudo'].update({'dataProrrogacao': item['dataprorrogacao'].strftime("%Y-%m-%d")})
        if 'datainicialinscricaopcd' in item and item['datainicialinscricaopcd'] is not None:
            dict_dados['conteudo'].update({'dataInicialInscricaoPcd': item['datainicialinscricaopcd'].strftime("%Y-%m-%d")})
        if 'datafinalinscricaopcd' in item and item['datafinalinscricaopcd'] is not None:
            dict_dados['conteudo'].update({'dataFinalInscricaoPcd': item['datafinalinscricaopcd'].strftime("%Y-%m-%d")})
        if 'datahomologacao' in item and item['datahomologacao'] is not None:
            dict_dados['conteudo'].update({'dataHomologacao': item['datahomologacao'].strftime("%Y-%m-%d")})
        if 'datavalidade' in item and item['datavalidade'] is not None:
            dict_dados['conteudo'].update({'dataValidade': item['datavalidade'].strftime("%Y-%m-%d")})
        if 'dataprorrogacaovalidade' in item and item['dataprorrogacaovalidade'] is not None:
            dict_dados['conteudo'].update({'dataProrrogacaoValidade': item['dataprorrogacaovalidade'].strftime("%Y-%m-%d")})
        if 'dataencerramento' in item and item['dataencerramento'] is not None:
            dict_dados['conteudo'].update({'dataEncerramento': item['dataencerramento'].strftime("%Y-%m-%d")})

        # print(f'\nDados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Concurso',
            'id_gerado': None,
            'i_chave_dsk1': item['id_entidade'],
            'i_chave_dsk2': item['numeroedital'],
            'i_chave_dsk3': item['tiporecrutamento']
        })
    print(f'- Processo de transformação finalizado. ({(datetime.now() - dh_inicio).total_seconds()} segundos)')
    if True:
        model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
        req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                      token=token,
                                                      url=url,
                                                      tipo_registro=tipo_registro,
                                                      tamanho_lote=limite_lote)
        model.insere_tabela_controle_lote(req_res)