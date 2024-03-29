import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'despesa'
url = 'https://compras.betha.cloud/compras-services/api/despesas'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # DE-PARA dados cadastrados cloud
    dados_verificar_cloud = coletar_dados(params_exec)
    busca_dados_cloud(params_exec, dados_verificar_cloud)

    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')


def busca_dados_cloud(params_exec, dados_base):
    print('- Iniciando busca de dados no cloud.')
    url_fonte_dados = 'https://compras.betha.cloud/compras/dados/api/despesas'
    campos = 'entidade(id), parametroExercicio(exercicio), recursoContabil(numero), organograma, funcao, subFuncao, ' \
             'programa, acao, natureza'
    contador = 0
    registros_inseridos = 0
    lista_dados = dados_base.to_dict('records')
    total_dados = len(lista_dados)

    for item in lista_dados:
        contador += 1
        print(f'\r- Verificando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        criterio = f'entidade.id = {item["id_entidade"]} and parametroExercicio.exercicio = {item["loaano"]} and ' \
                   f'organograma = \'{item["organograma"]}\' and funcao = {item["funcao"]} and subFuncao = ' \
                   f'{item["subfuncao"]} and programa = {item["programa"]} and acao = \'{item["acao"]}\' and ' \
                   f'natureza = \'{item["natureza"]}\' and recursoContabil.numero = \'{item["recurso_bth"]}\''
        # print('criterio', criterio)
        registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url_fonte_dados,
                                                               campos=campos, criterio=criterio)

        if registro_cloud is not None and len(registro_cloud) > 0:
            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro,
                                                  item["id_entidade"], item['loaano'], item['dotcodigo'])
            registro_encontrado = {
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Cadastro de Despesas',
                'id_gerado': registro_cloud[0]['id'],
                'i_chave_dsk1': item['id_entidade'],
                'i_chave_dsk2': item['loaano'],
                'i_chave_dsk3': item['dotcodigo']
            }
            model.insere_tabela_controle_migracao_registro(params_exec, lista_req=[registro_encontrado])
            registros_inseridos += 1
    print(f'- Foram inseridos {registros_inseridos} registros na tabela de controle.')


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        query = model.get_consulta(params_exec, f'{tipo_registro}.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

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

            # INSERIR AS REGRAS DE PRÉ VALIDAÇÃO AQUI

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
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    token = params_exec['token']
    total_dados = len(dados)
    contador = 0
    total_erros = 0

    for item in dados:
        lista_dados_enviar = []
        lista_controle_migracao = []
        contador += 1
        print(f'\r- Enviando registros: {contador}/{total_dados}', '\n' if contador == total_dados else '', end='')
        hash_chaves = model.gerar_hash_chaves(sistema,
                                              tipo_registro,
                                              item['id_entidade'],
                                              item['loaano'],
                                              item['dotcodigo'])
        dict_dados = {
            'idIntegracao': hash_chaves,
            'organograma': item['organograma'],
            'funcao': item['funcao'],
            'subFuncao': item['subfuncao'],
            'programa': item['programa'],
            'acao': item['acao'],
            'descricao': item['descricao_despesa'],
            'naturezaDespesa': item['natureza'],
            'mascara': item['mascara'],
            'parametrosExerc': {
                'id': item['id_exercicio']
            },
            'recursoContabil': {
                'numeroRecurso': item['recurso_bth'],
                'descricao': item['desc_recurso']
            },
            'numeroDespesa': item['dotcodigo']
        }

        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Despesas',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['id_entidade'],
            'i_chave_dsk2': item['loaano'],
            'i_chave_dsk3': item['dotcodigo']
        })

        if True:
            model.insere_tabela_controle_migracao_registro(params_exec, lista_req=lista_controle_migracao)
            req_res = interacao_cloud\
                .preparar_requisicao_sem_lote(
                    lista_dados=lista_dados_enviar,
                    token=token,
                    url=url,
                    tipo_registro=tipo_registro)
            model.atualiza_tabelas_controle_envio_sem_lote(params_exec, req_res, tipo_registro=tipo_registro)
            if req_res[0]['mensagem'] is not None:
                total_erros += 1
    if total_erros > 0:
        print(f'- Envio finalizado. Foram encontrados um total de {total_erros} inconsistência(s) de envio.')
    else:
        print('- Envio de dados finalizado sem inconsistências.')


