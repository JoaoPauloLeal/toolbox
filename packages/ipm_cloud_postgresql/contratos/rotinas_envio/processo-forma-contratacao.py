import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import re
import math
from datetime import datetime

sistema = 305
tipo_registro = 'processo-forma-contratacao'
url = 'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/forma-contratacao'

# Define responsável padrao caso o SQL nao encontre um registro
# id_responsavel_padrao = 5461912 # Teste
id_responsavel_padrao = 5483794 # Oficial


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # T - Realiza a pré-validação dos dados
    dados_enviar = pre_validar(params_exec, dados_assunto)

    # L - Realiza o envio dos dados validados
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')


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


def list_unique(lista):
    list_of_unique = []
    for item in lista:
        if item not in list_of_unique:
            list_of_unique.append(item)
    return list_of_unique


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
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['clicodigo'],
                                              item['ano_minuta'], item['nro_minuta'])
        url_parametrizada = url.replace('{exercicio}', str(item['ano_processo']))\
                               .replace('{processoAdministrativoId}', str(item['id_processo']))
        dict_dados = {
            'idIntegracao': hash_chaves,
            'url': url_parametrizada,
            'processoAdmistrativo': {
                'id': item['id_processo']
            },
            'formaContratacao': {
                'valor': item['forma_contratacao']
            },
            'desclassificaPropostaInvalidaLote': {
                'valor': item['desc_prop_invalida_lote']
            },
            'desclassificaPropostaInvalida': {
                'valor': item['desc_prop_invalida']
            },
            'modalidade': {
                'id': item['id_modalidade']
            },
            'responsavel': {
                'id': id_responsavel_padrao if item['id_responsavel'] == 0 else item['id_responsavel']
            }
        }

        if item['ano_sequencial'] is not None:
            dict_dados.update({'anoSequencial': item['ano_sequencial']})

        if item['nro_sequencial'] is not None:
            dict_dados.update({'numeroSequencial': item['nro_sequencial']})
            # dict_dados.update({'numeroSequencial': int('6' + str(item['nro_sequencial']))})
            # dict_dados.update({'numeroSequencial': item['nro_minuta']})
            # dict_dados.update({'numeroSequencial': 999})

        if item['id_fundamento_legal'] is not None and item['id_fundamento_legal'] != 0:
            dict_dados.update({'fundamentacaoLegal': {'id': item['id_fundamento_legal']}})
            # dict_dados.update({'fundamentacaoLegal': {'id': 81}}) # Inegibilidade

        if item['id_membro_comissao'] is not None and item['id_membro_comissao'] != 0:
            dict_dados.update({'membroComissao': {'id': item['id_membro_comissao']}})
            # dict_dados.update({'membroComissao': {'id': 53788}})  # Pregoeiro
            # dict_dados.update({'membroComissao': {'id': 53417}})  # Presidente
            # dict_dados.update({'membroComissao': {'id': 53501}})  # Leiloeiro

        if item['registro_preco'] is not None:
            dict_dados.update({'registroPreco':  item['registro_preco']})

        if item['dh_inicio_recebimento_envelopes'] is not None:
            dict_dados.update({'dataInicioRecebimentoEnvelope':  item['dh_inicio_recebimento_envelopes']})

        if item['dh_final_recebimento_envelopes'] is not None:
            dict_dados.update({'dataFinalRecebimentoEnvelope':  item['dh_final_recebimento_envelopes']})

        if item['dh_abertura_envelopes'] is not None:
            dict_dados.update({'dataAberturaEnvelope':  item['dh_abertura_envelopes']})

        if item['data_autorizacao_rp'] is not None:
            dict_dados.update({'dataAutorizacaoAdesaoAtaRegPreco':  item['data_autorizacao_rp']})

        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Forma de Contratação do Processo',
            'id_gerado': None,
            'json': json.dumps(dict_dados),
            'i_chave_dsk1': item['clicodigo'],
            'i_chave_dsk2': item['ano_minuta'],
            'i_chave_dsk3': item['nro_minuta']
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
