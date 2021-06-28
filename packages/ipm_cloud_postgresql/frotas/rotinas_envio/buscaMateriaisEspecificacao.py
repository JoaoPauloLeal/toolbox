import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'materiais-especificacoes'
sistema = 306
limite_lote = 100
url = "https://frotas.betha.cloud/frotas-services/api/materiais/materialId/especificacoes"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    dados_assunto = coletar_dados(params_exec, ano)
    dados_buscar = pre_validar(params_exec, dados_assunto)
    print('- Iniciando busca dos dados de dados.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    for id_gerados in dados_buscar:
        url_replace = str(url).replace('materialId', id_gerados['id_gerado'])
        req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                    url=url_replace,
                                                    tipo_registro=tipo_registro,
                                                    tamanho_lote=limite_lote)

        print(req_res)
        for item in req_res:
            idGerado = item['id']
            chave_dsk1 = item['descricao'].upper()
            chave_dsk2 = id_gerados['id_gerado']

            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)
            # print(idGerado)
            lista_controle_migracao.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Busca de Materiais Especificões',
                'id_gerado': idGerado,
                'i_chave_dsk1': chave_dsk1,
                'i_chave_dsk2': chave_dsk2
            })
            contador += 1
        # print(lista_controle_migracao)
        model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    print('- Busca de dados finalizado.')


def coletar_dados(params_exec, ano):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, 'materiais-servicos.sql', ano)
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
