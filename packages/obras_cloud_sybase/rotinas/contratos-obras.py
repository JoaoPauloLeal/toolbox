import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import requests
from datetime import datetime

tipo_registro = 'contratos-obras'


def procura_contrato(numero_contrato, params_exec):
    headers = {
        'authorization': f"Bearer {params_exec['token']}",
        'app-context': params_exec['appcontext'],
        'user-access': params_exec['useraccess'],
    }

    separator = '/'
    num = numero_contrato.split(separator)[0]
    ano = numero_contrato.split(separator)[1]
    r = requests.get(url='https://obras.betha.cloud/obras/api/contratos/contratacoes', headers=headers, params={
        'filter': f'(numeroTermo like "{num}")',
        'limit': 25,
        'offset': 0,
        'term': num
    })

    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))
        dado = None
        for indice, x in enumerate(retorno['content']):
            # print(x['numeroTermo'])
            # print(x['ano'])
            if str(x['numeroTermo']) == str(num) and str(x['ano']) == str(ano):
                dado = retorno['content'][indice]
        # if len(retorno['content']) > 0:
        #     print(retorno['content']['numeroTermo'])
        #     print(retorno['content']['ano'])
        return dado
    return None


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        tempo_inicio = datetime.now()
        if params_exec['ESTADO'] == 'SC':
            query = db.get_consulta(params_exec, f'{tipo_registro}-sc.sql')
        else:
            query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        conn = db.conectar(params_exec)
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        contrato = procura_contrato(i['numero_contrato'], params_exec)
        if contrato:
            dict_enviar = {
                "dataInclusaoContrato": i['data_inclusao'],
                "contratoMap": contrato,
                "contrato": {
                    "codContrato": contrato['id'],
                    "nroContrato": str(contrato['numeroTermo']),
                    "anoContrato": contrato['ano'],
                    "nroAnoContrato": str(contrato['numeroTermo']) + "/" + str(contrato['ano']),
                    "dtAssinatura": contrato['dataAssinatura'],
                    "tipoObjeto": contrato['tipoObjeto']['descricao'],
                    "objeto": contrato['objetoContratacao'],
                    "fornecedor": contrato['fornecedor']['pessoa']['nome'],
                    "vlContrato": contrato['valorSolFornec']
                }
            }

            print(dict_enviar)
            if not db.checa_existe_registro(params_exec, tipo_registro, str(i['i_entidades']), str(i['i_obras'])):
                print(f'https://obras.betha.cloud/obras/api/obras/{i["i_obras"]}/contratosobras')
                id_registro, mensagem_erro = cloud.envia_registro(f'https://obras.betha.cloud/obras/api/obras/{i["i_obras"]}/contratosobras', json.dumps(dict_enviar), params_exec, False)
                if id_registro is not None:
                    print('Id gerado: ', id_registro)
                    registro = [
                        str('308'),
                        tipo_registro,
                        'Cadastro de Contratos de Obras',
                        str(id_registro),
                        str(i['i_entidades']),
                        str(i['i_obras'])
                    ]
                    conn = db.conectar(params_exec)
                    db.regista_controle_migracao(conn, registro)
                else:
                    print(f"Erro ao enviar JSON - {dict_enviar}")
        else:
            print(f'Erro ao registrar contrato, contrato não encontrado na base Cloud.\nID da obra: {i["i_obras"]}, Contrato: {i["numero_contrato"]}')