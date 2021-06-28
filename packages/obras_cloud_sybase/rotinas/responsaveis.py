import bth.db_connector as db
import bth.cloud_connector as cloud
import json

import settings
import requests
import os
from datetime import datetime

tipo_registro = 'responsaveis'
url = 'https://obras.betha.cloud/obras/api/responsaveistecnicos'

def procura_pessoa(nome, cpf):
    headers = {
        'authorization': f'Bearer {settings.USER_TOKEN}',
        'app-context': settings.APP_CONTEXT,
        'user-access': settings.USER_ACCESS,
    }

    r = requests.get(url='https://obras.betha.cloud/obras/api/pessoas', headers=headers, params={
        'filter': f'((nome like "{nome}" or cpfcnpj like "{cpf}") and (tipo = "F"))'
    })

    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))
        if len(retorno) > 0:
            return retorno[0]

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
        query = None
        if settings.ESTADO == 'SC':
            query = db.get_consulta(params_exec, f'{tipo_registro}_sc.sql')
        if settings.ESTADO == 'PR':
            query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        conn = db.conectar()
        df = db.consulta_sql(conn, query, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
              f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto.to_dict('records')
    for i in dict_dados:
        pessoa_cloud = procura_pessoa(i['nome'], i['cpf'])
        if pessoa_cloud:
            dict_enviar = {
                "ativo": True,
                "pessoa": {
                    "fisica": {
                        "dataNascimento": None,
                        "rg": i['rg'],
                        "orgaoEmissorRg": i['orgao_emis_rg'],
                        "dataEmissaoRg": i['dt_emis_rg'],
                        "ufEmissaoRg": i['uf_emis_rg'],
                        "pessoa": None,
                        "cnh": None,
                        "categoriaCnh": None,
                        "dataEmissaoCnh": None,
                        "dataPrimeiraCnh": None,
                        "dataVencimentoCnh": None,
                        "inscricaoMunicipal": None,
                        "municipioInscricao": None
                    }
                },
                "tecnico": {
                    "nome": i['nome'],
                    "id": pessoa_cloud['id']
                },
                "tipoResponsavelTecnico": {
                    "id": i['tipo_responsavel'],
                    "descricao": i['descricao'],
                    "campoAdicional": None
                },
                "registroCrea": i['num_registro']
            }

            dict_enviar['pessoa'] |= pessoa_cloud

            json_envio = json.dumps(dict_enviar)

            if not db.checa_existe_registro(tipo_registro,params_exec.get('entidade'),str(i['cpf'])):
                id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, False)
                if id_registro is not None:
                    print('Id gerado: ', id_registro)
                    registro = [
                        str(settings.SISTEMA),
                        tipo_registro,
                        'Cadastro de Responsaveis',
                        str(id_registro),
                        str(i['i_entidades']),
                        str(i['cpf'])
                    ]
                    conn = db.conectar()
                    db.regista_controle_migracao(conn, registro)
                else:
                    save_path = os.path.realpath('logs')
                    path = os.path.join(save_path, f"{tipo_registro}.txt")
                    f = open(path, "a")
                    f.write(f"JSON - {json_envio}\nErro - {mensagem_erro}\n\n")
                    f.close()