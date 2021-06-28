import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import requests
import os
from datetime import datetime

tipo_registro = 'responsaveis'
url = 'https://obras.betha.cloud/obras/api/responsaveistecnicos'

def procura_pessoa(nome, cpf, params_exec):
    pessoa = None
    headers = {
        'authorization': f"Bearer {params_exec['token']}",
        'app-context': params_exec['appcontext'],
        'user-access': params_exec['useraccess'],
    }

    r = requests.get(url='https://obras.betha.cloud/obras/api/pessoas', headers=headers, params={
        'filter': f'((nome like "{nome}" or cpfcnpj like "{cpf}") and (tipo = "F"))'
    })
    if r.ok:
        retorno = json.loads(r.content.decode('utf8'))

        if len(retorno) > 0:
            pessoa = retorno[0]

    return pessoa


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # E - Realiza a consulta dos dados que serão enviados
    dados_assunto = coletar_dados(params_exec)

    # L - Realiza o envio dos dados validados
    iniciar_envio(params_exec, dados_assunto)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    query = None
    try:
        tempo_inicio = datetime.now()
        if params_exec['ESTADO'] == 'SC':
            query = db.get_consulta(params_exec, f'{tipo_registro}_sc.sql')
        if params_exec['ESTADO'] == 'PR':
            query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        # conn = db.conectar(params_exec)
        df = db.consulta_sql(query, params_exec, index_col='id')
        tempo_total = (datetime.now() - tempo_inicio)
        # print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s). '
        #       f'(Tempo consulta: {tempo_total.total_seconds()} segundos.)')

    except Exception as error:
        print(f'Erro ao executar função "enviar_assunto". {error}')

    finally:
        return df


def iniciar_envio(params_exec, dados_assunto):
    dict_dados = dados_assunto
    for i in dict_dados:
        pessoa_cloud = procura_pessoa(i['nome'], i['cpf'], params_exec)

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

            if not db.checa_existe_registro(params_exec, f"{tipo_registro}_obras", params_exec.get('entidade'),str(i['cpf'])):
                if db.checa_existe_registro(params_exec, f"{tipo_registro}_obras", params_exec['entidade'], str(i['cpf'])):
                    conn = db.conectar(params_exec)
                    retorna = db.consulta_sql(f"SELECT 1 as id, * FROM bethadba.controle_migracao_registro where sistema = 308 and tipo_registro='responsaveis' AND i_chave_dsk2 = '{str(i['cpf'])}'")
                    retornaid = 'null'
                    for item in retorna:
                        retornaid = item[5]
                    registro = [
                        str(308),
                        f"{tipo_registro}_obras",
                        'Cadastro de Responsaveis',
                        str(retornaid),
                        str(params_exec.get('entidade')),
                        str(i['cpf'])
                    ]
                    db.regista_controle_migracao(conn, registro)
                    print('- Registro gerado na tabela de migração não foi migrado!')
                else:
                    id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec, False)
                    print(mensagem_erro)
                    if str(mensagem_erro).__contains__('com o mesmo cpf e tipo de responsabilidade cadastrada'):
                        # print('sim')
                        # print(f"SELECT 1 as id, * FROM bethadba.controle_migracao_registro where sistema = 308 and tipo_registro='responsaveis' AND i_chave_dsk2 = '{str(i['cpf'])}'")
                        retorna = db.consulta_sql(f"SELECT 1 as id, * FROM bethadba.controle_migracao_registro where sistema = 308 and tipo_registro='responsaveis' AND i_chave_dsk2 = '{str(i['cpf'])}'",index_col='id').to_dict('records')
                        retornaid = None
                        for item in retorna:
                            retornaid = item['id_gerado']
                        if retornaid:
                            print('Id gerado: ', retornaid)
                            registro = [
                                str(308),
                                f"{tipo_registro}_obras",
                                'Cadastro de Responsaveis',
                                str(retornaid),
                                str(i['i_entidades']),
                                str(i['cpf'])
                            ]
                            # print(registro)
                            conn = db.conectar(params_exec)
                            db.regista_controle_migracao(conn, registro)
                    if id_registro is not None:
                        print('Id gerado: ', id_registro)
                        registro = [
                            str(308),
                            tipo_registro,
                            'Cadastro de Responsaveis',
                            str(id_registro),
                            str(i['i_entidades']),
                            str(i['cpf'])
                        ]
                        conn = db.conectar(params_exec)
                        db.regista_controle_migracao(conn, registro)
                    else:
                        print(f'Erro: {mensagem_erro}')