import bth.db_connector as db
import bth.cloud_connector as cloud
import json
import settings
import os
from datetime import datetime

tipo_registro = 'obras'
url = 'https://obras.betha.cloud/obras/api/obras'


def dms2dd(degrees, minutes, seconds, direction):
    if degrees and minutes and seconds:
        dd = float(degrees) + float(minutes) / 60 + float(seconds) / (60 * 60)
        if direction == 'L' or direction == 'N':
            dd *= -1
        return dd
    else:
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
        query = db.get_consulta(params_exec, f'{tipo_registro}.sql')
        conn = db.conectar(params_exec)
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
        dict_enviar = {
            'arquivos': [],
            'descricao': i['descricao'],
            'dataCadastro': i['datacadastro'],
            'tipoObra': {
                'id': i['tipo_obra']
            },
            'categoria': {
                'id': i['categoria']
            },
            'destinosObra': [],
            'obraProjeto': i['obraprojeto'],
            'situacao': 'NAO_INICIADA',
            'situacaoDesc': "Não iniciada",
            'latitudeChegada': dms2dd(i['grau_latitude'], i['minutos_latitude'], i['segundos_latitude'], i['posicao_latitude']),
            "longitudeChegada": dms2dd(i['grau_longitude'], i['minutos_longitude'], i['segundos_longitude'], i['posicao_longitude']),
            "latitudeSaida": dms2dd(i['grau_latitude_f'], i['minutos_latitude_f'], i['segundos_latitude_f'], i['posicao_latitude_f']),
            "longitudeSaida": dms2dd(i['grau_longitude_f'], i['minutos_longitude_f'], i['segundos_longitude_f'], i['posicao_longitude_f']),
            "objeto": i['objeto'],
            'diasTermino': i['diastermino'],
            'obraProjeto': i['obraprojeto'],
            'tipoExecucao': i['tipoexecucao_id'],
            'quantidade': i['quantidade'] if i['quantidade'] > 0 else None
        }

        json_envio = json.dumps(dict_enviar)
        if not db.checa_existe_registro(params_exec, tipo_registro,str(params_exec.get('entidade')), str(i['i_obras'])):
            print(json_envio)
            id_registro, mensagem_erro = cloud.envia_registro(url, json_envio, params_exec, True)
            if id_registro is not None:
                print('Id gerado: ', id_registro)
                registro = [
                    str(308),
                    tipo_registro,
                    'Cadastro de Obras',
                    str(id_registro),
                    str(i['i_entidades']),
                    str(i['i_obras'])
                ]
                conn = db.conectar(params_exec)
                db.regista_controle_migracao(conn, registro)
            else:
                print(f'Erro: {mensagem_erro}')
