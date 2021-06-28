import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'assuntoOrganograma'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/assuntoOrganograma"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Assunto Organograma.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    print(req_res)
    dados_update = []

    for item in req_res:
        if 'id' in item:
            idGerado = item['id']
            assunto = item['assunto']
            organograma = item['organograma']

            idAssunto = assunto['id']
            descricaoAssunto = assunto['descricao']
            iAssunto = assunto['iAssuntos']

            idOrganograma = organograma['id']
            descricaoOrganograma = organograma['nome']
            mascara = organograma['mascaraCompleta']

            hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, idGerado)

            print(f'assunto {idAssunto} - {descricaoAssunto} :: organograma {idOrganograma} - {descricaoOrganograma}')

            lista_controle_migracao.append({
                'sistema': sistema,
                'tipo_registro': tipo_registro,
                'hash_chave_dsk': hash_chaves,
                'descricao_tipo_registro': 'Busca de Assunto Organograma',
                'id_gerado': None,
                'i_chave_dsk1': idAssunto,
                'i_chave_dsk2': descricaoAssunto,
                'i_chave_dsk3': iAssunto,
                'i_chave_dsk4': idOrganograma,
                'i_chave_dsk5': descricaoOrganograma,
                'i_chave_dsk6': mascara
            })
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)


