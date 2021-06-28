import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'organograma'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/organograma"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
        print('- Iniciando busca dos dados de Organograma.')
        lista_controle_migracao = []
        hoje = datetime.now().strftime("%Y-%m-%d")
        contador = 0

        req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                    url=url,
                                                    tipo_registro=tipo_registro,
                                                    tamanho_lote=limite_lote)
        print(req_res)

        for item in req_res:
            if 'id' in item:
                idGerado = item['id']
                chave_dsk1 = item['nome'].upper()
                chave_dsk2 = item['mascaraFormatada']
                # print(f'idCloud {idGerado} - {chave_dsk1}')
                hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)

                lista_controle_migracao.append({
                    'sistema': sistema,
                    'tipo_registro': tipo_registro,
                    'hash_chave_dsk': hash_chaves,
                    'descricao_tipo_registro': 'Busca de Organogramas',
                    'id_gerado': idGerado,
                    'i_chave_dsk1': chave_dsk1,
                    'i_chave_dsk2': chave_dsk2
                })
            contador += 1
        # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
        model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
        # print(contador)
        print('- Busca de dados finalizado.')
