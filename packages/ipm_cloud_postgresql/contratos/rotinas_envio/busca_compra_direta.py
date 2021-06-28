import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
import requests
from datetime import datetime

tipo_registro = 'compra-direta'
sistema = 305
limite_lote = 1000
url = "https://contratos.betha.cloud/contratacao-services/api/exercicios/2020/contratacoes"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    contador = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)
    for item in req_res:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item['clicodigo'], item['ano_cd'], item['nro_cd'])
        lista_conteudo_retorno.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Compras Diretas',
            'id_gerado': item['id'],
            # 'json': json.dumps(dict_dados),
            'i_chave_dsk1': 2016,
            'i_chave_dsk2': item['ano_cd'],
            'i_chave_dsk3': item['nro_cd']
        })
        contador += 1

    print(contador)
    print('- Busca de dados finalizado.')
