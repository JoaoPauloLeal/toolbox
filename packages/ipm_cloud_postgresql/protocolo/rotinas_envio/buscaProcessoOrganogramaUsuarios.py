import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'processo-organogramas-usuarios'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processo-organogramas-usuarios"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Localização de processo.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    print(req_res)
