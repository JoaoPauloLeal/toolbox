import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'tomadoresprestadores'
sistema = 999
limite_lote = 50
url = "https://livro-eletronico.cloud.betha.com.br/service-layer-livro/api/tomadoresprestadores/60145a077bdd0e00f7e40ad2"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Tomadores Prestadores.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    print(req_res)
