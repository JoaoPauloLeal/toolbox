import packages.ipm_cloud_postgresql.model as model
import packages.ipm_cloud_postgresql.protocolo.rotinas_envio.buscaAssuntoDocumento as assuntoDocumentoBusca
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'assuntoDocumento'
sistema = 304
limite_lote = 100
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/assuntoDocumento"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    assuntoDocumentoBusca.iniciar_processo_busca(params_exec)