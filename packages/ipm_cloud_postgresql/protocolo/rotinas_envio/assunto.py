import packages.ipm_cloud_postgresql.model as model
import packages.ipm_cloud_postgresql.protocolo.rotinas_envio.buscaAssuntos as assuntosBusca
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'assunto'
sistema = 304
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/assunto"

def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    assuntosBusca.iniciar_processo_busca(params_exec)