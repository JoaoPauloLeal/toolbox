import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'processos-extra-atualizado'
sistema = 304
limite_lote = 1000
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processos"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    print("INICIANDO BUSCA NO CLOUD")
    req_res = interacao_cloud.busca_dados_cloud_alterado(params_exec,
                                                          url=url,
                                                          tipo_registro=tipo_registro,
                                                          tamanho_lote=limite_lote)

    # print(req_res)
    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in req_res:
        idGerado = item['id']
        chave_dsk1 = item['numeroProcesso']
        chave_dsk2 = item['situacao']

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk1).replace("-", "/"))
        # print(idGerado)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de processos no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2,
        })
        contador += 1
    # print(lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")
    print('- Busca de dados finalizado.')
