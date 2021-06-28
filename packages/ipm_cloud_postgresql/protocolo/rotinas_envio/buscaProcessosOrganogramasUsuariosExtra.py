import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'processo-organogramas-usuarios-extra-atualizado'
sistema = 304
limite_lote = 100
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/processo-organogramas-usuarios"


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
        if item['processo'] is not None:
            processo = item['processo']
            chave_dsk1 = processo['id']
            chave_dsk2 = processo['numeroProcesso']
        if item['organograma'] is not None:
            organograma = item['organograma']
            chave_dsk3 = organograma['id']
            chave_dsk4 = organograma['nome'].upper()
        chave_dsk5 = item['tipoTramitacao']
        chave_dsk6 = item['idUsuario']

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5, chave_dsk6)
        # print(idGerado)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de processos organogramas no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2,
            'i_chave_dsk3': chave_dsk3,
            'i_chave_dsk4': chave_dsk4,
            'i_chave_dsk5': chave_dsk5,
            'i_chave_dsk6': chave_dsk6
        })
        contador += 1
    # print(lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_controle_migracao)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")
    print('- Busca de dados finalizado.')
