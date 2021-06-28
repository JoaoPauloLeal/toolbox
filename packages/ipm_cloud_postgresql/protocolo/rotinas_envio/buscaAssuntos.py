import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime


tipo_registro = 'pe'
sistema = 306
limite_lote = 500
url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/assunto"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de Assuntos.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0

    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    # print(req_res)
    dados_update = []

    for item in req_res:
        if 'id' in item:
            idGerado = item['id']
            chave_dsk1 = item['descricao'].upper()
            classif = item['classificacao']
            chave_dsk2 = classif['id']
            # chave_dsk2 = item['iAssuntos']
            # print(f'idCloud {idGerado} - {chave_dsk1} - {chave_dsk2}')
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)
        # print(hash_chaves)

        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de Assuntos',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2
        })
        contador += 1
    # print(lista_controle_migracao)
    # model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    print(contador)
    print('- Busca de dados finalizado.')

#         classif = item['classificacao']
#         print(classif)
#     keys_update = [key for key in item.keys() if key == "id"]
#
#     hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1)
#     item.update({"idIntegracao": hash_chaves, "conteudo": {	'iAssuntos': item['iAssuntos'],
#                                                             'descricao': item['descricao'],
#                                                             'mascara': item['mascara'],
#                                                             'tramitavel': item['tramitavel'],
#                                                             'disponivel': item['disponivel'],
#                                                             'volume': item['volume'],
#                                                             'aberturaExterna': item['aberturaExterna'],
#                                                             'sigiloso': item['sigiloso'],
#                                                             'tramitarProcessosTaxaPendente': item['tramitarProcessosTaxaPendente'],
#                                                             'classificacao': {'id': classif['id'], 'descricao': classif['descricao']},
#                                                             'vencimentoManual': item['vencimentoManual'],
#                                                             'taxasConsultaExterna': item['taxasConsultaExterna']}})
#     [item.pop(key) for key in keys_update]
#
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "iAssuntos"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "descricao"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "mascara"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "tramitavel"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "disponivel"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "volume"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "sumula"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "aberturaExterna"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "sigiloso"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "responsavelEdita"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "adicionaTaxasAbertura"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "tramitarProcessosTaxaPendente"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "informacoesAoCidadao"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "emitirGuiaAutomaticamente"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "classificacao"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "convenio"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "tipoVencimento"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "vencimentoManual"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "taxasConsultaExterna"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#     keys_update2 = [key2 for key2 in item.keys() if key2 == "qtdDias"]
#     item.update('')
#     [item.pop(key2) for key2 in keys_update2]
#
#
#     contador += 1
#
#     dados_update.append(item)
#
# print(dados_update)
