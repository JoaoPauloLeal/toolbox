import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

tipo_registro = 'organogramas_frotas'
sistema = 667
limite_lote = 1000
url = "https://frotas.betha.cloud/frotas-services/api/organogramas"


def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando replicação de organogramas do frotas.')
    """
    Função para replicação de organogramas do frotas cloud.
    :id_conf_org_paste: Id configuração de organograma ao qual deve receber replicação.
    :id_conf_org_reply: Id configuração de organograma ao qual deve ser buscado os organogramas.
    Obs.: Caso necessario evoluir para input dos campos
    """

    id_conf_org_paste = 9074
    id_conf_org_reply = 9095

    list_receive_sl = []
    cont = 0
    req_res = interacao_cloud.busca_dados_cloud(params_exec,
                                                url=url,
                                                tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)

    for item in req_res:
        id_configuracao_organograma = item["configuracaoOrganograma"]
        list_receive_sl.append({
            'idConfiguracaoOrganograma': id_configuracao_organograma["id"],
            'descricao': item["descricao"],
            'numeroOrganograma': item["numeroOrganograma"],
            'nivel': item["nivel"]
        })
        cont += 1

    cont_two = 0
    cont_thre = 0

    list_search_font_aux = []
    for item in list_receive_sl:
        if int(item["idConfiguracaoOrganograma"]) == id_conf_org_paste:
            list_search_font_aux.append(str(item["numeroOrganograma"]))

    list_data_send = []
    for item in list_receive_sl:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(item["idConfiguracaoOrganograma"]),
                                              str(item["numeroOrganograma"]), str(item["descricao"]))
        if int(item["idConfiguracaoOrganograma"]) == id_conf_org_reply:
            if str(item["numeroOrganograma"]) not in list_search_font_aux:
                dict_dados = {
                    "hash": hash_chaves,
                    # "conteudo": {
                    "configuracaoOrganograma": {
                        "id": id_conf_org_paste
                    },
                    "numeroOrganograma": item["numeroOrganograma"],
                    "descricao": item["descricao"],
                    "nivel": item["nivel"]
                    # }
                }
                cont_two += 1

                list_data_send.append(dict_dados)
        cont_thre += 1
    cont_four = 0

    for item in list_data_send:
        interacao_cloud.enviar_cadastro_frotas_unico(item, token=params_exec["token"], url=url)
        cont_four += 1
    print(cont_four)
    print('- Replicação de dados finalizado.')
