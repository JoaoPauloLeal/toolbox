import json
import time

import requests as rq

url_base = 'https://compras.betha.cloud/compras-services/api/'


def preencher_entidade_item_proc(**kwargs):
    headers = {
        'authorization': f"Bearer {kwargs.get('token')}",
        'content-type': 'application/json'
    }
    hasNext = True
    limit = 500
    pagina = 0
    offset = pagina * limit

    lista_proc = []

    url_proc_itens = 'https://compras.betha.cloud/compras/dados/api/processosadministrativositens'
    criterio_proc_itens = 'configuracao.processoAdministrativo.parametroExercicio.exercicio = 2021 and entidade.id = ' \
                          '663 and configuracao.processoAdministrativo.id = 244766'
    campos_proc_itens = 'id, configuracao(processoAdministrativo(id)), ' \
                        'configuracao.processoAdministrativo.parametroExercicio.exercicio, quantidade '
    count = 0
    while hasNext:  #busca todos os processos e os itens de cada processo
        print('buscand processo...')
        itens_proc = rq.get(
            url=url_proc_itens,
            headers=headers,
            params={
                'limit': limit,
                'offset': offset,
                'filter': criterio_proc_itens,
                'fields': campos_proc_itens
            }
        )
        time.sleep(1)

        for count, x in enumerate(itens_proc.json().get('content')):
            print('adicionando item', count)
            dict_item_proc = {
                'exercicio': x.get('configuracao').get('processoAdministrativo').get('parametroExercicio').get('exercicio'),
                'id_item': x.get('id'),
                'id_proc': x.get('configuracao').get('processoAdministrativo').get('id'),
                'quantidade': x.get('quantidade')
            }
            lista_proc.append(dict_item_proc)
            hasNext = itens_proc.json().get('hasNext')
        count += 1

    lista_envio = []

    for item_lista in lista_proc:
        url_nova = f"{url_base}exercicios/{item_lista.get('exercicio')}/processos-administrativo/{item_lista.get('id_proc')}/itens/{item_lista.get('id_item')}/entidades"

        rmv_itens = rq.get(
            url=url_nova,
            headers=headers,
        )
        print(rmv_itens.status_code)


        if rmv_itens.ok:
            retorno = rmv_itens.json()

            if not retorno['content']:
                lista_envio.append(item_lista)
        else:
            print('Erro na requisição')
            time.sleep(28)


    # ADD PARTICIPANTE
    for count, it in enumerate(lista_envio):
        url_busca = f"{url_base}exercicios/{it.get('exercicio')}/processos-administrativo/{it.get('id_proc')}/entidades"
        resp = rq.get(url=url_busca, headers=headers)
        time.sleep(1)
        lista_envio[count].update({
                        'id_ent_part': resp.json().get('content')[0].get('id')
        })

    for envia in lista_envio:
        url_envio = f"{url_base}exercicios/{envia.get('exercicio')}/processos-administrativo/{envia.get('id_proc')}/itens/{envia.get('id_item')}/entidades"
        json_envio = {
            'item': {
                'id': int(envia.get('id_item'))
            },
            'entidadeParticipante': {
                'id': int(envia.get('id_ent_part'))
            },
            'quantidadeDistribuida': int(envia.get('quantidade'))
        }

        retorno = rq.post(url=url_envio, headers=headers, data=json.dumps(json_envio))
        if retorno.status_code == 201:
            print('Item adicionado com sucesso !')



preencher_entidade_item_proc(token='b5e7f13a-47d9-427f-8c48-7fa7dd952d2f')