import json
import requests
import settings
import time
import random


def envia_registro(url, body, params_exec, multipart=False, nome_campo='obras'):
    id_gerado = None
    content = None
    try:
        headers = {
            'authorization': f"Bearer {params_exec['token']}",
            'app-context': params_exec['appcontext'],
            'user-access': params_exec['useraccess'],
            'content-type': 'application/json'
        }

        r = requests.post(url=url, headers=headers, data=body)
        print(r.status_code)
        print(r.text)
        if r.ok:
            retorno = json.loads(r.content.decode('utf8'))
            id_gerado = retorno['id']
            content = r.content
        else:
            print('Erro ao enviar requisição: ', r.status_code, r.content)
    finally:
        return id_gerado, content


def get_service_layer(params_exec, **kwargs):
    hasNext = True
    pagina = 0
    limit = 500
    headers = {
        'authorization': f"bearer {params_exec['token']}",
        'content-type': 'application/json'
    }
    url = kwargs.get('url')

    while hasNext:
        responta_req = requests.get(url=url, headers=headers, params={'limit': limit, 'offset': pagina * limit})
        pagina += 1
        if responta_req.status_code != 200:
            print('Erro na conexão !')
            print('Revise os parametros enviados')
            print(f'Token : {settings.CLOUD_TOKEN}')
            print(f'URL : {url}')
        elif responta_req.json().get('content') == []:
            print('Retorno vazio!', responta_req.json())
        else:
            retorno = responta_req.json().get('content')
            return retorno

        hasNext = responta_req.json().get('hasNext')


# Contabil
def buscaFonte(**kwargs):
    url = kwargs.get('url')
    token = settings.TOKEN_MIGRACAO
    criterio = kwargs.get('criterio')
    fields = kwargs.get('fields')

    # print(url)
    # print(token)
    # print(criterio)
    # print(fields)

    header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}

    hasNext = True
    limit = 100
    offset = 0
    pagina = 0
    lista = []

    while hasNext:
        offset = pagina * limit
        req = requests.get(url=url, headers=header, params={'limit': limit, 'offset': offset, 'fields': str(fields), 'filter': str(criterio)})
        pagina += 1
        # print(req.json())
        if req.ok:
                for item in req.json()['content']:
                    lista.append(item)
                if req.json().get("hasNext") == False:
                    hasNext = False
        else:
            print(f'Problemas na requisição! {req.text} - {req.status_code}')
            hasNext = False
    return lista

def buscaServiceLayer(**kwargs):
    url = kwargs.get('url')
    token = kwargs.get('token')

    header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}

    hasNext = True
    limit = 100
    offset = 0
    pagina = 0
    lista = []

    while hasNext:
        offset = pagina * limit
        req = requests.get(url=url, headers=header, params={'limit': limit, 'offset': offset})
        pagina += 1
        # print(req.json())
        if req.ok:
                for item in req.json()['content']:
                    lista.append(item)
                if req.json().get("hasNext") == False:
                    hasNext = False
        else:
            print(f'Problemas na requisição! {req.text} - {req.status_code}')
            hasNext = False
    return lista

def ExcluirServiceLayerSemJson(**kwargs):
    url = kwargs.get('url')
    token = kwargs.get('token')

    header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}
    req = requests.delete(url, headers=header)
    return 1


def ExcluirServiceLayerComJson(**kwargs):
    url = kwargs.get('url')
    token = kwargs.get('token')
    data = kwargs.get('data')
    # print(url)
    # print(token)
    # print(data)

    header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}
    # print(header)
    req = requests.delete(url=url,headers=header,data=data)
    return req.json(),req.ok,req.status_code


# Compras

def requisicao_service_layer(url_sl, json_req=False):
    metodo = int(input('1 - GET / 2 - POST / 3 - PUT / 4 - DEL'))
    try:
        headers = {
            'authorization': f'Bearer {settings.TOKEN_MIGRACAO}',
            'app-context': settings.APP_CONTEXT,
        }

        if metodo == 1:
            print(f'URL DE ENVIO : {url_sl} | Headers = {headers}')
            r = requests.get(url=url_sl, headers=headers)
        if metodo == 2:
            r = requests.post(url=url_sl, headers=headers, data=json_req)
        if metodo == 3:
            r = requests.put(url=url_sl, headers=headers, data=json_req)
        if metodo == 4:
            r = requests.delete(url=url_sl, headers=headers)

        if r.ok:
            retorno = json.loads(r.content.decode('utf8'))
            id_gerado = retorno['id']
        else:
            print('Erro ao enviar requisição: ', r.status_code, r.content)
    finally:
        return id_gerado, r.content



def buscaLoteConversoesCompras(**kwargs):
    url = kwargs.get('url')
    token = kwargs.get('token')
    header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}

    hasNext = True

    while hasNext:
        req = requests.get(url, headers=header)
        if req.ok:
            # print(req.json())
            if str(req.json().get('statusLote') == 'EXECUTADO'):
                # print(req.json())
                for x in req.json()['retorno']:
                    print(f"{req.json().get('statusLote')} - IdGerado: {x.get('idGerado')}")
        else:
            print('Problemas na requisição!')
            hasNext = False
        time.sleep(3)
    return 'Fim'


def retorna_url_servico_compras(**kwargs):
    url = 'https://compras.betha.cloud/compras-services'
    if kwargs.get('servico') == 'get_processoadm': url += f"/api/exercicios/{kwargs.get('exercicio')}/processos-administrativo"
    if kwargs.get('servico') == 'del_processoadm': url += f"/api/exercicios/{kwargs.get('exercicio')}/processos-administrativo/{kwargs.get('id_proc')}"
    if kwargs.get('servico') == 'get_fornecedores_id': url += f"/api/fornecedores/{kwargs.get('id_fornecedor')}"
    if kwargs.get('servico') == 'get_fornecedores': url += f"/api/fornecedores"
    if kwargs.get('servico') == 'get_entidades_processo': url += f"/api/exercicios/{kwargs.get('exercicio')}/processos-administrativo/{kwargs.get('id_proc')}/entidades"
    if kwargs.get('servico') in ('del_entidades_processo', 'put_entidades_processo'):
        url += f"/api/exercicios/{kwargs.get('exercicio')}/processos-administrativo/{kwargs.get('id_proc')}/entidades/{kwargs.get('id_entidade')}"
    if kwargs.get('servico') == 'entidades': url += '/api/entidades'
    return url

# API


def geraCpf(): n = [random.randrange(10) for a in range(9)];\
o=11-sum(map(int.__mul__,n,range(10+len(n)-9,1,\
-1)))%11;n+=[(o>=10 and[0]or[o])[0]];o=11-sum( \
map(int.__mul__,n,range(10+len(n)-9,1,-1)))%11;n+=[(o>=10 and[0]\
or[o])[0]];return"%d%d%d%d%d%d%d%d%d%d%d"%tuple(n)
