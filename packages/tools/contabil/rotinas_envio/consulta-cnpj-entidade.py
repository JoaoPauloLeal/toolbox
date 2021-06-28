import requests


def iniciar_processo_busca(params_exec, *args, **kwargs):
    url = 'https://compras.betha.cloud/compras-services/api/entidades'
    headers = {
        'authorization': f"Bearer {params_exec['token']}",
        'content-type': 'application/json'
    }
    resp = requests.get(url=url, headers=headers)
    for x in resp.json().get('content'):
        cnpj = x.get('cnpj')
        print(x.get('nome'))
        print(f'CNPJ : {cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}')
