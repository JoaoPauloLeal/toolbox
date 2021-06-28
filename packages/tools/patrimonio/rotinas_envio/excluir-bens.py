import requests
import asyncio
import bth.db_connector as db
import bth.cloud_connector as cloud

# Para que funcione corretamente é necessario aliementar corretamente o
# header para que ele consiga colocar os itens em aguardando tombamento.
# Execute até que finalize mais de uma vez.

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    entidadeCloud = kwargs.get('entidadeCloud')
    url = 'https://patrimonio.betha.cloud/patrimonio-services/api/bens'
    for x in cloud.buscaServiceLayer(url=url,token=params_exec['token']):
        url = 'https://patrimonio.betha.cloud/patrimonio-services/api/bens'
        url += f"/{x['id']}"
        print(url)
        print(x['situacaoBem'].get('valor'))
        if x['situacaoBem'].get('valor') == 'EM_USO':
            url2 = f"https://patrimonio.betha.cloud/patrimonio/api/bens/{x['id']}/desfazerTombamento"




            header = {'content-type': 'application/json',
                      'Authorization': f"Bearer 832c60ba-d6f9-4b4d-a462-a0e31cefba96",
                          'user-access': 'e8KuTvFSZTQ=', 'app-context': 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMjEsImluc3VsYXRpb24iOmZhbHNlfX0='}



            asyncio.run(exl(url2=url2, header=header))
        if x['situacaoBem'].get('valor') != 'EM_USO':
            asyncio.run(xl(url=url,token=params_exec['token']))


async def xl(**kwargs):
    print(cloud.ExcluirServiceLayerSemJson(url=kwargs.get('url'), token=kwargs.get('token')))


async def exl(**kwargs):
    url2 = kwargs.get('url2')
    header = kwargs.get('header')
    print(requests.post(url2, headers=header))