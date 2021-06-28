import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

url = "https://api.protocolo.betha.cloud/protocolo/service-layer/v1/api/upload/processos"


def iniciar_processo_envio(params_exec, ano, *args, **kwargs):
    filename = 'id_gerado.json'
    hash_chaves = str('ENVIO123')
    fileObj = get_path(filename)
    m = MultipartEncoder(
        fields={'file': (filename, open(fileObj, "rb"), 'tipodoarquivo'),
                'idIntegracao': filename}
    )
    print('EMPACOU 2')
    response = requests.post(url=url, data=m, headers={'content-type': m.content_type, 'Authorization': 'Bearer b8ddcd70-e359-4814-bdfb-6aff6f02522d'})
    print('EMPACOU 3')
    print(response.request.headers)
    print(response.request.body)
    print(response.text)


def get_path(nome_arquivo):
        path_padrao = f'sistema_origem/ipm_cloud_postgresql/frotas/json_default/'
        path = path_padrao + nome_arquivo
        return path