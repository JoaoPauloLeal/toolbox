import bth.db_connector as db
import bth.cloud_connector as cloud
import json as json


def iniciar_processo_busca(params_exec, *args, **kwargs):
    url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/comprovantes'
    fields = 'id'
    criterio = f"entidade.id = {db.busca_id_entidade_migracao(params_exec)}"

    urlEx = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/comprovantes'
    cont = 0
    lista = []
    for x in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
        cont += 1
        js = {"idIntegracao": str(cont),"idGerado": {"id": x['id']},"content": {"exercicio": int(params_exec['exercicio'])}}
        lista.append(js)
        if cont == 50:
            js = json.dumps(lista)
            print(cloud.ExcluirServiceLayerComJson(url=urlEx, token=params_exec['token'], data=js))
            print('Enviado!')
            lista = []
            cont = 0

    print(cont)