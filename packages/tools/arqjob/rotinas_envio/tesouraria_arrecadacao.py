import bth.interacao_cloud as interacao_cloud
import requests

sistema = 667
tipo_registro = 'tesouraria_arrecadacao'
url = 'https://contabilidade.cloud.betha.com.br/contabilidade/api/contabil/arrecadacoes-consolidadas?filter=((+(+valor+%3E%3D+653+and+valor+%3C%3D+653+)+))+and+(classificavel%3Dfalse)&limit=20&offset=0'


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    lista_dados = []
    contador = 0
    print("EMPACOU")
    registro_intranet = interacao_cloud.busca_dados_cloud_api_tela(params_exec, url=url)
    print("EMPACOU")

    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_intranet:
        print(item)
        headers = {'authorization': f'bearer {params_exec["token"]}',
                   'app-context': 'eyJleGVyY2ljaW8iOjIwMjB9',
                   'user-access': '6v22r9-WajTaHXLQVNZyhA==',
                   'content-type': 'application/json'
                   }
        print(item['id'])
        urlPut = f'https://contabilidade.cloud.betha.com.br/contabilidade/api/contabil/arrecadacoes-orcamentarias/{item["id"]}'
        print(urlPut)
        retorno_req = requests.put(urlPut, headers=headers, data=item, )
        print(retorno_req.status_code)
        contador += 1
    # print(lista_controle_migracao)
    # model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_dados)
    # print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')
