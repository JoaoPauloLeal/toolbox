import bth.cloud_connector as cloud
import bth.db_connector as db

# Falta corrigir

# Execução

def iniciar_processo_busca(params_exec, *args, **kwargs):
    url = f"https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/empenhos"
    entidade = db.busca_id_entidade_migracao(params_exec)

    criterio = f'entidade.id = {entidade}'
    campos = 'id'
    cont = 0
    txt = ''
    for x in cloud.buscaFonte(url=url, criterio=criterio, fields=campos, token=params_exec['token']):
        # print(x['idGerado'].get('id'))
        if db.consulta_sql(f"select * from bethadba.controle_migracao_registro where tipo_registro = 'empenhos' and id_gerado = {x['idGerado'].get('id')}",index_col='i_chave_dsk1').get('id_gerado').count() == 0:
            print(x['content'].get('numeroCadastro'))

    # for conteudo in cloud.buscaFonte():
    #
    #     header = {'content-type': 'application/json', 'Authorization': f'Bearer {token}'}
    #     while hasNext:
    #             offset = pagina * limit
    #             req = requests.get(url, headers=header, params={'limit': limit, 'offset': offset})
    #             if req.json().get("hasNext") == False:
    #                 hasNext = False
    #             lista = []
    #             contador = 0
    #             for x in req.json().get('content'):
    #                 contador += 1
    #                 processado = True
    #
    #                 gg = {
    #                     "idIntegracao": f"{contador}",
    #                         "idGerado": {
    #                             "id": x.get('idGerado').get('id')
    #                         },
    #                         "content": {
    #                         "exercicio": int(x.get('content').get('data')[0:4].replace("'",''))
    #                         }
    #                     }
    #                 print(contador)
    #
    #                 if(contador < 10):
    #                     lista.append(js.dumps(gg))
    #                 else:
    #                     contador = 0
    #                     json = str(lista).replace("'",'')
    #                     print(json)
    #                     req = requests.delete(url, headers=header, data=json)
    #                     lote = req.json().get('idLote')
    #                     req = requests.get(f'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/lotes/{req.json().get("idLote")}',headers=header)
    #                     while processado:
    #                         if(req.json().get('status') == 'EXECUTADO' or req.json().get('status') =='EXECUTADO_PARCIALMENTE'):
    #                             processado = False
    #                             print(req.json())
    #                         else:
    #                             req = requests.get(f'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/lotes/{lote}',headers=header)
    #                             print(req.json())
    #                             time.sleep(3)
    #                     print(req.json())
    #                     lista = []
    #             pagina += 1

