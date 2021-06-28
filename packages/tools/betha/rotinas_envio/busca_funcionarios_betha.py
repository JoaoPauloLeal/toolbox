import requests

sistema = 666
tipo_registro = 'funcionarios-betha'
url = 'https://intranet.betha.com.br/localizadordefuncionarios'


def iniciar_processo_envio(params_exec, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de funcionarios na intranet da betha.')
    headers = params_exec.get('token')
    # headers = {'authorization': f'bearer {token}', 'content-type': 'application/json'}
    registro_intranet = requests.get(url, headers=headers)
    contador = 0
    print(registro_intranet.text)
    # print(registro_intranet)

    # for item in registro_intranet.json():
    #     contador += 1
    # model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_dados)

    # with open(get_path(f'retorno_fonte.json'), "w", encoding='utf-8') as f:
    #     f.write(str(lista_dados))
    #     f.close()

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path