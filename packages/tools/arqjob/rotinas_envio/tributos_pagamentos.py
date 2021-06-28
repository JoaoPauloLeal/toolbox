import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud

sistema = 667
tipo_registro = 'tributos_pagos'
url = 'https://tributos.betha.cloud/tributos/dados/api/pagamentos/'


def iniciar_processo_busca(params_exec, *args, **kwargs):
    busca_dados_cloud(params_exec)


def busca_dados_cloud(params_exec):
    print('- Iniciando busca de dados no cloud.')
    lista_dados = []
    contador = 0
    # criterio = "dataPagamento >= 2021-01-01"
    criterio = "dataPagamento >= 2020-01-01 and dataPagamento <= 2020-12-31"
    registro_cloud = interacao_cloud.busca_api_fonte_dados(params_exec, url=url, criterio=criterio)

    print("INICIANDO GRAVAÇÃO EM BANCO DO RETORNO")
    for item in registro_cloud:
        idGerado = item['id']
        
        chave_dsk1 = item['valorPago']
        
        chave_dsk2 = item['valorPagoParcela']
        
        chave_dsk3 = item['valorPagoLancado']
        
        # debito = item['debito']
        # debito = item['debito']
        if item['debito'] is None:
            print(item['debito'])
            debito = None
        else:
            debito = item['debito']

        if debito is None:
            chave_dsk4 = "NONE"
        else:
            chave_dsk4 = debito['idImovel']

        
        # contribuinte = item['contribuinte']
        if item['contribuinte'] is None:
            print(item['debito'])
            contribuinte = None
        else:
            contribuinte = item['contribuinte']
        
        if contribuinte is None:
            chave_dsk5 = "NONE"
        else:
            chave_dsk5 = contribuinte['cpfCnpj']
        
        # chave_dsk6 = contribuinte['nome']
        if contribuinte is None:
            chave_dsk6 = "NONE"
        else:
            chave_dsk6 = contribuinte['nome']
        chave_dsk7 = item['dataCredito']
        chave_dsk8 = item['dataPagamento']
        if item['dataHoraEstorno'] is None:
            chave_dsk9 = 'None'
        else:
            chave_dsk9 = item['dataHoraEstorno']
        print("FINALIZOU")
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, str(chave_dsk4), str(idGerado), str(chave_dsk5), str(chave_dsk6))
        # print(idGerado)
        lista_dados.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de pagamentos tributos no cloud',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2,
            'i_chave_dsk3': chave_dsk3,
            'i_chave_dsk4': chave_dsk4,
            'i_chave_dsk5': chave_dsk5,
            'i_chave_dsk6': chave_dsk6,
            'i_chave_dsk7': chave_dsk7,
            'i_chave_dsk8': chave_dsk8,
            'i_chave_dsk9': chave_dsk9,
        })
        contador += 1
    # print(lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar_bkp(params_exec, lista_req=lista_dados)
    print("FINALIZOU GRAVAÇÃO EM BANCO DO RETORNO")

    # with open(get_path(f'retorno_fonte.json'), "w", encoding='utf-8') as f:
    #     f.write(str(lista_dados))
    #     f.close()

    print(f'Busca de dados finalizada. Foram encotrados {contador} registros.')
    print(f'Tabelas de controle atualizadas com sucesso.')


def get_path(tipo_json):
    path_padrao = f'sistema_origem/ipm_cloud_postgresql/protocolo/json_default/'
    path = path_padrao + tipo_json
    return path