import pandas as pd
import pyodbc
import bth.db_connector as db
import bth.cloud_connector as cloud
import requests

tipo_registro = 'valida-receita-loa'

def inicial(params_exec):
    # Prê Validação
    print('Inicia Validação...')

    sql = db.get_consulta(params_exec, f'valida-receita-loa-pre.sql')
    data = db.consulta_sql(sql)
    for x in data.values:
        print(f'Existe uma receita que esta com desdobramento avaliar/remover desdobramento, Rubrica Principal {x[2]}, Rubrica Auxiliar {x[3]}')
    print('Fim Validação...\n')

    # Execução
    identidade = ''
    sql = f"select * from bethadba.controle_migracao_registro cmr where tipo_registro = 'entidades' and i_chave_dsk1 = {params_exec['entidade']}"
    data = db.consulta_sql(sql)
    for x in data.values:
        identidade = x[4]

    url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/planejamento/loa-receitas'

    sql = db.get_consulta(params_exec, f'valida-receita-loa.sql', 'contabil')
    data = db.consulta_sql(sql)
    print('Consultando Receitas LOA...')
    for item in data:
        print(f'Natureza: {item[8]}')
        print(f'Valor: {item[13]}')

        # criterio = f"loa.exercicio.ano = {item[0]} and natureza.numero = '{item[8]}' and entidade.id = {identidade}"
        criterio = f"loa.exercicio.ano = 2021 and natureza.numero = '11130341000003' and entidade.id = {identidade}"
        campos = 'natureza.numero,metaFinanceira,loa.exercicio.ano,entidade.id,entidade.nome'
        # print(criterio)
        for cr in cloud.buscaFonte(campos=campos, criterio=criterio, url=url, token=params_exec['token']):
            # print(f'item cloud: {cr}')
            if cr.get('metaFinanceira') != float(item[13]):
                print(f"Diferença: Cloud - {cr.get('metaFinanceira')} Desktop - {float(item[13])}")
            # if item == []:
            #     if float(item[13] == float(0)):
            #         print(f'Natureza com valor Zerado no Desk! - (Ignorar) - {item[8]}')
            #     else:
            #         print(f'Sem Natureza: {item[8]}')
            # else:
            #     if float(item['content'][0].get('metaFinanceira')) != float(item[13]):
            #         print(
            #             f"Valor cloud: {float(item['content'][0].get('metaFinanceira'))} | Valor Desk: {float(item[13])} | Natureza: {item[8]}")
            #     print(item['content'][0].get('metaFinanceira'))
    # print('Finzalizado!')