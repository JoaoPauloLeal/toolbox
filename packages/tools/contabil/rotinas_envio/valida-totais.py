import bth.db_connector as db
import bth.cloud_connector as cloud

# Execuação


def iniciar_processo_busca(params_exec, *args, **kwargs):
    sql = f"select * from bethadba.controle_migracao_registro cmr where tipo_registro = 'entidades' and i_chave_dsk1 = {params_exec['entidade']}"
    identidade = db.busca_id_entidade_migracao(params_exec)

# Arrecadação
    sql = f"select sum(valor) as valor,1 as sistema from sapo.arrecadacoes a where ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']} and SUBSTRING(i_rubricas,0,1) = 4"
    for x in db.consulta_sql(sql,index_col='sistema', params_exec=params_exec):
        valorArrecDsk = x['valor']
        valorArrecCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/arrecadacoes-orcamentarias'
        criterio = f"exercicio.ano = {str(params_exec['exercicio'])} and entidade.id = {db.busca_id_entidade_migracao(params_exec)}"
        fields = 'valor'
        for y in cloud.buscaFonte(criterio=criterio, fields=fields, url=url, token=params_exec['token']):
            valorArrecCloud += float(y.get('valor'))
        if float(valorArrecDsk) != round(valorArrecCloud,2):
            print(f'Valor de arrecadação: Cloud: R$ {round(valorArrecCloud,2)} Desktop: R$ {float(valorArrecDsk)} - Inconsistente')
        else:
            print(f'Valor de arrecadação: Cloud R$ {round(valorArrecCloud,2)} Desktop R$ {float(valorArrecDsk)} - OK')

    # Despesa Extra
    sql = f"select sum(valor) as valor,1 as sistema from sapo.despexs d where ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']}"
    for x in db.consulta_sql(sql, index_col='sistema', params_exec=params_exec):
        valorArrecDsk = x['valor']
        valorArrecCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/despesas-extras'
        criterio = f"exercicio.ano = {params_exec['exercicio']} and entidade.id = {db.busca_id_entidade_migracao(params_exec)}"
        fields = 'valor'
        for y in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            valorArrecCloud += float(y.get('valor'))
        if float(valorArrecDsk) != round(valorArrecCloud,2):
            print(f'Valor de D.E: Cloud: R$ {round(valorArrecCloud,2)} Desktop: R$ {float(valorArrecDsk)} - Inconsistente')
        else:
            print(f'Valor de D.E: Cloud R$ {round(valorArrecCloud,2)} Desktop R$ {float(valorArrecDsk)} - OK')

    # Empenhos
    sql = f"select sum(valor) as valor,1 as sistema from sapo.empenhos where ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']}"
    for x in db.consulta_sql(sql, index_col='sistema', params_exec=params_exec):
        valorArrecDsk = x['valor']
        valorArrecCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/empenhos'
        criterio = f"exercicio.ano = {params_exec['exercicio']} and entidade.id = {db.busca_id_entidade_migracao(params_exec)}"
        fields = 'valor'
        for y in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            valorArrecCloud += float(y.get('valor'))
        if float(valorArrecDsk) != round(valorArrecCloud,2):
            print(f'Valor de Empenhos: Cloud: R$ {round(valorArrecCloud,2)} Desktop: R$ {float(valorArrecDsk)} - Inconsistente')
        else:
            print(f'Valor de Empenhos: Cloud R$ {round(valorArrecCloud,2)} Desktop R$ {float(valorArrecDsk)} - OK')

    # Anulação Empenhos
    sql = f"select sum(valor) as valor,1 as sistema from sapo.anl_empenhos ae where ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']}"
    for x in db.consulta_sql(sql, index_col='sistema', params_exec=params_exec):
        valorArrecDsk = x['valor']
        valorArrecCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/anulacoes-empenhos'
        criterio = f"empenho.exercicio.ano = {params_exec['exercicio']} and empenho.entidade.id = {db.busca_id_entidade_migracao(params_exec)}"
        fields = 'anulacao.valor'
        for y in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            valorArrecCloud += float(y['anulacao'].get('valor'))
        if float(valorArrecDsk) != round(valorArrecCloud, 2):
            print(
                f'Valor de Anulação Empenhos: Cloud: R$ {round(valorArrecCloud, 2)} Desktop: R$ {float(valorArrecDsk)} - Inconsistente')
        else:
            print(
                f'Valor de Anulação Empenhos: Cloud R$ {round(valorArrecCloud, 2)} Desktop R$ {float(valorArrecDsk)} - OK')

    # Liquidações
    sql = f"select sum(liq.valor) as valor,1 as sistema from sapo.liquidacao liq where liq.ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']}"
    for x in db.consulta_sql(sql, index_col='sistema', params_exec=params_exec):
        valorLiqDsk = float(x['valor'])
        valorLiqCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/liquidacoes-empenhos'
        criterio = f"exercicio.ano = {params_exec['exercicio']} and entidade.id = {db.busca_id_entidade_migracao(params_exec)} and restoPagar = false"
        fields = 'valor'
        for y in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            valorLiqCloud += float(y.get('valor'))
        if float(valorLiqDsk) != round(valorLiqCloud,2):
            print(f'Valor de Liquidacoes: Cloud: R$ {round(valorLiqCloud,2)} Desktop: R$ {float(valorLiqDsk)} - Inconsistente')
        else:
            print(f'Valor de Liquidacoes: Cloud R$ {round(valorLiqCloud,2)} Desktop R$ {float(valorLiqDsk)} - OK')

    # Anulação de Liquidações
    sql = f"select sum(valor) as valor,1 as sistema from sapo.anl_liquidacoes al where ano_exerc = {params_exec['exercicio']} and i_entidades = {params_exec['entidade']}"
    for x in db.consulta_sql(sql, index_col='sistema', params_exec=params_exec):
        valorLiqDsk = float(x['valor'])
        valorLiqCloud = 0
        url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/anulacoes-liquidacoes-empenhos'
        criterio = f"liquidacao.exercicio.ano = {params_exec['exercicio']} and liquidacao.entidade.id = {db.busca_id_entidade_migracao(params_exec)} and restoPagar = false"
        fields = 'anulacao.valor'
        for y in cloud.buscaFonte(url=url, criterio=criterio, fields=fields, token=params_exec['token']):
            valorLiqCloud += float(y['anulacao'].get('valor'))
        if float(valorLiqDsk) != round(valorLiqCloud,2):
            print(f'Valor de Anl Liquidacoes: Cloud: R$ {round(valorLiqCloud,2)} Desktop: R$ {float(valorLiqDsk)} - Inconsistente')
        else:
            print(f'Valor de Anl Liquidacoes: Cloud R$ {round(valorLiqCloud,2)} Desktop R$ {float(valorLiqDsk)} - OK')
    print('Fim Execução!')