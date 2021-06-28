# Falta corrigir

# Execução
sql = f"select * from bethadba.controle_migracao_registro cmr where tipo_registro = 'entidades' and i_chave_dsk1 = {entidadedsk}"
for x in Busca.buscaBanco(sql=sql, banco=conexaobanco):
    entidadedsk = x[4]

url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/liquidacoes-empenhos'
sql = "select * from bethadba.controle_migracao_registro where tipo_registro = 'liquidacoes' and i_chave_dsk1 = 8 and id_gerado is null and i_chave_dsk6='LIQ'"
data = Busca.buscaBanco(sql=sql, banco=conexaobanco)
cont = 0
total = len(data)
for x in data:
    hash = x[2]
    exe_empenho = x[6]
    num_empenho = x[7]
    num_liq = x[8]
    exe_liq = x[9]
    tipo = x[10]
    criterio = f'entidade.id = {entidadedsk} and numeroCadastro.numero = {num_liq} and empenho.numero = {num_empenho} and empenho.exercicio.ano = {exe_empenho} and exercicio.ano = {exe_liq}'
    fields = 'exercicio.ano,numeroCadastro.numero'
    fonte = Busca.buscaFonte(url=url, criterio=criterio, fields=fields, token=token)
    for item in fonte:
        cont += 1
        qslq = f"update bethadba.controle_migracao_registro set id_gerado = {item.get('id')} where hash_chave_dsk = '{hash}'"
        Busca.EscreveBanco(sql=qslq, banco=conexaobanco)
        print(f'Executando: {cont}/{total}')