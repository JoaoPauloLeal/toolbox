# Falta corrigir

# Execução
sql = f"select * from bethadba.controle_migracao_registro cmr where tipo_registro = 'entidades' and i_chave_dsk1 = {entidadedsk}"
data = Busca.buscaBanco(sql=sql, banco=conexaobanco)
for x in data:
    identidade = x[4]
url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/pagamentos-empenhos'
sql = f"select * from bethadba.controle_migracao_registro where tipo_registro = 'pagamentos' and i_chave_dsk1 = {entidadedsk} and id_gerado is null"
req = Busca.buscaBanco(sql=sql, banco=conexaobanco)
total = len(data)
cont = 0

for x in req:
    hash = x[2]
    exe_empenho = x[6]
    num_empenho = x[7]
    num_pag = x[11]
    exe_pag = x[9]
    tipo = x[11]
    criterio = f'entidade.id = {identidade} and numeroCadastro.numero = {num_pag} and exercicio.ano = {exe_pag} and empenho.numero = {num_empenho}'
    fields = 'exercicio.ano,numeroCadastro.numero'

    for item in Busca.buscaFonte(token=token, criterio=criterio, fields=fields, url=url):
        cont += 1
        qslq = f"update bethadba.controle_migracao_registro set id_gerado = {item.get('id')} where hash_chave_dsk = '{hash}'"
        Busca.EscreveBanco(banco=conexaobanco, sql=qslq)
        print(f'Executando: {cont}/{total}')