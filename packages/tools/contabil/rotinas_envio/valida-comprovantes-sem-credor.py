import bth.db_connector as db
import bth.cloud_connector as cloud
import json as json

# Excluir dados dentro do arquivo comprovantes na pasta relatorios.
# Após executado caso tenha alguma coisa para demonstrar será gerado dentro do arquivo.


def iniciar_processo_busca(params_exec, *args, **kwargs):
    arquivo = open(f"packages/tools/contabil/relatorios/comprovantes.txt", 'a')
    for x in db.consulta_sql(db.get_consulta(params_exec, 'valida-comprovantes-sem-credor.sql'), params_exec=params_exec, index_col='tipo_registro'):
        arquivo.writelines(f"Documento Fiscal {x['numero']} sem credor cadastrado no sistema com CNPJ {x['chave_dsk5']} no exercicio de {x['exercicio']}\n")
    print('Finalizado!')
