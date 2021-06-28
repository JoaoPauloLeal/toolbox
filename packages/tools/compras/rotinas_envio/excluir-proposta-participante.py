import db_connector as db
import cloud_connector as cloud

def inicial(params_exec):
    sql = f"select * from bethadba.controle_migracao_registro cmr where tipo_registro = 'entidades' and i_chave_dsk1 = {params_exec['entidade']}"
    entidadedsk = ''
    for x in db.consulta_sql(sql):
        entidadedsk = x[4]

    url = 'https://compras.betha.cloud/compras/dados/api/processosadministrativosparticipantes'

    criterio = f'entidade.id = {entidadedsk}'
    print(criterio)
    retorno = cloud.buscaFonte(url=url,
                               criterio=criterio, fields='fornecedor.id,id,processoAdministrativo.parametroExercicio'
                                                         '.exercicio,processoAdministrativo.id,'
                                                         'processoAdministrativo.numeroProcesso,'
                                                         'processoAdministrativo.dataProcesso', token=params_exec['token'])

    for x in retorno:
        print(x)
        exercicio = x.get('processoAdministrativo').get('parametroExercicio').get('exercicio')
        processoAdministrativoId = x.get('processoAdministrativo').get('id')
        id = x.get('id')
        urlEx = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/participante-licitacao/{id}'
        print(urlEx)
        print(cloud.ExcluirServiceLayerSemJson(url=urlEx, token=params_exec['token']))