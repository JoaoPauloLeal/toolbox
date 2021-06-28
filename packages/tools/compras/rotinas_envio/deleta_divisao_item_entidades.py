import bth.db_connector as db
import bth.cloud_connector as cloud


def iniciar_processo_busca(params_exec, ano):
    processos = cloud.buscaFonte(
        params_exec,
        url='https://compras.betha.cloud/compras/dados/api/processoadministrativoentidadeparticipanteitem',
        fields='',
        criterio=f'entidade.id = {db.busca_id_entidade_migracao(params_exec)}'
    )
    print(processos)
    # exercicio = 2019
    # url =f"https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/itens/{itemId}/entidades"
    # config_item = cloud.get_service_layer(param_exec, url=)