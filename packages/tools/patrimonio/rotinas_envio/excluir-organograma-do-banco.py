import requests
import asyncio
import bth.db_connector as db
import bth.cloud_connector as cloud

# Busca os organogramas constantes na tabela de migração e exclui do cloud.


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    for x in db.consulta_sql(db.get_consulta(params_exec,'consulta-organograma.sql'),index_col='sistema', params_exec=params_exec).to_dict('records'):
        url = f"https://patrimonio.betha.cloud/patrimonio-services/api/organogramas/{x['id_gerado']}"
        print(url)
        print(cloud.ExcluirServiceLayerSemJson(url=url, token=params_exec['token']))