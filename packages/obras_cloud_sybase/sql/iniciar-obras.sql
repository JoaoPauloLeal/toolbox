--SELECT DISTINCT
--    id = 1,
--    oma.i_entidades,
--    i_obras,
--    cast(MIN(data_medicao) as varchar) as dtHistorico,
--    cmr.id_gerado as id_obra
--FROM sapo.obras_medicao_acompanhamento oma
--LEFT JOIN bethadba.controle_migracao_registro cmr ON (oma.i_obras = cmr.i_chave_dsk2 and tipo_registro = 'obras')
--GROUP BY i_obras, i_entidades, id_obra

SELECT DISTINCT
    id = 1,
    oma.i_entidades,
    i_obras,
	cast(MIN(data_cadastramento) as varchar) as dtHistorico,
    cmr.id_gerado as id_obra
FROM sapo.obras oma
LEFT JOIN bethadba.controle_migracao_registro cmr ON (oma.i_obras = cmr.i_chave_dsk2 and tipo_registro = 'obras'  and cmr.i_chave_dsk1 = {{entidade}})
where oma.i_entidades = {{entidade}}
GROUP BY i_obras, i_entidades, id_obra