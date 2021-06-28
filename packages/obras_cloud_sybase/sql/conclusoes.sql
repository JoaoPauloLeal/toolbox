SELECT
	id = 1,
	oma.i_medicao_acompanhamento,
	oma.i_entidades,
	oma.i_obras,
	oma.observacao,
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_obra,
	cast(oma.data_medicao as varchar) as data_medicao
FROM sapo.obras_medicao_acompanhamento oma
LEFT JOIN bethadba.controle_migracao_registro cmr ON (oma.i_responsaveis = cmr.i_chave_dsk2 AND cmr.tipo_registro='responsaveis')
LEFT JOIN bethadba.controle_migracao_registro cmrr ON (oma.i_obras = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='obras')
WHERE oma.tipo_acompanhamento=3