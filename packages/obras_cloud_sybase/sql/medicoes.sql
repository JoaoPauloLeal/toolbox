SELECT
	id = 1,
	oma.i_medicao_acompanhamento,
	oma.i_entidades,
	oma.percentual_fisico,
	oma.i_obras,
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_tipo_medicao,
	cmrrr.id_gerado as id_obra,
	oma.observacao,
	cast(oma.data_medicao as varchar) as data_medicao,
	c.numero_contrato as numero_contrato
FROM sapo.obras_medicao_acompanhamento oma
LEFT JOIN sapo.contratos c ON (oma.i_contratos = c.i_contratos)
LEFT JOIN bethadba.controle_migracao_registro cmr ON (oma.i_responsaveis = cmr.i_chave_dsk2 AND cmr.tipo_registro='responsaveis')
LEFT JOIN bethadba.controle_migracao_registro cmrr ON (oma.tipo_medicao = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='tipo-medicao')
LEFT JOIN bethadba.controle_migracao_registro cmrrr ON (oma.i_obras = cmrrr.i_chave_dsk2 AND cmrrr.tipo_registro='obras')
WHERE oma.tipo_acompanhamento=1