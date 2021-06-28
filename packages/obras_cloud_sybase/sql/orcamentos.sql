SELECT
	id = 1,
	opo.i_entidades,
	opo.i_obras,
	opo.i_planilhas,
	CASE
		WHEN opo.tipo_planilha = 1 THEN 'BASE'
	   	WHEN opo.tipo_planilha = 2 THEN 'CONTRATO'
	   	WHEN opo.tipo_planilha = 3 THEN 'ADITIVO'
	END tipo_planilha,
	cast(opo.data_base as varchar),
	opo.valor,
	cast(opo.data_inclusao as varchar),
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_obra,
	c.numero_contrato
FROM sapo.obras_planilha_orcamentaria opo
LEFT JOIN sapo.contratos c ON (opo.i_contratos = c.i_contratos)
LEFT JOIN bethadba.controle_migracao_registro cmr ON (opo.i_responsaveis = cmr.i_chave_dsk2 AND cmr.tipo_registro='responsaveis')
LEFT JOIN bethadba.controle_migracao_registro cmrr ON (opo.i_obras = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='obras')