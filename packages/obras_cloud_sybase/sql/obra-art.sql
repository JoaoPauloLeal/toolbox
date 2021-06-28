SELECT
	id = 1,
	i_entidades,
	i_obras,
	i_art as nroResponsabilidadeTecnica,
	cast(data_inclusao as varchar),
	cmr.id_gerado as responsavel,
	cmrr.id_gerado as tipoResponsabilidade,
	cmrrr.id_gerado as id_obra
FROM sapo.obras_art oa
INNER JOIN bethadba.controle_migracao_registro cmr ON (oa.i_responsaveis = cmr.i_chave_dsk2 AND cmr.tipo_registro = 'responsaveis')
INNER JOIN bethadba.controle_migracao_registro cmrr ON (oa.tipo_rt = cmrr.i_chave_dsk2 AND cmrr.tipo_registro = 'tipo-responsabilidade-tecnica')
INNER JOIN bethadba.controle_migracao_registro cmrrr ON (oa.i_obras = cmrrr.i_chave_dsk2 AND cmrrr.tipo_registro = 'obras')