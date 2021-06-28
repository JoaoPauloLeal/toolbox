select id = 1,
	i_entidades,
	i_obras,
	nroResponsabilidadeTecnica = '2',
	cast(data_cadastramento as varchar)  as data_inclusao,
	cmr.id_gerado as responsavel,
	cmrr.id_gerado as tipoResponsabilidade,
	cmrrr.id_gerado as id_obra
FROM sapo.obras
INNER JOIN bethadba.controle_migracao_registro cmr ON (cmr.tipo_registro = 'responsaveis' and cmr.i_chave_dsk1 = {{entidade}})
INNER JOIN bethadba.controle_migracao_registro cmrr ON (cmrr.i_chave_dsk3 = 'Outra' AND cmrr.tipo_registro = 'tipo-responsabilidade-tecnica')
INNER JOIN bethadba.controle_migracao_registro cmrrr ON (cmrrr.tipo_registro = 'obras' and cmrrr.i_chave_dsk1 = {{entidade}} and cmrrr.i_chave_dsk2 = i_obras)
WHERE i_entidades = {{entidade}}