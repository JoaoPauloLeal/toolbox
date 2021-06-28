SELECT
	id = 1,
	i_entidades,
	i_obras,
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_obra,
	cast(data_conclusao as varchar) as data_medicao,
	observacao = 'Migração',
	i_medicao_acompanhamento = 'null'
FROM sapo.obras
INNER JOIN bethadba.controle_migracao_registro cmr ON (cmr.tipo_registro = 'responsaveis' and cmr.i_chave_dsk1 = {{entidade}})
INNER JOIN bethadba.controle_migracao_registro cmrr ON (i_obras = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='obras' AND cmrr.i_chave_dsk1 = {{entidade}})
WHERE i_entidades = {{entidade}} and data_conclusao is not null