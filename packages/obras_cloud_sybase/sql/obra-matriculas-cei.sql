SELECT
    id = 1,
    i_entidades,
    i_obras,
    i_matricula_cei,
    cast(data_cadastro as varchar) data_cadastro,
    cmr.id_gerado as id_obra,
    cmrr.id_gerado as id_matricula
FROM sapo.obras_matricula_cei omc
LEFT JOIN bethadba.controle_migracao_registro cmr ON (omc.i_obras = cmr.i_chave_dsk2 and cmr.tipo_registro='obras')
LEFT JOIN bethadba.controle_migracao_registro cmrr ON (omc.i_matricula_cei = cmrr.i_chave_dsk2 and cmrr.tipo_registro='matriculas-cei')