SELECT
    id = 1,
    oc.i_entidades,
    oc.i_obras,
    oc.i_contratos,
    cast(oc.data_inclusao as varchar) data_inclusao,
    oc.i_responsaveis,
    ISNULL(c.contrato_sup, c.numero_contrato) as numero_contrato,
    c.i_contratos as contrato_dsk,
    cmr.id_gerado as id_obra
FROM sapo.obras_contratos oc
INNER JOIN sapo.contratos c ON (oc.i_contratos = c.i_contratos)
INNER JOIN bethadba.controle_migracao_registro cmr ON (oc.i_obras = cmr.i_chave_dsk2 and cmr.tipo_registro='obras')