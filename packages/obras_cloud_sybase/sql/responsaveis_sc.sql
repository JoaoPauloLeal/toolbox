
SELECT DISTINCT first
    id = 1,
    r.i_entidades,
    r.i_responsaveis,
    r.nome,
    r.cpf,
    r.identidade as rg,
    r.orgao_emis_rg,
    r.uf_emis_rg,
    cast(r.dt_emis_rg as varchar),
    r.email,
    r.tipo,
    ISNULL(r.num_registro, r.num_matricula) as num_registro,
    r.telefone_cel,
    cmr.id_gerado as tipo_responsavel,
    cmr.i_chave_dsk3 as descricao
--    cmr.id_gerado as tipo_responsavel,
--    cmr.i_chave_dsk3 as descricao
FROM sapo.responsaveis r
INNER JOIN bethadba.controle_migracao_registro cmr ON (cast(r.tipo as varchar) = cmr.i_chave_dsk2 AND cmr.tipo_registro = 'tipo-responsavel')
WHERE r.i_entidades = {{entidade}}
