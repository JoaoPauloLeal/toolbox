SELECT DISTINCT
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
FROM sapo.obras_planilha_orcamentaria opo
INNER JOIN sapo.responsaveis r ON (opo.i_responsaveis = r.i_responsaveis)
INNER JOIN bethadba.controle_migracao_registro cmr ON (cast(r.tipo as varchar) = cmr.i_chave_dsk2 AND cmr.tipo_registro = 'tipo-responsavel')
UNION
SELECT DISTINCT
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
FROM sapo.obras_medicao_acompanhamento oma
INNER JOIN sapo.responsaveis r ON (oma.i_responsaveis = r.i_responsaveis)
INNER JOIN bethadba.controle_migracao_registro cmr ON (cast(r.tipo as varchar) = cmr.i_chave_dsk2 AND cmr.tipo_registro = 'tipo-responsavel')
UNION
SELECT DISTINCT
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
FROM sapo.obras_art oa
INNER JOIN sapo.responsaveis r ON (oa.i_responsaveis = r.i_responsaveis)
INNER JOIN bethadba.controle_migracao_registro cmr ON (cast(r.tipo as varchar) = cmr.i_chave_dsk2 AND cmr.tipo_registro = 'tipo-responsavel')