SELECT
    id = 1,
    i_entidades,
    i_matricula_cei,
    cast(data_cadastro as varchar) data_cadastro,
    cast(data_emissao_cnd as varchar) data_emissao_cnd,
    cast(data_validade_cnd as varchar) data_validade_cnd,
    numero_cnd
FROM sapo.obras_matricula_cei