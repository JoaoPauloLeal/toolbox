SELECT
    id = 1,
    A.*,
    B.id_gerado as i_classificacao_obra,
    B.i_chave_dsk3 as classificacao_obra_desc
FROM sapo.tipo_obras A
LEFT JOIN bethadba.controle_migracao_registro B on A.classificacao_obra = B.i_chave_dsk2 and B.tipo_registro = 'classificacao-obra'
WHERE A.i_entidades = {{entidade}}