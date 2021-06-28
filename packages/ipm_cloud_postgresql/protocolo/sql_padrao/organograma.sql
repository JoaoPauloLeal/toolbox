SELECT id as key1,
       id,
       public.bth_get_id_gerado('304','organogramaConfiguracao','2') as organogramaconfig,
       descricao as nome,
       nivel,
       sigla,
       'true' as ativo,
       'true' as protocolizacao,
       'ANALISTA' as atribuicao,
       left(((case when nivel = '1' then '' else ano::varchar end) || organograma || '000000000000000'),15)  as "mascara",
       left(((case when nivel = '1' then '' else ano::varchar end) || organograma || '000000000000000'),15)  as "mascaraCompleta",
       right(((case when nivel = '1' then '' else ano::varchar end) || organograma || '00.000.000.0000'),15)  as "mascaraFormatada"
  FROM (
        -- Nível 1 - Orgãos
        select distinct
            concat(cast(organo as text), cast(orgcodigo as text)) as id,
            organo as ano,
            right('00' || cast(orgcodigo as text), 2) as organograma,
            1 as nivel,
            left(orgdescricao, 60) as descricao,
            null as sigla
        from wun.tborgao
       where orgcodigo < 9
        -- Nível 2 - Unidades
        union
        select distinct
            concat(cast(unidade.organo as text), cast(unidade.undcodigo as text)) as id,
            unidade.organo as ano,
            ( select right('00' || cast(orgcodigo as text), 2)
                from wun.tborgao orgao
                where orgao.orgcodigo = unidade.orgcodigo
                and orgao.organo = unidade.organo) || right('000' || cast(unidade.undcodigo as text), 3) as organograma,
            2 as nivel,
            left(unidade.unddescricao, 60) as descricao,
            null as sigla
        from wun.tbunidade unidade
       where orgcodigo < 9
        -- Nível 3 - Centro de Cursos
        union
        select distinct
            concat(cast(organo as text), cncclassif) as id,
            organo as ano,
            regexp_replace(cncclassif, '[\.]', '', 'gi') as organograma,
            ((SELECT COUNT(*) FROM regexp_matches(cncclassif, '[.]', 'g')) + 1) as nivel,
            left(cncdescricao, 60) as descricao,
            null as sigla
        from wun.tbcencus
       where orgcodigo < 9
       ) tab
 WHERE nivel::int >= 1
   AND LENGTH(organograma) <= 15
   AND ano = 2020
   AND (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','organograma', (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','configuracao-organograma', 2))), left(((case when nivel = '1' then '' else ano::varchar end) || organograma || '000000000000000'),15)))) is null
order by
  ano asc,
  nivel asc