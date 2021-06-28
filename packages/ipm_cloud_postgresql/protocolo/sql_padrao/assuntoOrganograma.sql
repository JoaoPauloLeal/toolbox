SELECT ROW_NUMBER() OVER()::varchar as id,
	   ROW_NUMBER() OVER()::varchar as key1,
        (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','assunto',cma.i_chave_dsk2, cma.i_chave_dsk3))) as id_assunto_cloud,
	   cma.i_chave_dsk2 as descricao_assunto,
	   (select min(id_gerado) from public.controle_migracao_registro where i_chave_dsk1 = cma.i_chave_dsk5) as id_organograma_cloud,
--	   hash_chave_dsk = md5(concat('304','organogramas',cma.i_chave_dsk5, cma.i_chave_dsk6))) as idOrganogramaCloud,
	   cma.i_chave_dsk4,
	   cma.i_chave_dsk5 as descricao_organograma,
	   cma.i_chave_dsk6

  FROM controle_migracao_auxiliar cma
 WHERE (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','assunto',cma.i_chave_dsk2, cma.i_chave_dsk3))) is not null
   AND (select min(id_gerado) from public.controle_migracao_registro where i_chave_dsk1 = cma.i_chave_dsk5) is not null
