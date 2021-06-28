SELECT  ROW_NUMBER() OVER()::varchar as key1,
		0 as id,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','organogramaConfiguracao','2'))) as organogramaconfig,
   		nivel,
   		descricao,
   		digitos,
   		separador
  FROM (select  1 as nivel,
				'Órgão' as descricao,
				2 as digitos,
				null as separador
		union
		select  2 as nivel,
				'Unidade' as descricao,
				3 as digitos,
				'PONTO' as separador
		union
		select  3 as nivel,
		        'Centro de Custo' as descricao,
		        3 as digitos,
		        'PONTO' as separador
		union
		 select 4 as nivel,
				'Centro de Custo Pai' as descricao,
				3 as digitos,
				'PONTO' as separador
		union
		 select 5 as nivel,
		        'Secretaria' as descricao,
		 		3 as digitos,
		    	'PONTO' as separador)
	 as Tab1
   order by nivel
