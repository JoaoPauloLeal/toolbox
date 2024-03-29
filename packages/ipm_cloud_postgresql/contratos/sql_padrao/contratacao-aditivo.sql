select
	row_number() over() as id,
	'305' as sistema,
	'contratacao-aditivo' as tipo_registro,
	concat(nro_minuta, '/', ano_minuta) as minuta,
	concat(nro_superior, '/', ano_contrato) as contratacao,
	concat(nro_aditivo, '/', ano_aditivo, ' (', identificador_aditivo, ')') as aditivo,
	*
from (
	select
		c.clicodigo,
		c.ctranosup as ano_contrato,
		c.ctridentsup as nro_superior,
		c.ctrdataassinatura::varchar as data_assinatura,
		c.ctrdatainivig::varchar as data_aditivo,
		(case when c.ctrtipoaditivo in (3, 5, 7, 8, 14) then c.ctrdatafimvig::varchar else null end) data_final_nova,
		c.adjsequencia,
		--coalesce((select trunc(sum(auxci.itcvlrtotal), 2) from wco.tbitemcompra auxci where md5(concat(auxci.clicodigo , '@', auxci.copano, '@', auxci.copnro)) in (select md5(concat(auxc.clicodigo , '@', copano, '@', copnro)) as hash from wco.tbcompra auxc where auxc.clicodigo = c.clicodigo and auxc.clicodigo = auxc.clicodigoctr and auxc.ctrano = c.ctrano and auxc.ctridentificador = c.ctridentificador)), 0) as valor_aditivo,
		0.00 as valor_aditivo,
		c.ctrano as ano_aditivo,
		c.ctrnro as nro_aditivo,
		c.ctridentificador as identificador_aditivo,
		c.minano as ano_minuta,
		c.minnro as nro_minuta,
		concat(c.ctrobjeto, ' (Migração: Aditivo ', c.ctrnro, '/', c.ctrano, ', Identificador ', c.ctridentificador, ', Minuta ', c.minnro, '/', c.minano, ')') as objeto,
		c.ctrtipoaditivo,
		(case c.ctrtipoaditivo when 1 then 'Normal' when 2 then 'Objeto' when 3 then 'Prazo' when 4 then 'Valor (Equilibrio)' when 5 then 'Prazo Valor'when 6 then 'Objeto Valor' when 7 then 'Objeto Prazo' when 8 then 'Objeto Prazo Valor' when 9 then 'Outros Aditivos' when 10 then 'Recisão Contratual' when 11 then 'Rerratificação' when 12 then 'Apostila' when 13 then '???' when 14 then 'Prorrogação' else 'Cessão' end) as desc_tipo_aditivo,
		false as continua,
		false as reforma,
		(select id_gerado from public.controle_migracao_registro_compras where hash_chave_dsk = md5(concat(305, 'contratacao', c.clicodigo, c.ctranosup, c.ctridentsup))) as id_contratacao,
		(select id_gerado from public.controle_migracao_registro_compras where hash_chave_dsk = md5(concat(305,'tipo-aditivo', c.ctrtipoaditivo))) as id_tipo_aditivo,
		(select id_gerado from public.controle_migracao_registro_compras where hash_chave_dsk = md5(concat(305, 'contratacao-aditivo', c.clicodigo, c.ctrano, c.ctrnro))) as id_gerado
	from wco.tbcontrato c
	where c.clicodigo = {{clicodigo}}
	and c.minano = {{ano}}
	--and c.minnro in (144)
	and c.ctrtipoaditivo is not null
	and c.ctrtipoaditivo <> 12
	order by 1, 2 desc, 3 desc, 4 asc
) tab
where id_gerado is null
and id_contratacao is not null
and id_tipo_aditivo is not null
--{{id_contratacao}}
