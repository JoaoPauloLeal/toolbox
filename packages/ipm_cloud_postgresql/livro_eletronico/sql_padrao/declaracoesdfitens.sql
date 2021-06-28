select  wnItem.id_documento as id,
        wnItem.i_declaracoes as i_declaracoes,
	   substring(substring((select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoesdf', wnItem.i_declaracoes, wnItem.id_documento))) from 25 for 23 ) from '[0-9]{1,10}')  as i_documentos,
--	   (select id_gerado from controle_migracao_registro cmr2 where hash_chave_dsk = wnItem.hash_lista) as lista,
	   substring((select id_gerado from controle_migracao_registro cmr2 where hash_chave_dsk = wnItem.hash_lista) from '[0-9]{1,10}') as i_listas_servicos,
	   wnItem.sequencias as i_sequencias,
	   'S' as servico_no_pais,
	   wnItem.nome_cidade as nome_municipios,
	   1 as qtd_servico,
	   wnItem.vl_unitario as vl_unitario,
		wnItem.vl_servico as vl_servico,
		wnItem.vl_base_calculo as vl_base_calculo,
		wnItem.aliquota as aliquota,
		wnItem.vl_iss as vl_iss
  from (
		select wn.codtom as id,
				(select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat(999,'declaracoes', wn.codtom, e.codigo))),
				e.codigo as cpf_cnpj_tomador,
				substring((select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoes', wn.codtom, e.codigo))) from '[0-9]{1,10}') as i_declaracoes,
				ROW_NUMBER() OVER() as id_documento,
				wn.cnpjpre,
		        wn2.codigo::character(128) as i_lista_servicos,
		        public.bth_get_hash_chave(300,'listasservicos',wn2.codigo::character(128), (select aliquota from "8045_isarq".listserv l where l.codigo = wn2.codigo)::char) as hash_lista,
		        (select aliquota from "8045_isarq".listserv l where l.codigo = wn2.codigo)::char as aliquota_lista_servico,
		        wn2.item as sequencias,
		        (select t.cidnome from public.tbcidade t where t.cidcodigoreceita = wn2.cidcodigo) as nome_cidade,
				wn2.bascalc as vl_unitario,
				wn2.bascalc as vl_servico,
				wn2.bascalc as vl_base_calculo,
				wn2.aliquota as aliquota,
				((wn2.bascalc * wn2.aliquota) / 100) as vl_iss

		   from "8045_isarq".wm_nfst wn join "8045_isarq".economico e on e.cadastro = wn.codtom
		   								join "8045_isarq".wm_nfstit wn2 on wn.codtom = wn2.codtom
				  																and wn.cnpjpre = wn2.cnpjpre
				  																and wn.compet = wn2.compet
				  																and wn.numero = wn2.numero
		  where wn.compet = '12/2020'
		   and (select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoes', wn.codtom, e.codigo))) is not null
	) as wnItem
   where (select id_gerado from controle_migracao_registro cmr2 where hash_chave_dsk = wnItem.hash_lista) is not null

--limit 1