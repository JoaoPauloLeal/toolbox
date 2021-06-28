--select
--       wn.codtom as key1,
--	   wn.codtom as id,
--	   e.codigo as cpf_cnpj_tomador,
--	   (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
--	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) as i_pessoas,
--       wn.compet as competencia,
--       '28' as i_competencias,
--       '2020' as i_anos,
--       case wn.tpdoc
--         when 7 then 'N'
--         else 'A'
--       end as situacao,
--       'T' as tipo,
--       'N' as simplificada,
--       'N' as situacao_guia,
--        --wn2.bascalc as vl_base_calculo,
--       '0.00' as vl_base_calculo,
--       '0.00' as vl_servico_simplificada,
--       '0.00' as vl_imposto_simplificada,
--       '0.00' as vl_servico,
--       '0.00' as vl_imposto,
--       '0.00' as vl_deducao,
--       '0.00' as vl_saldo,
--       '0.00' as vl_taxas,
--       '0.00' as vl_documento,
--       '0' as qtd_documentos,
--       wn.simples as optante_sn
--  from "8045_isarq".wm_nfst wn join "8045_isarq".economico e on wn.codtom = e.cadastro
----                                join "8045_isarq".wm_nfstit wn2 on wn.codtom = wn2.codtom
----  																and wn.cnpjpre = wn2.cnpjpre
----  																and wn.compet = wn2.compet
----  																and wn.numero = wn2.numero
-- where compet = '12/2020'
--   and (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
--	   				and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) is not null
--group by wn.tpdoc,wn.codtom,wn.compet, e.codigo, wn.simples




select wnx.id as key1,
	   wnx.id as id,
  	   e.codigo as cpf_cnpj_tomador,
--		(select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
--	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) as i_pessoas,
  	   wnx.competencia as competencia,
	   '28' as i_competencias,
       '2020' as i_anos,
  	   wnx.situacao as situacao,
  	   wnx.tipo as tipo,
  	   wnx.simplificada as simplificada,
  	   'N' as situacao_guia,
  	   count(*) as qtd_documentos,
  	   sum(wnx.vlrtot) as vl_documento,
  	   sum(wnx.vlrtot) as vl_servico,
  	   sum(wnx.bascalc) as vl_base_calculo,
  	   sum(wnx.vlr_imposto) as vl_imposto,

       '0.00' as vl_servico_simplificada,
       '0.00' as vl_imposto_simplificada,
       '0.00' as vl_servico,
       '0.00' as vl_imposto,
       '0.00' as vl_deducao,
       '0.00' as vl_saldo,
       '0.00' as vl_taxas,
       'N' as situacao_guia,
        wnx.optante_sn as optante_sn

  from (select wn.codtom as id,
		       wn.compet as competencia,
			   wn.cnpjpre as cpf_cnpj_prestador,
		       case wn.tpdoc
		         when 7 then 'N'
		         else 'A'
		       end as situacao,
		       'T' as tipo,
		       'N' as simplificada,
				wn.vlrtot,
				wn2.bascalc,
				wn2.aliquota,
				wn2.deducao,
				wn2.vlrret,
				((wn2.bascalc * wn2.aliquota) / 100) as vlr_imposto,
				wn.simples as optante_sn
		  from "8045_isarq".wm_nfst wn join "8045_isarq".wm_nfstit wn2 on wn.codtom = wn2.codtom
		  															  	and wn.cnpjpre = wn2.cnpjpre
		  																and wn.compet = wn2.compet
		  																and wn.numero = wn2.numero

		 where wn.compet = '12/2020') as wnx join "8045_isarq".economico e on e.cadastro = id
--		   and (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
--	   		and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) is not null

group by wnx.id, e.codigo, wnx.competencia, wnx.situacao, wnx.tipo, wnx.simplificada, wnx.optante_sn
