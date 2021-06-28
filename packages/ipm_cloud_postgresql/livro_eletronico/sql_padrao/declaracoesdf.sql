select ROW_NUMBER() OVER() as key1,
	   wn.codtom as id,
	   wn.numero as df_inicial,
       1 as df_final,
		ROW_NUMBER() OVER() as i_documentos,
		wn.cnpjpre,
		to_char(wn.dtemis::date, 'YYYY-MM-DD') as dt_emissao,
		case wn.tpdoc
          when 1 /*Nota Fiscal de Serviços*/ then 'N'
		  when 2 /*Recibo de Serviços*/ then 'R'
		  when 3 /*Nota Fiscal Conjugada*/ then 'J'
		  when 4 /*Documento Fiscal Estrangeiro*/ then 'O'
		  when 5 /*Cupom Fiscal Conjugado*/ then 'C'
		  when 6 /*Estimativa / Arbitramento*/ then 'O'
		  when 7 /*Nota Fiscal de Serviços Eletrônica*/ then 'N'
		  when 8 /*(Contingência)Nota Fiscal de Serviços Eletrônica*/ then 'N'
		  else 'O'
		end as tipo,
        wn2.codsit,
        case wn2.codsit
            when  0 /*Tributada Integralmente*/ then 'N'
            when  1 /*Tributada Integralmente com ISSRF*/ then 'R'
            when  2 /*Tributada Integralmente e sujeita à Substituição Tributária*/ then 'S'
            when  3 /*Tributada com redução da base de cálculo*/ then 'N'
            when  4 /*Tributada com redução da base de cálculo com ISSRF*/ then 'R'
            when  5 /*Tributada com redução da base de cálculo e sujeita à Substituição Tributária*/ then 'S'
            when  6 /*Isenta*/ then 'C'
            when  7 /*Imune*/ then 'C'
            when  8 /*Não Tributada - ISS regime Fixo*/ then 'N'
            when  9 /*Não Tributada - ISS regime Estimativa*/ then 'N'
            when 10 /*Não Tributada - ISS Construção Civil recolhido antecipadamente*/ then 'N'
            when 11 /*Não Tributada - ISS recolhido por Nota Avulsa*/ then 'N'
            when 12 /*Não Tributada - Prestador estabelecido no Município*/ then 'N'
            when 13 /*Não Tributada - Recolhimento efetuado pelo prestador de fora do Município*/ then 'N'
            when 14 /*Não Tributada*/ then 'N'
            when 15 /*Não Tributada - Ato Cooperado*/ then 'N'
        end as status,
        case wn2.codsit
            when  0 /*Tributada Integralmente*/ then 'N'
            when  1 /*Tributada Integralmente com ISSRF*/ then 'R'
            when  2 /*Tributada Integralmente e sujeita à Substituição Tributária*/ then 'S'
            when  3 /*Tributada com redução da base de cálculo*/ then 'N'
            when  4 /*Tributada com redução da base de cálculo com ISSRF*/ then 'R'
            when  5 /*Tributada com redução da base de cálculo e sujeita à Substituição Tributária*/ then 'S'
            when  6 /*Isenta*/ then 'C'
            when  7 /*Imune*/ then 'C'
            when  8 /*Não Tributada - ISS regime Fixo*/ then 'N'
            when  9 /*Não Tributada - ISS regime Estimativa*/ then 'N'
            when 10 /*Não Tributada - ISS Construção Civil recolhido antecipadamente*/ then 'N'
            when 11 /*Não Tributada - ISS recolhido por Nota Avulsa*/ then 'N'
            when 12 /*Não Tributada - Prestador estabelecido no Município*/ then 'N'
            when 13 /*Não Tributada - Recolhimento efetuado pelo prestador de fora do Município*/ then 'N'
            when 14 /*Não Tributada*/ then 'N'
            when 15 /*Não Tributada - Ato Cooperado*/ then 'N'
        end as situacao_tributaria,
        wn2.tributanomunicippre as natureza_operacao,
        'N' as descontado_prefeitura,
		wn.vlrtot as vl_documento,
		wn.vlrtot as vl_servico,
		wn2.bascalc as vl_base_calculo,
		((wn2.bascalc * wn2.aliquota) / 100) as vl_imposto,
		wn2.deducao as vl_deducao,
		wn.simples as optante_sn,
		'N' as origem,
		(select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoes', wn.codtom, e.codigo))),
		substring((select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoes', wn.codtom, e.codigo))) from '[0-9]{1,10}') as i_declaracoes,
		(select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) as i_contribuintes,
--		(select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','tomadoresprestadores', wn.codtom, wn.cnpjpre))),
		substring((select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','tomadoresprestadores', wn.codtom, wn.cnpjpre))) from '[0-9]{1,10}') as i_declarados
	--   	'4979' as i_declarados
--		 substring(telefone from '^[0-9]{4}')
--		regexp_replace(i_declaracoes, '[\.]', '', 'gi')

   from "8045_isarq".wm_nfst wn join "8045_isarq".economico e on e.cadastro = wn.codtom
   								join "8045_isarq".wm_nfstit wn2 on wn.codtom = wn2.codtom
		  																and wn.cnpjpre = wn2.cnpjpre
		  																and wn.compet = wn2.compet
		  																and wn.numero = wn2.numero
  where wn.compet = '12/2020'
   and (select id_gerado from controle_migracao_registro cmr  where hash_chave_dsk =  md5(concat('999','declaracoes', wn.codtom, e.codigo))) is not null
--   and wn.tpdoc not in(1,7,8)
--limit 1