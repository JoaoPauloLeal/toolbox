SELECT t.pronumero as key1,
       t.procodigo as id,
	   t.procodigo as id_codigo,
	   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
	   t.proano  as ano,
	   t2.doccodigo as id_doc,
	   --(select tdoc.docdescricao from wpt.tbdocumento tdoc where tdoc.doccodigo = t2.doccodigo) as nome_doc,
	   --'null' as nome_doc,
	  -- 'null' as assinar,
	  -- 'null' as tipo_pessoa,
	   'Z:\'||t3.arqid as arq_file,
	   (select t4.arqnomeoriginal from arquivo.tbwpt t4 where t4.arqid = t3.arqid ) as nome_arq,
	   (select t4.arqmime from arquivo.tbwpt t4 where t4.arqid = t3.arqid ) as content_type,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','entrega-documentos',t.pronumero,t.procodigo,t.proano,t2.doccodigo))) as id_processo_docto_cloud
  FROM wpt.tbprocesso t join wpt.tbdocpro t2 on t.procodigo = t2.procodigo
		  				left outer join wpt.tbanexo t3 on t2.procodigo = t3.procodigo
		  												and t3.doccodigo = t2.doccodigo
 WHERE proano = ano_extracao
   and t3.arqid is not null
   and t3.arqid not in('69FCC015DE40782BF47DD64C2E0F2E2FE972188B',
			'ECACB2EE1C36E8CF2F6C615C8B7E51C6772F064E',
			'211A869376A9B1A02FB9EE8728919C330806BCD7',
			'789ABBDCFC620E629EAAF9B54CE988D716D23881',
			'44D28D67A1F7C88450418263D040B8EAC988FEED')
   --and t.prodatahoraulttramordena <= '2020-10-30'::date
   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','entrega-documentos',t.pronumero,t.procodigo,t.proano,t2.doccodigo))) is not null
   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
   and not exists (select 1 from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos-anexo',t.pronumero,t.procodigo,t.proano,t2.doccodigo)))
