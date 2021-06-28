SELECT t.pronumero as key1,
       t.procodigo as id,
	   t.procodigo as id_codigo,
	   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
	   t.proano  as ano,
	   t2.doccodigo as id_doc,
	   (select tdoc.docdescricao from wpt.tbdocumento tdoc where tdoc.doccodigo = t2.doccodigo) as descr_doc,
	   'true' as documento_adicionado,
	   'AMBAS' as tipo_pessoa,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','documento',upper((select tdoc.docdescricao from wpt.tbdocumento tdoc where tdoc.doccodigo = t2.doccodigo))))) as id_docto_cloud,
	   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud,
       (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)),
         (select id_gerado from public.controle_migracao_auxiliar
           where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) )))
           --20690
           as id_assunto_cloud
  FROM wpt.tbprocesso t join wpt.tbdocpro t2 on t.procodigo = t2.procodigo
  						join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
  						--join wpt.tbrequerente t4 on t4.procodigo = t.procodigo and t4.traprincipal = 1
 WHERE proano = ano_extracao
   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','documento',upper((select tdoc.docdescricao from wpt.tbdocumento tdoc where tdoc.doccodigo = t2.doccodigo))))) is not null
   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
   and public.bth_get_situacao_registro('304', 'entrega-documentos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar), cast(t2.doccodigo as varchar)) in (0)

