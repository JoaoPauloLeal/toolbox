select TbAnexParecer.key1,
	   TbAnexParecer.id,
	   TbAnexParecer.id_codigo,
	   TbAnexParecer.ano,
	   TbAnexParecer.codigo_parecer,
	   TbAnexParecer.numero_processo,
	   TbAnexParecer.id_parecer_cloud,
	   TbAnexParecer.usuario,
	   TbAnexParecer.arq_file,
	   TbAnexParecer.nome_arq,
   	   TbAnexParecer.content_type
  from (
		SELECT t.pronumero as key1,
			   t.procodigo as id,
			   t.procodigo as id_codigo,
			   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
			   t.proano  as ano,
			   row_number() over(partition by t.procodigo) as codigo_parecer,
		       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','pareceres',t.pronumero,t.procodigo,t.proano))) as id_parecer_cloud,
		       (select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 where t8.usucodigo = t2.usucodigo) as nome_usuario,
			   coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
						 																						where t8.usucodigo = t2.usucodigo)) ) ), 'UsuarioMigracao') as usuario,

			   'Z:\'||t3.arqid as arq_file,
			   (select t4.arqnomeoriginal from arquivo.tbwpt t4 where t4.arqid = t3.arqid ) as nome_arq,
			   (select t4.arqmime from arquivo.tbwpt t4 where t4.arqid = t3.arqid ) as content_type

		  FROM wpt.tbprocesso t join wpt.tbhistoricoprocesso t2 on t.procodigo = t2.procodigo and t.hissequencia = t2.hissequencia
		  						join wpt.tbanexo t3 on t.procodigo = t3.procodigo
		 WHERE proano = ano_extracao
		   and t.histipo in(6,7)
		   --and t.pronumero = 6271
		   and t3.doccodigo is null
		   and t3.arqid is not null
		   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','pareceres',t.pronumero,t.procodigo,t.proano))) is not null
--		   and public.bth_get_situacao_registro('304', 'parecer-anexos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
		   --and t.prodatahoraulttramordena <= '2020-07-31'::date
		) as TbAnexParecer
		   where not exists (select 1 from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','parecer-anexos',TbAnexParecer.key1,TbAnexParecer.id_codigo,TbAnexParecer.ano,TbAnexParecer.codigo_parecer)))
