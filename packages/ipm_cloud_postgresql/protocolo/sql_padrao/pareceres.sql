select TbParecer.key1,
	   TbParecer.id,
	   TbParecer.id_codigo,
	   TbParecer.ano,
	   TbParecer.numero_processo,
	   TbParecer.codigo_parecer,
	   TbParecer.id_processo_cloud,
	   TbParecer.usuario,
	   TbParecer.encerrar_processo,
	   TbParecer.resultado_encerramento,
	   TbParecer.sigiloso_externamente,
	   TbParecer.resultado_encerramento as parecer_restrito,
	   TbParecer.id_organograma_cloud,
	   TbParecer.emissao,
   	   TbParecer.parecer_restrito as parecer_restrito_aux,
	   case TbParecer.usuario
            when 'UsuarioMigracao' then TbParecer.observacao ||'.    Usuário parecer: '||TbParecer.nome_usuario
            else TbParecer.observacao
	   end as observacao_aux,
	   TbParecer.encerramento_processo,
	   TbParecer.usuario as usuario_encerramento_processo
  from (
		SELECT t.pronumero as key1,
			   t.procodigo as id,
			   t.procodigo as id_codigo,
			   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
			   t.proano  as ano,
			   row_number() over(partition by t.procodigo) as codigo_parecer,
		       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud,
		       (select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 where t8.usucodigo = t2.usucodigo) as nome_usuario,
			   coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
						 																						where t8.usucodigo = t2.usucodigo)) ) ), 'UsuarioMigracao') as usuario,
			   'true' as encerrar_processo,
			   case t2.parcodigo
			    when 1 then 'INDEFERIDO'
			    when 2 then 'DEFERIDO'
			    when 3 then 'PARCIAL'
			    when 4 then 'CONFIRMADO'
			    when 5 then 'ANULADO'
			    when 6 then 'OUTROS'
			    else 'OUTROS'
			   end as resultado_encerramento,
		--	   'DEFERIDO' as resultado_encerramento,
		--	   'DEFERIDO' as parecer_registro,
			   'false' as sigiloso_externamente,
			   'false' as parecer_restrito,
			   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) as id_organograma_cloud,
			   to_char(t3.hisdatahoraultimotram::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as emissao,
			   left(regexp_replace(t3.hisobservacaoenc,'[\n\r]+',' - ','g'),440) as observacao,
			   to_char(t3.hisdatahoraenc::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as encerramento_processo

		  FROM wpt.tbprocesso t join wpt.tbhistoricoprocesso t2 on t.procodigo = t2.procodigo and t.hissequencia = t2.hissequencia
		  						join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
		  						--join wpt.tbrequerente t4 on t4.procodigo = t.procodigo and t4.traprincipal = 1
		 WHERE proano = ano_extracao
		   and t.histipo in (6,7)
		   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
		   and public.bth_get_situacao_registro('304', 'pareceres', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
		) as TbParecer
