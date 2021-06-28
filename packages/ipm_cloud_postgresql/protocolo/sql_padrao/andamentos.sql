SELECT tbA.key1,
		tbA.id,
		tbA.id_codigo,
		tbA.id_seq,
		tbA.numero_processo,
		tbA.ano,
		tbA.data_andamento,
		tbA.id_usuario_origem,
		tbA.id_usuario_destino,
		case tbA.id_usuario_destino
            when 'UsuarioMigracao' then tbA.observacao ||'.    Usuário destino: '||tbA.nome_usuario_destino
            else tbA.observacao
	    end as observacao,
	    tbA.id_processo_cloud,
	    case tbA.id_seq
	     when 1 then
			coalesce(tbA.id_organograma_cloud_orig, tbA.id_organograma_abertura)
		 else
			coalesce(tbA.id_organograma_movto_ant, tbA.id_organograma_cloud_orig, tbA.id_organograma_atual)
		 end as id_organograma_orig_aux,
		case tbA.id_seq
		 when 1 then
			coalesce(tbA.id_organograma_cloud_dest, tbA.id_organograma_abertura)
		 else
			coalesce(tbA.id_organograma_movto_ant, tbA.id_organograma_cloud_dest, tbA.id_organograma_atual)
		 end as id_organograma_destino_aux,
--	    tbA.id_organograma_cloud_orig,
--	    tbA.id_organograma_cloud_dest,
	    tbA.situacao_execucao_script_decisao,
       tbA.situacao_andamento,
       tbA.situacao_decisao_workflow,
       tbA.possui_lote,
       tbA.transferencia,
       tbA.automatico
  FROM (
        SELECT t.pronumero as key1,
               t.procodigo as id,
               t.procodigo as id_codigo,
               t2.hissequencia as id_seq,
               right('000000000' || cast(t.pronumero as text ), 9) || '/' || cast(t.proano as text) as numero_processo,
               t.proano  as ano,
                to_char (t2.hisdatahora::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as data_andamento,
			   case t2.hissequencia
			   	when 1 then
					coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',
								(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo where t8.usucodigo = t2.usucodigo)) ) ), 'UsuarioMigracao')
			    else
				  coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',
				  			(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
		                      where t8.usucodigo = (select t6.usucodigo from wpt.tbhistoricoprocesso t6 where t6.procodigo = t2.procodigo and t6.hissequencia = (t2.hissequencia - 1)))) ) ), 'UsuarioMigracao')
				end as id_usuario_origem,
		       coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
		                                                                                             where t8.usucodigo = t2.usucodigo)) ) ), 'UsuarioMigracao') as id_usuario_destino,
			   case t2.hissequencia
			   	when 1 then
			   		(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 where t8.usucodigo = t2.usucodigo)
			    else
			      (select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				    where t8.usucodigo = (select t6.usucodigo from wpt.tbhistoricoprocesso t6 where t6.procodigo = t2.procodigo and t6.hissequencia = (t2.hissequencia - 1)))
				end as nome_usuario_origem,
		       (select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 where t8.usucodigo = t2.usucodigo) as nome_usuario_destino,
               t2.hisobservacao as observacao,
               (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoabert),t3.cncclassifabert))) as id_organograma_abertura,
               (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) as id_organograma_atual,
            coalesce( (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper((select tcc.cncdescricao from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia))),(select tcc.cncclassif from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia))))),
                      (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper((select tcc.cncdescricao from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia - 1))),(select tcc.cncclassif from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia - 1))))) ) as id_organograma_movto_ant,
               (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud,
               cast((select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper((select tcc.cncdescricao from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncorigem from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia ))),(select tcc.cncclassif from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncorigem from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia ))))) as varchar) as id_organograma_cloud_orig,
               cast((select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper((select tcc.cncdescricao from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia ))),(select tcc.cncclassif from wun.tbcencus tcc where tcc.cnccodigo = (select tm.cncdestino from wpt.tbmovimento tm
                                                                                      where tm.procodigo = t2.procodigo and tm.hissequencia = t2.hissequencia ))))) as varchar) as id_organograma_cloud_dest,
               'SUCESSO' as situacao_execucao_script_decisao,
               'CONFIRMADO' as situacao_andamento,
               'NAO_POSSUI' as situacao_decisao_workflow,
               'true' as possui_lote,
               'true' as transferencia,
               'true' as automatico

          FROM wpt.tbprocesso t join wpt.tbhistoricoprocesso t2 on t.procodigo = t2.procodigo
                                join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
         WHERE proano = 2021
           --and t.procodigo = 166335
           and pronumero in(1,2,3,4,5,6,7,8,9)
           and t2.histipo not in(6,7,8)
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','classificacao',(select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo)))) ))) is not null
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
           and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
        --   and cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) > 0
           and public.bth_get_situacao_registro('304', 'andamentos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar), cast(t2.hissequencia as varchar)) in (0)
    ) as tbA
