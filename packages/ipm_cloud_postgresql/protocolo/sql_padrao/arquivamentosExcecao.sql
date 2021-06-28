select tabAux.key1,
       tabAux.id,
       tabAux.id_codigo,
       tabAux.numero_processo,
       tabAux.ano,
       tabAux.id_sequencia,
       tabAux.processo_cloud,
       tabAux.id_usuario,
       tabAux.nome_usuario,
       tabAux.tipo_movimentacao,
       tabAux.id_organograma_cloud,
       tabAux.data_hora_movimentacao,
	   case tabAux.id_usuario
            when 'UsuarioMigracao' then tabAux.observacao ||'    Usuário: '||tabAux.nome_usuario
            else tabAux.observacao
	   end as observacao_aux,
       tabAux.arquivamento_localizacao
  from (
		SELECT t.pronumero as key1,
			   t.procodigo as id,
			   t.procodigo as id_codigo,
			   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
			   t.proano  as ano,
			   t4.hissequencia as id_sequencia,
		       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as processo_cloud,
			   coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 																						where t8.usucodigo = t2.usucodigo)) ) ), 'UsuarioMigracao') as id_usuario,
			   (select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 where t8.usucodigo = t2.usucodigo) as nome_usuario,
			   md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
				 						where t8.usucodigo = t2.usucodigo))) as hash_usuario,
			   'ARQUIVAMENTO_PROCESSO' as tipo_movimentacao,
			   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) as id_organograma_cloud,
			   to_char(t2.hisdatahora::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as data_hora_movimentacao,
			   t2.hisobservacao as observacao,
			   left((select locdescricao from wpt.tblocal tl where tl.loccodigo = t4.loccodigo),20) as arquivamento_localizacao

		  FROM wpt.tbprocesso t join wpt.tbhistoricoprocesso t2 on t.procodigo = t2.procodigo and t.hissequencia = t2.hissequencia
		  						join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
		  						join wpt.tbarquivamento t4 on t.procodigo = t4.procodigo and t4.hissequencia != t.hissequencia
		  						join wpt.tbrequerente t5 on t5.procodigo = t.procodigo and t5.traprincipal = 1
		 WHERE proano = ano_extracao
		   and t.histipo = 7
		   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
		   AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
		   and exists ( select * from public.bth_get_situacao_registro('304', 'arquivamentos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar), cast(t4.hissequencia as varchar)))
	) as tabAux
