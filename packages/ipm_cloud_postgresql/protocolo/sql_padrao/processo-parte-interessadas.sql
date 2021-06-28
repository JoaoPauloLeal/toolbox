select distinct tbPartes.key1,
		tbPartes.id,
		tbPartes.id_codigo,
		tbPartes.numero_processo,
		tbpartes.partes_interessadas,
		tbPartes.id_pessoa,
		tbPartes.id_cloud_pessoa,
		tbPartes.ano,
		tbPartes.id_processo_cloud
  from (
		SELECT t.pronumero as key1,
			   t.procodigo as id,
			   t.procodigo as id_codigo,
			   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
			   cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) as id_cloud_pessoa,
			   t.proano  as ano,
			   (select uninomerazao from wun.tbunico t5 where t5.unicodigo = t2.unicodigo) as partes_interessadas,
			   t2.traprincipal as principal,
			   t2.unicodigo  as id_pessoa,
		       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud
		  FROM wpt.tbprocesso t join wpt.tbrequerente t2 on t.procodigo = t2.procodigo and t2.traprincipal = 0
		 WHERE proano = ano_extracao
		   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
		   and cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) > 0
--		   and public.bth_get_situacao_registro('304', 'processo-parte-interessadas', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
		   and not exists (select 1 from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processo-parte-interessadas',t.pronumero,t.procodigo,t.proano,t2.unicodigo)))
		union all

		SELECT t.pronumero as key1,
			   t.procodigo as id,
			   t.procodigo as id_codigo,
			   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
			   cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t3.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t3.unicodigo ))), 0) as integer) as id_cloud_pessoa,
			   t.proano  as ano,
			   (select uninomerazao from wun.tbunico t5 where t5.unicodigo = t3.unicodigo ) as partes_interessadas,
			   null as principal,
			   t3.unicodigo as id_pessoa,
		       (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) as id_processo_cloud
		  FROM wpt.tbprocesso t join wpt.tbprocessomensagem t2 on t.procodigo = t2.procodigo and t2.pmgsequencia = 1
		    					join webbased.tbmensagem t3 on t2.msgcodigo = t3.msgcodigo
		 WHERE proano = ano_extracao
		   and t3.unicodigo not in (select t4.unicodigo from wpt.tbrequerente t4 where t4.procodigo = t3.unicodigo)
		   and (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processos',t.pronumero,t.procodigo,t.proano))) is not null
		   and cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t3.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t3.unicodigo ))), 0) as integer) > 0
--		   and public.bth_get_situacao_registro('304', 'processo-parte-interessadas', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
		   and not exists (select 1 from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','processo-parte-interessadas',t.pronumero,t.procodigo,t.proano,t3.unicodigo)))
	) as tbPartes