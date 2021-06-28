select tbProcess.key1,
		tbProcess.id,
		tbProcess.id_codigo,
		tbProcess.numero_processo,
		tbProcess.ano,
		tbProcess.id_classificacao,
		tbProcess.id_protocolo_cloud,
		tbProcess.id_classif_cloud,
		tbProcess.id_assunto_cloud,
		tbProcess.id_cloud_pessoa,
		tbProcess.tipo_identificacao,
		tbProcess.possui_data_prevista,
		tbProcess.previsto_para,
		tbProcess.situacao,
		tbProcess.procedencia,
		tbProcess.prioridade,
		tbProcess.tipo_processo,
		cast(tbProcess.observacao||' :: '||tbProcess.requerente_cidadao as varchar(480)) as observacao
  from (
	SELECT t.pronumero as key1,
		   t.procodigo as id,
		   t.procodigo as id_codigo,
		   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
		   t.proano as ano,
		   t.asscodigo as id_classificacao,
		   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','protocolos',t.pronumero, t.procodigo, t.proano))) as id_protocolo_cloud,
		   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) as id_classif_cloud,
		   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) as id_assunto_cloud,
		   t.subcodigo as id_assunto,
		   t2.unicodigo as id_pessoa,
		   cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) as id_cloud_pessoa,
		   'QUERO_IDENTIFICAR' as tipo_identificacao,
		   true as possui_data_prevista,
		   to_char(t.prodataprevisao::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as previsto_para,
		   case t.histipo
	            when 1 then 'EM_ABERTURA'
	            when 2 then 'EM_ANALISE'
	            when 3 then 'EM_ANALISE'
	            when 5 then 'CANCELADO'
	            when 6 then 'ENCERRADO'
	            when 7 then 'ARQUIVADO'
	            when 12 then 'PARADO'
	            when 18 then 'AGUARDANDO_SOLICITANTE'
	            else 'NAO_ANALISADO'
	       end as situacao,
	       case t.proorigem
	       	 	when 1 then 'INTERNA'
	       	 	else 'EXTERNA'
	       end as procedencia,
	       'ALTA' as prioridade,
	       case t.profinalidade
	       		when 2 then 'DIGITAL'
	       		else 'FISICO'
	       end as tipo_processo,
	       t3.hisobservacaoabert as observacao,
	       null as con_nome,
	       null as con_email,
	       null as con_telefone,
	       null as con_celular,
	       null as con_estado,
	       null as con_cidade,
	       null as con_bairro,
	       null as con_endereco,
	       null as con_numero,
	       null as con_observacao,
	       null as con_cpf_cnpj,
	       null as requerente_cidadao

	  FROM wpt.tbprocesso t join wpt.tbrequerente t2 on t.procodigo = t2.procodigo and t2.traprincipal = 1
	  						join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
	 WHERE proano = ano_extracao
	   AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) is not null
	   AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
	   and cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) > 0
	   and public.bth_get_situacao_registro('304', 'processos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)

	union all

	SELECT t.pronumero as key1,
		   t.procodigo as id,
		   t.procodigo as id_codigo,
		   right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
		   t.proano as ano,
		   t.asscodigo as id_classificacao,
		   (select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('304','protocolos',t.pronumero, t.procodigo, t.proano))) as id_protocolo_cloud,
		   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) as id_classif_cloud,
		   (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) as id_assunto_cloud,
		   t.subcodigo as id_assunto,
		   null as id_pessoa,
		   null as id_cloud_pessoa,
		   'ANONIMO' as tipo_identificacao,
		   true as possui_data_prevista,
		   to_char(t.prodataprevisao::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as previsto_para,
		   case t.histipo
	            when 1 then 'EM_ABERTURA'
	            when 2 then 'EM_ANALISE'
	            when 3 then 'EM_ANALISE'
	            when 5 then 'CANCELADO'
	            when 6 then 'ENCERRADO'
	            when 7 then 'ARQUIVADO'
	            when 12 then 'PARADO'
	            when 18 then 'AGUARDANDO_SOLICITANTE'
	            else 'NAO_ANALISADO'
	       end as situacao,
	       case t.proorigem
	       	 	when 1 then 'INTERNA'
	       	 	else 'EXTERNA'
	       end as procedencia,
	       'ALTA' as prioridade,
	       case t.profinalidade
	       		when 2 then 'DIGITAL'
	       		else 'FISICO'
	       end as tipo_processo,
	       t3.hisobservacaoabert as observacao,
	       t2.connome as con_nome,
	       t2.conemail as con_email,
	       t2.contelefone as con_telefone,
	       t2.concelular as con_celular,
	       (select estsigla from wun.tbestado t3 where t3.estcodigo = t2.estcodigo ) as con_estado,
	       (select cidnome from wun.tbcidade t6 where t6.cidcodigo = t2.cidcodigo) as con_cidade,
	       t2.conbairro as con_bairro,
	       t2.conendereco as con_endereco,
	       t2.connumero as con_numero,
	       t2.conobservacao as con_observacao,
	       t2.concpfcnpj as con_cpf_cnpj,
	       ' Dados do Requerente( '||'Nome: '||left(coalesce(t2.connome,''),60)||' - email: '||left(coalesce(t2.conemail,''),60)||' - fone: '||left(coalesce(t2.contelefone::varchar,''),20)||' - cel.: '
	       ||left(coalesce(t2.concelular::varchar,''),20)||' - cidade: '||left(coalesce((select cidnome from wun.tbcidade t6 where t6.cidcodigo = t2.cidcodigo),''),50)||'-'||
	       coalesce((select estsigla from wun.tbestado t3 where t3.estcodigo = t2.estcodigo),'')||' - bairro: '||left(coalesce(t2.conbairro,''),60)||' - endereco: '||left(coalesce(t2.conendereco,''),60)||' - Nยบ '||
	       left(coalesce(t2.connumero::varchar,''),12)||' - cpf/cnpj: '||left(coalesce(t2.concpfcnpj::varchar,''),20)||' )' as requerente_portal

	  FROM wpt.tbprocesso t join wpt.tbcontatoprocesso t2 on t.procodigo = t2.procodigo
	  						join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
	 WHERE proano = ano_extracao
	   AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) is not null
	   AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
	   and public.bth_get_situacao_registro('304', 'processos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
  ) as tbProcess