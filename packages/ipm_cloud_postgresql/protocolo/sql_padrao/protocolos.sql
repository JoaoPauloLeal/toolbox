SELECT tbProt.key1,
       tbProt.id,
       tbProt.id_codigo,
       tbProt.numero_processo,
       tbProt.ano,
       tbProt.id_organograma,
       tbProt.descr_organograma,
       tbProt.mascara_formatada,
       tbProt.id_organograma_cloud,
       tbProt.protocolado_por,
       tbProt.tipo
  FROM (SELECT t.pronumero as key1,
               t.procodigo as id,
               t.procodigo as id_codigo,
               right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
               t.proano  as ano,
               t3.cnccodigoatual as id_organograma,
               t3.cncdescricaoatual as descr_organograma,
               t3.cncclassifatual as mascara_formatada,
               (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) as id_organograma_cloud,
               coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
                                                                                                     where t8.usucodigo = t3.usucodigoatual)) ) ), 'UsuarioMigracao') as protocolado_por,
               'ABERTURA_PROCESSOS' as tipo
          FROM wpt.tbprocesso t join wpt.tbrequerente t2 on t.procodigo = t2.procodigo and traprincipal = 1
                                join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
         WHERE proano = ano_extracao
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) is not null
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
           and cast(coalesce((select id_gerado from public.controle_migracao_registro cmr where hash_chave_dsk = md5(concat('304','pessoaFisica', t2.unicodigo))), (select id_gerado from public.controle_migracao_registro cmr2 where hash_chave_dsk = md5(concat('304', 'pessoaJuridica', t2.unicodigo ))), 0) as integer) > 0
           and public.bth_get_situacao_registro('304', 'protocolos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)

        union all

        SELECT t.pronumero as key1,
               t.procodigo as id,
               t.procodigo as id_codigo,
               right('000000000' || cast(t.pronumero as text ), 9) || '.' || cast(t.proano as text) as numero_processo,
               t.proano  as ano,
               t3.cnccodigoatual as id_organograma,
               t3.cncdescricaoatual as descr_organograma,
               t3.cncclassifatual as mascara_formatada,
               (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) as id_organograma_cloud,
               coalesce((select id_gerado from controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('304','usuarios',(select t7.uninomerazao from webbased.tbusuario t8 join wun.tbunico t7 on t7.unicodigo = t8.unicodigo
                                                                                                     where t8.usucodigo = t3.usucodigoatual)) ) ), 'UsuarioMigracao') as protocolado_por,
               'ABERTURA_PROCESSOS' as tipo
          FROM wpt.tbprocesso t join wpt.tbcontatoprocesso t2 on t.procodigo = t2.procodigo
                                join wpt.tbprocessodesnormalizada t3 on t.procodigo = t3.procodigo
         WHERE proano = ano_extracao
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','assunto', upper((select subdescricao from wpt.tbsubassunto t5 where t5.subcodigo = t.subcodigo)), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','classificacao',upper((select assdescricao from wpt.tbassunto t4 where t4.asscodigo = t.asscodigo))))) ))) is not null
           AND (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('304','organograma',upper(t3.cncdescricaoatual),t3.cncclassifatual))) is not null
           and public.bth_get_situacao_registro('304', 'protocolos', cast(t.pronumero as varchar), cast(t.procodigo as varchar), cast(t.proano as varchar)) in (0)
        ) as tbProt