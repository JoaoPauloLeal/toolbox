SELECT bethadba.rec_loa.i_exercicios,
              bethadba.rec_loa.i_loas,
              bethadba.rec_loa.i_entidades,
              bethadba.rec_loa.i_rec_loa,
              bethadba.rec_loa.i_ldos,
              bethadba.rec_loa.i_rec_ldo,
              bethadba.rec_loa.i_planos_contas,
              bethadba.rec_loa.i_contas,
              --bethadba.contas.mascara,
              SUBSTRING( bethadba.contas.mascara,2,15),
              if has_contas_prop_exerc = 1 then contas_prop.descricao
              else 'Rubrica nï¿½o cadastrada para o exercï¿½cio corrente! (efetuar o cadastro para regularizar)' endif as contas_prop_descricao,
              bethadba.rec_loa.i_config_organ,
              bethadba.rec_loa.i_organogramas,
              bethadba.organogramas.descricao,
              (SELECT sum(rec_loa_val.valOR)
                 FROM bethadba.rec_loa_val
                 WHERE rec_loa_val.i_entidades = rec_loa.i_entidades AND
                       rec_loa_val.i_exercicios = rec_loa.i_exercicios AND
                       rec_loa_val.i_loas = rec_loa.i_loas AND
                       rec_loa_val.i_rec_loa = rec_loa.i_rec_loa) AS valor,
              protect_organ = IF (SELECT count(hist_rec_ldo.i_organogramas)
     		                       FROM bethadba.hist_rec_ldo
     							  WHERE hist_rec_ldo.i_exercicios = rec_loa.i_exercicios AND
     							        hist_rec_ldo.i_ldos = rec_loa.i_ldos AND
     									hist_rec_ldo.i_rec_ldo = rec_loa.i_rec_ldo AND
     									hist_rec_ldo.i_dt_alteracao = bethadba.dbf_GetHistRecLdo(hist_rec_ldo.i_exercicios, hist_rec_ldo.i_ldos,hist_rec_ldo.i_rec_ldo) ) > 0  THEN 1 ELSE 0 ENDIF,
              bethadba.rec_loa.aprovado_loa,

              bethadba.contas_prop.tipo_conta,
              config_organ = (SELECT exerc_entid.i_config_organ
     		                   FROM bethadba.exerc_entid
     						  WHERE exerc_entid.i_exercicios = rec_loa.i_exercicios AND
     						        exerc_entid.i_entidades = rec_loa.i_entidades),
              protect_conta = IF rec_loa.i_rec_ldo is not null AND contas_prop.tipo_conta = 'A' THEN 1 ELSE  0  ENDIF,
              detalhado_convenio = IF EXISTS (SELECT 1 FROM bethadba.rec_loa_val rlv
     										            WHERE rec_loa.i_loas = rlv.i_loas AND
     										                  rec_loa.i_rec_loa = rlv.i_rec_loa AND
     											             	rec_loa.i_exercicios = rlv.i_exercicios AND
     												            rec_loa.i_entidades = rlv.i_entidades AND
     										   EXISTS(SELECT 1 FROM bethadba.fontes_rec_detalhamentos frd
     											        WHERE frd.i_recursos = rlv.i_recursos AND
     													        frd.i_exercicios = rlv.i_exercicios AND
     													        frd.i_detalhamentos_fontes = rlv.i_detalhamentos_fontes AND
     													        frd.i_convenio is not null)) THEN 1 ELSE 0 ENDIF,
              bethadba.rec_loa.i_tipos_deducoes_rec,
              redutora = bethadba.dbf_cta_ehredutORacod(rec_loa.i_planos_contas,rec_loa.i_contas),
              desc_tipo_deducao =  (SELECT descricao
     										   FROM bethadba.tipos_deducoes_rec
     										  WHERE tipos_deducoes_rec.i_exercicios = rec_loa.i_exercicios AND
     											     tipos_deducoes_rec.i_entidades = rec_loa.i_entidades AND
     											     tipos_deducoes_rec.i_tipos_deducoes_rec = rec_loa.i_tipos_deducoes_rec),
              valor_mes1 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 1),
              valor_mes2 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 2),
              valor_mes3 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 3),
              valor_mes4 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 4),
              valor_mes5 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 5),
              valor_mes6 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 6),
              valor_mes7 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 7),
              valor_mes8 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 8),
              valor_mes9 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 9),
              valor_mes10 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 10),
              valor_mes11 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 11),
              valor_mes12 = (select sum(rec_loa_mens.valor) from bethadba.rec_loa_mens where rec_loa_mens.i_exercicios = rec_loa.i_exercicios and rec_loa_mens.i_loas = rec_loa.i_loas and rec_loa_mens.i_entidades = rec_loa.i_entidades and rec_loa_mens.i_rec_loa = rec_loa.i_rec_loa and rec_loa_mens.i_mes = 12),
     			has_contas_prop_exerc = if exists (select 1 from bethadba.contas_prop c_prop
                                                  where c_prop.i_planos_contas = contas.i_planos_contas and
                                                        c_prop.i_contas = contas.i_contas and
                                                        (rec_loa.i_exercicios between c_prop.exerc_ini AND c_prop.exerc_fin)) then 1 else 0 endif

        FROM bethadba.rec_loa LEFT OUTER JOIN
     	     bethadba.organogramas ON (bethadba.organogramas.i_config_organ = bethadba.rec_loa.i_config_organ AND
     		                           bethadba.organogramas.i_organogramas = bethadba.rec_loa.i_organogramas),
     		  bethadba.rec_loa LEFT OUTER JOIN
       	     bethadba.rec_ldo ON (bethadba.rec_ldo.i_exercicios = bethadba.rec_loa.i_exercicios AND
     		                      bethadba.rec_ldo.i_ldos = bethadba.rec_loa.i_ldos AND
     							  bethadba.rec_ldo.i_rec_ldo = bethadba.rec_loa.i_rec_ldo),
              bethadba.contas,
              bethadba.contas_prop
        WHERE ( bethadba.contas.i_planos_contas = bethadba.rec_loa.i_planos_contas ) AND
              ( bethadba.contas.i_contas = bethadba.rec_loa.i_contas ) AND
              ( bethadba.contas_prop.i_planos_contas = bethadba.contas.i_planos_contas ) AND
              ( bethadba.contas_prop.i_contas = bethadba.contas.i_contas ) AND
              (contas_prop.exerc_ini = (select max(cta_prop.exerc_ini)
                                          from bethadba.contas_prop as cta_prop
                                         where cta_prop.i_contas = contas.i_contas and
                                               cta_prop.i_planos_contas = contas.i_planos_contas and
                                               cta_prop.exerc_ini <= bethadba.rec_loa.i_exercicios))
              --and bethadba.contas.mascara like '4'+'%11130311000003%'
              and bethadba.rec_loa.i_exercicios = {{exercicio}}
              and bethadba.rec_loa.i_entidades = {{entidade}}
              and SUBSTRING(bethadba.contas.mascara,0,1) = 4
     ORDER BY bethadba.rec_loa.i_entidades ASC,
              bethadba.rec_loa.i_exercicios ASC,
              bethadba.rec_loa.i_loas ASC,
              bethadba.rec_loa.i_rec_loa ASC