select *
            into #tabTempContasBancarias
            from (
                  SELECT 'C' as movBancaria,
                         'contasBancariasCredor' as tipo_registro,
                         right('000' || pc.i_bancos, 3) as i_bancos,
                         (string(pc.i_agencias) || if dvAgencia <> '' then '-' endif || dvAgencia) as agenciaDigito,
                         agencias.dv_agencia as dvAgencia,
                         if pc.status = 'A' then 'ABERTA' else 'ENCERRADA' endif as situacao,
                         if locate(pc.num_conta,'-') > 0 then
                             left(pc.num_conta,locate(pc.num_conta,'-')-1)
                         else
                             pc.num_conta
                         endif as numeroConta,
                         if locate(pc.num_conta,'-',-1) > 0 then
                             right(pc.num_conta,length(pc.num_conta) - locate(pc.num_conta,'-',-1))
                         endif as digitoConta,
                         '' as descricao,
                         if pc.tipo_conta = '2' then 'POUPANCA' else 'CORRENTE' endif as tipoConta,
                         if (select first padrao
                               from sapo.contas_credores cc
                              where cc.i_entidades = c.i_entidades and
                                    cc.i_credores = c.i_credores and
                                    cc.conta_banco = pc.num_conta and
                                    cc.tipo_conta = pc.tipo_conta and
                                    sapo.dbf_mesmobancosapo(cc.i_entidades, cc.i_bancos, pc.i_bancos, pc.i_agencias) = 1
                              order by cc.i_contas_credores desc) = 'S' then 'true' else 'false' endif as padrao,
                         c.i_credores,
                         isnull(c.cpf,c.cgc) as cpf_cnpj,
                         1 as sistema,
                         c.i_entidades,
                         null  as idResponsavel,
                         null as tipo,
                         null as dataInicial,
                         null as motivoFinal,
                         null as dataFinal,
                         bethadba.dbf_get_id_gerado(1,'bancos',i_bancos) as idBanco,
                         isnull(bethadba.dbf_get_id_gerado(1,'agencias',i_bancos, agenciaDigito),'') as idAgencia,
                         (select first i_contas_credores
                            from sapo.contas_credores cc
                           where cc.i_entidades = c.i_entidades and
                                 cc.i_credores = c.i_credores and
                                 cc.conta_banco = pc.num_conta and
                                 cc.tipo_conta = pc.tipo_conta and
                                 sapo.dbf_mesmobancosapo(cc.i_entidades, cc.i_bancos, pc.i_bancos, pc.i_agencias) = 1
                           order by cc.i_contas_credores desc) as i_contas,
                         pc.i_agencias as i_agencias
                    FROM bethadba.pessoas_contas pc join
                         bethadba.agencias on(agencias.i_bancos = pc.i_bancos and
                                              agencias.i_agencias = pc.i_agencias) join
                         bethadba.pessoas p on(p.i_pessoas = pc.i_pessoas) join
                         sapo.credores c on(p.i_pessoas = c.i_pessoas and
                                            c.i_credores in (SELECT *
                                                               from #tabTempMovCredores))
                   where c.i_entidades = {{entidade}} and
                         i_contas is not null

           union all

           select 'E' as movBancaria,
                  'contasBancarias' as tipo_registro,
                  right('000' || bancos.numero, 3) as i_bancos,
                  (contas.i_agencias || if dvAgencia <> '' then '-' endif || dvAgencia) as agenciaDigito,
                  agencias.dv_agencia as dvAgencia,
                  '' as situacao,
                  if locate(contas.conta_banco,'-') > 0 then
                       left(contas.conta_banco,locate(contas.conta_banco,'-')-1)
                  else
                       contas.conta_banco
                  endif as numeroConta,
                  if locate(contas.conta_banco,'-',-1) > 0 then
                       right(contas.conta_banco,length(contas.conta_banco) - locate(contas.conta_banco,'-',-1))
                  endif as digitoConta,
                  left(contas.descricao,40) as descricao,
                  if contas.bancaria = 'C' then 'CORRENTE'  else 'POUPANCA' endif as tipoConta,
                  '' as padrao,
                  null as i_credores,
                  null as cpf_cnpj,
                  1 as sistema,
                  contas.i_entidades,
                  bethadba.dbf_get_id_gerado(1,'responsaveis',(select isnull(cpf,'')
                                                                 from sapo.responsaveis
                                                                where i_entidades = contas.i_entidades and
                                                                      i_responsaveis = cda.i_responsaveis), tipo) as idResponsavel,
                  isnull((select case r.tipo
                                    when 1 then 'Contador'
                                    when 2 then 'Ordenador'
                                    when 3 then 'Prefeito'
                                    when 4 then 'Vice-Prefeito'
                                    when 5 then 'Vereador'
                                    when 6 then 'Vereador-Presidente'
                                    when 7 then 'Secretario Municipal'
                                    when 8 then if e.ufed = 'MG' then
                                                    'Presidente da câmara'
                                                ELSE
                                                    'Gestor de Fundo'
                                                endif
                                    when 9 then if e.ufed in('MG','ES') then
                                                    'Dirigente da entidade'
                                                ELSE
                                                    'Diretor-Presidente'
                                                endif
                                    when 10 then if e.ufed = 'RR' then
                                                    'Superintendente'
                                                ELSE
                                                    'Controle Interno'
                                                endif
                                    when 11 then 'Outros'
                                    when 12 then 'Técnico de Contabilidade'
                                    when 13 then 'Jurídico'
                                    when 14 then 'Gestor'
                                    when 15 then 'Arquiteto'
                                    when 16 then 'Engenheiro'
                                  end as tipo
                             from sapo.entidades e join
                                  sapo.responsaveis r on(r.i_entidades = e.i_entidades)
                            where r.i_entidades = contas.i_entidades and
                                  r.i_responsaveis = cda.i_responsaveis),'') as tipo,
                  string(contas.inclusao) as dataInicial,
                  if dataFinal <> '' then 'Inativação da conta bancária' endif as motivoFinal,
                  isnull(string(contas.data_inativacao),'') as dataFinal,
                  bethadba.dbf_get_id_gerado(1,'bancos',i_bancos) as idBanco,
                  isnull(bethadba.dbf_get_id_gerado(1,'agencias',i_bancos, agenciaDigito),'') as idAgencia,
                  contas.i_contas,
                  contas.i_agencias
             from sapo.contas left join
                  sapo.contas_dados_adic cda on (cda.i_plano_contas = contas.i_plano_contas and
                                                 cda.ano_exerc = {{exercicio}} and
                                                 cda.i_entidades = contas.i_entidades and
                                                 cda.i_contas = contas.i_contas) left join
                  sapo.bancos on(bancos.i_entidades = contas.i_entidades and
                                 bancos.i_bancos = contas.i_bancos) join
                  bethadba.agencias on (agencias.i_agencias = contas.i_agencias and
                                        agencias.i_bancos = contas.i_bancos_febraban) left join
                  (select i_plano_contas from sapo.parametros where parametros.ano_exerc = {{exercicio}} and parametros.i_entidades = {{entidade}}) as param on contas.i_plano_contas = param.i_plano_contas
            where contas.i_entidades = {{entidade}} AND
                  isnull(exerc_fin,2999) >= {{exercicio}} and
                  contas.bancaria <> 'N'
                 //  and
                 // exists (select first 1 from sapo.lancamentos l
                //            where l.ano_exerc in ({{exercicio}},{{exercicio}}-1,{{exercicio}}-2) and
                //                  l.i_entidades = {{entidade}} and
                //                 l.i_contas in (contas.i_contas))
) as tab