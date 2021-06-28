select Tab.i_contratos,
                valor_principal = sum(sld_principal),
                valor_juros = sum(sld_juros),
                valor_encargos = sum(sld_encargos)
           into #tabTempMovDividas
           from (select cc.i_contratos,
                        sld_principal = sum(if cc.conta_fp in ('5', 'F', 'O', 'P') then
                                               if l.tipo_lancamento = 'C' then l.valor_lancamento else -l.valor_lancamento endif
                                            else 0 endif),
                        sld_juros = sum(if cc.conta_fp in ('3', 'G', 'H', 'T') then
                                               if l.tipo_lancamento = 'C' then l.valor_lancamento else -l.valor_lancamento endif
                                            else 0 endif),
                        sld_encargos = sum(if cc.conta_fp in ('4', 'A', 'B', 'U') then
                                               if l.tipo_lancamento = 'C' then l.valor_lancamento else -l.valor_lancamento endif
                                            else 0 endif)
                   from sapo.contratos_contas cc join
                        (select i_plano_contas from sapo.parametros where parametros.ano_exerc = {{exercicio}} and parametros.i_entidades = {{entidade}}) as param on cc.i_plano_contas = param.i_plano_contas join
                        sapo.lancamentos l on (cc.i_contas = l.i_contas and
                                               cc.i_entidades = l.i_entidades and
                                               l.ano_exerc = {{exercicio}} and
                                               isnull(l.lanc_inicial,'') = '*')
                  where cc.i_entidades = {{entidade}}
                    and cc.conta_fp in ('3', 'G', 'H', 'T', '4', 'A', 'B', 'U', '5', 'F', 'O', 'P')
                  group by cc.i_contratos
                 having sld_principal <> 0 or sld_juros <> 0 or sld_encargos <> 0

                  union all

                 select distinct cc.i_contratos,
                        sld_principal = (select con.valor from sapo.contratos con
                                          where con.i_entidades = cc.i_entidades AND
                                                con.i_contratos = cc.i_contratos),
                        sld_juros = 0,
                        sld_encargos = 0
                   from sapo.contratos_contas cc join
                        (select i_plano_contas from sapo.parametros where parametros.ano_exerc = {{exercicio}} and parametros.i_entidades = {{entidade}}) as param on cc.i_plano_contas = param.i_plano_contas join
                        sapo.contratos_mov_controles on (cc.i_contratos = contratos_mov_controles.i_contratos AND
                                                         contratos_mov_controles.origem = 1 and
                                                         contratos_mov_controles.i_contratos_mov_control = 1 and
                                                         contratos_mov_controles.i_entidades = cc.i_entidades and
                                                         contratos_mov_controles.i_exercicios = {{exercicio}})
                  where cc.i_entidades = {{entidade}}
                    and cc.conta_fp in ('3', 'G', 'H', 'T', '4', 'A', 'B', 'U', '5', 'F', 'O', 'P')
                    and sld_principal <> 0
                ) as Tab
            group by Tab.i_contratos