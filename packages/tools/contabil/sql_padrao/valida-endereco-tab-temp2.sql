select distinct i_credores
           into #tabTempMovCredores
           from (select i_credores
                   from sapo.empenhos
                  where ano_exerc between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.despexs
                  where ano_exerc between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.devolucoes
                  where ano_exerc between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.precatorios
                  where year(data_precatorio) between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.empenhos_ant
                  where ano_exerc between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.ordens_ant
                  where ano_exerc between {{exercicio}}-2 and {{exercicio}} AND
                        i_entidades = {{entidade}} and
                        i_credores is not null

                 union all

                 select i_credores
                   from sapo.contratos left join
                        sapo.credores on(credores.i_entidades = contratos.i_entidades AND
                                         isnull(credores.cpf,credores.cgc) = contratos.cgc_cpf)
                  where contratos.i_entidades = {{entidade}} and
                        tipo_contrato <> 'N' and
                        isnull(credores.cpf,credores.cgc) is not null and
                        contratos.i_contratos in (select i_contratos
                                                    from #tabTempMovDividas)

                 union all

                 select distinct c1.i_credores
                   from sapo.credores c1 join
                        (select cpf as docto
                           from sapo.dctos_fiscais
                          where i_entidades = {{entidade}} and
                                ano_exerc in ({{exercicio}}-1,{{exercicio}})) as Tab on (c1.cpf = Tab.docto)
                  where c1.i_entidades = {{entidade}}

                 union all

                 select distinct c1.i_credores
                   from sapo.credores c1 join
                        (select cgc as docto
                           from sapo.dctos_fiscais
                          where i_entidades = {{entidade}} and
                                ano_exerc in ({{exercicio}}-1,{{exercicio}})) as Tab on (c1.cgc = Tab.docto)
                  where c1.i_entidades = {{entidade}}

                 union all

                 select distinct credores.i_credores
                   from sapo.convenios left join
                        sapo.credores on(credores.i_entidades = convenios.i_entidades AND
                                         credores.cgc = convenios.cnpj_beneficiario)
                  where convenios.i_entidades = {{entidade}} and
                        credores.cgc is not null

                 ) as tab