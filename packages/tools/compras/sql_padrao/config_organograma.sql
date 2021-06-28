select distinct  i_entidades as chave_dsk1,
                 ano_exerc as chave_dsk2,
                 '##.###.#####' as mascara,
                 'Configuração organograma ' + String(ano_exerc) as descricao,
                 '.' as separador,
                 305 as sistema,
                 'configuracoes-organogramas' as  tipo_registro,
                 1 as nivel1,
                 2 as digito1,
                 'Orgão' as descricao1,
                 2 as nive2,
                 3 as digito2,
                 'Unidade' as descricao2,
                 3 as nive3,
                 5 as digito3,
                 'Centro de Custo' as descricao3,
                ISNULL( (SELECT FIRST bethadba.dbf_get_id_gerado(sistema,tipo_registro,par.i_entidades,par.ano_exerc) as id
                           FROM compras.parametros_anuais par
                          WHERE id IS NOT NULL
                            and chave_dsk2 = par.ano_exerc
                            AND bethadba.dbf_get_situacao_registro(sistema,tipo_registro,par.i_entidades,par.ano_exerc) IN (2,4)
                       ORDER BY chave_dsk1
                       ), '') as id_gerado
           from compras.parametros_anuais
           where bethadba.dbf_get_situacao_registro(sistema,tipo_registro,chave_dsk1,chave_dsk2) in(5,3)
             and ano_exerc in(select anoProc as exercicio
                   from (select distinct i_ano_proc as anoProc
                          from compras.processos
                         where processos.i_entidades ={{entidade}}
                           and ((processos.i_ano_proc >= {{exercicio}})
                              or (processos.i_ano_proc < {{exercicio}}
                                  and processos.data_homolog is null
                                  and not exists(select ap.i_processo
                                                   from compras.anl_processos ap
                                                  where ap.i_entidades = processos.i_entidades
                                                    and ap.i_processo = processos.i_processo
                                                    and ap.i_ano_proc = processos.i_ano_proc))
                              or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is not null
                                  and (    (processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                           or year(processos.data_contrato_transformers) >= {{exercicio}})))
                         UNION ALL //SQL para considerar os exercicios vinculados as AF's que estão relacionados aos processos que serao migrados
                           (select distinct(sequ_autor.i_ano) as anoProc
                              from compras.sequ_autor key join compras.processos
                             where processos.i_entidades = {{entidade}}
                               and sequ_autor.i_entidades = processos.i_entidades
                               and sequ_autor.i_processo = processos.i_processo
                               and sequ_autor.i_ano_proc = processos.i_ano_proc
                               and sequ_autor.i_ano is not null
                               and ((processos.i_ano_proc >= {{exercicio}}) or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is null
                                     and not exists(select ap.i_processo
                                                  from compras.anl_processos ap
                                             where ap.i_entidades = processos.i_entidades
                                               and ap.i_processo = processos.i_processo
                                               and ap.i_ano_proc = processos.i_ano_proc))
                                  or (processos.i_ano_proc < 2018 and processos.data_homolog is not null
                                      and ((processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                            or year(processos.data_contrato_transformers) >= {{exercicio}}))))
                      UNION ALL
                         select distinct(requisicao.i_ano) as anoProc
                           from compras.requisicao
                          where tipo = 1  /* Tipo 1: Solicitação de compra - Tipo 2: Solicitação de fornecimento*/
                            and i_ano >= {{exercicio}}
                            and year(data_req) = i_ano
                           and (i_entidades_sol = {{entidade}} or i_entidades = {{entidade}})
                        ) as tabExerc
                 group by exercicio
                 order by exercicio)
             and i_entidades = {{entidade}}
             and exists(select 1 from (
                            select distinct i_ano_proc
                              from compras.processos
                             where processos.i_entidades = parametros_anuais.i_entidades
                               and processos.i_ano_proc in(select anoProc as exercicio
                   from (select distinct i_ano_proc as anoProc
                          from compras.processos
                         where processos.i_entidades ={{entidade}}
                           and ((processos.i_ano_proc >= {{exercicio}})
                              or (processos.i_ano_proc < {{exercicio}}
                                  and processos.data_homolog is null
                                  and not exists(select ap.i_processo
                                                   from compras.anl_processos ap
                                                  where ap.i_entidades = processos.i_entidades
                                                    and ap.i_processo = processos.i_processo
                                                    and ap.i_ano_proc = processos.i_ano_proc))
                              or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is not null
                                  and (    (processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                           or year(processos.data_contrato_transformers) >= {{exercicio}})))
                         UNION ALL //SQL para considerar os exercicios vinculados as AF's que estão relacionados aos processos que serao migrados
                           (select distinct(sequ_autor.i_ano) as anoProc
                              from compras.sequ_autor key join compras.processos
                             where processos.i_entidades = {{entidade}}
                               and sequ_autor.i_entidades = processos.i_entidades
                               and sequ_autor.i_processo = processos.i_processo
                               and sequ_autor.i_ano_proc = processos.i_ano_proc
                               and sequ_autor.i_ano is not null
                               and ((processos.i_ano_proc >= {{exercicio}}) or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is null
                                     and not exists(select ap.i_processo
                                                  from compras.anl_processos ap
                                             where ap.i_entidades = processos.i_entidades
                                               and ap.i_processo = processos.i_processo
                                               and ap.i_ano_proc = processos.i_ano_proc))
                                  or (processos.i_ano_proc < 2018 and processos.data_homolog is not null
                                      and ((processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                            or year(processos.data_contrato_transformers) >= {{exercicio}}))))
                      UNION ALL
                         select distinct(requisicao.i_ano) as anoProc
                           from compras.requisicao
                          where tipo = 1  /* Tipo 1: Solicitação de compra - Tipo 2: Solicitação de fornecimento*/
                            and i_ano >= {{exercicio}}
                            and year(data_req) = i_ano
                           and (i_entidades_sol = {{entidade}} or i_entidades = {{entidade}})
                        ) as tabExerc
                 group by exercicio
                 order by exercicio)

                            union all

                            select distinct(sequ_autor.i_ano)
                              from compras.sequ_autor key join compras.processos
                             where processos.i_entidades = parametros_anuais.i_entidades
                               and sequ_autor.i_entidades = processos.i_entidades
                               and sequ_autor.i_processo = processos.i_processo
                               and sequ_autor.i_ano_proc = processos.i_ano_proc
                               and sequ_autor.i_ano is not null
                               and (processos.i_ano_proc in(select anoProc as exercicio
                   from (select distinct i_ano_proc as anoProc
                          from compras.processos
                         where processos.i_entidades ={{entidade}}
                           and ((processos.i_ano_proc >= {{exercicio}})
                              or (processos.i_ano_proc < {{exercicio}}
                                  and processos.data_homolog is null
                                  and not exists(select ap.i_processo
                                                   from compras.anl_processos ap
                                                  where ap.i_entidades = processos.i_entidades
                                                    and ap.i_processo = processos.i_processo
                                                    and ap.i_ano_proc = processos.i_ano_proc))
                              or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is not null
                                  and (    (processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                           or year(processos.data_contrato_transformers) >= {{exercicio}})))
                         UNION ALL //SQL para considerar os exercicios vinculados as AF's que estão relacionados aos processos que serao migrados
                           (select distinct(sequ_autor.i_ano) as anoProc
                              from compras.sequ_autor key join compras.processos
                             where processos.i_entidades = {{entidade}}
                               and sequ_autor.i_entidades = processos.i_entidades
                               and sequ_autor.i_processo = processos.i_processo
                               and sequ_autor.i_ano_proc = processos.i_ano_proc
                               and sequ_autor.i_ano is not null
                               and ((processos.i_ano_proc >= {{exercicio}}) or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is null
                                     and not exists(select ap.i_processo
                                                  from compras.anl_processos ap
                                             where ap.i_entidades = processos.i_entidades
                                               and ap.i_processo = processos.i_processo
                                               and ap.i_ano_proc = processos.i_ano_proc))
                                  or (processos.i_ano_proc < 2018 and processos.data_homolog is not null
                                      and ((processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                            or year(processos.data_contrato_transformers) >= {{exercicio}}))))
                      UNION ALL
                         select distinct(requisicao.i_ano) as anoProc
                           from compras.requisicao
                          where tipo = 1  /* Tipo 1: Solicitação de compra - Tipo 2: Solicitação de fornecimento*/
                            and i_ano >= {{exercicio}}
                            and year(data_req) = i_ano
                           and (i_entidades_sol = {{entidade}} or i_entidades = {{entidade}})
                        ) as tabExerc
                 group by exercicio
                 order by exercicio))
                           union all
                           select distinct i_ano
                              from compras.requisicao
                             where (requisicao.i_entidades_sol = parametros_anuais.i_entidades or
                                   requisicao.i_entidades = parametros_anuais.i_entidades)
                               and requisicao.i_ano in (select anoProc as exercicio
                   from (select distinct i_ano_proc as anoProc
                          from compras.processos
                         where processos.i_entidades ={{entidade}}
                           and ((processos.i_ano_proc >= {{exercicio}})
                              or (processos.i_ano_proc < {{exercicio}}
                                  and processos.data_homolog is null
                                  and not exists(select ap.i_processo
                                                   from compras.anl_processos ap
                                                  where ap.i_entidades = processos.i_entidades
                                                    and ap.i_processo = processos.i_processo
                                                    and ap.i_ano_proc = processos.i_ano_proc))
                              or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is not null
                                  and (    (processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                           or year(processos.data_contrato_transformers) >= {{exercicio}})))
                         UNION ALL //SQL para considerar os exercicios vinculados as AF's que estão relacionados aos processos que serao migrados
                           (select distinct(sequ_autor.i_ano) as anoProc
                              from compras.sequ_autor key join compras.processos
                             where processos.i_entidades = {{entidade}}
                               and sequ_autor.i_entidades = processos.i_entidades
                               and sequ_autor.i_processo = processos.i_processo
                               and sequ_autor.i_ano_proc = processos.i_ano_proc
                               and sequ_autor.i_ano is not null
                               and ((processos.i_ano_proc >= {{exercicio}}) or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is null
                                     and not exists(select ap.i_processo
                                                  from compras.anl_processos ap
                                             where ap.i_entidades = processos.i_entidades
                                               and ap.i_processo = processos.i_processo
                                               and ap.i_ano_proc = processos.i_ano_proc))
                                  or (processos.i_ano_proc < 2018 and processos.data_homolog is not null
                                      and ((processos.data_contrato_transformers is null and processos.continuado_ano >= {{exercicio}})
                                            or year(processos.data_contrato_transformers) >= {{exercicio}}))))
                      UNION ALL
                         select distinct(requisicao.i_ano) as anoProc
                           from compras.requisicao
                          where tipo = 1  /* Tipo 1: Solicitação de compra - Tipo 2: Solicitação de fornecimento*/
                            and i_ano >= {{exercicio}}
                            and year(data_req) = i_ano
                           and (i_entidades_sol = {{entidade}} or i_entidades = {{entidade}})
                        ) as tabExerc
                 group by exercicio
                 order by exercicio)
                       ) valida)
           order by  chave_dsk1, chave_dsk2