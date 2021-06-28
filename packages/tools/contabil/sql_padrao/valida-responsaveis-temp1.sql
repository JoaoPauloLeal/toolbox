select *
            into #tabTempResponsaveis
           from (
                 select 'responsaveis' as tipo_registro,
                        r.cpf as chave_dsk1,
                        case r.tipo
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
                        end as chave_dsk2,
                        1 as sistema,
                        chave_dsk1 as cpf,
                        chave_dsk2 as tipo,
                        e.i_entidades,
                        r.i_responsaveis as responsavel,
                        0 as pessoa,
                        r.nome,
                        r.cargo,
                        r.num_registro as registro,
                        r.identidade as rg,
                        r.dt_emis_rg as dataEmissao,
                        r.orgao_emis_rg as orgaoEmissor,
                        r.uf_emis_rg as ufEmissaoRG,
                        isnull(bethadba.dbf_get_id_gerado(sistema, 'tiposResponsaveis', chave_dsk2),'') as idTipoResponsaveis
                   from sapo.entidades e join
                        sapo.responsaveis r on(r.i_entidades = e.i_entidades)
                  where e.i_entidades = {{entidade}} and
                        r.tipo <> 2

                 union all

                 select 'responsaveis' as tipo_registro,
                        pf.cpf as chave_dsk1,
                        case rc.tipos_responsabilidade
                            when 'C' then 'Controle Interno'
                            when 'U' then 'Unidade Gestora'
                            when 'O' then 'Outros'
                        end as chave_dsk2,
                        1 as sistema,
                        chave_dsk1 as cpf,
                        chave_dsk2 as tipo,
                        0 as i_entidades,
                        0 as responsavel,
                        pf.i_pessoas as pessoa,
                        p.nome,
                        fe.descricao as cargo,
                        '' as registro,
                        pf.rg,
                        pf.dt_emis_rg as dataEmissao,
                        pf.orgao_emis_rg as orgaoEmissor,
                        (select sigla
                           from bethadba.estados
                          where i_estados = pf.uf_emis_rg) as ufEmissaoRG,
                        isnull(bethadba.dbf_get_id_gerado(sistema, 'tiposResponsaveis', chave_dsk2),'') as idTipoResponsaveis
                   from bethadba.responsaveis_compl rc join
                        bethadba.responsaveis r on (rc.i_responsaveis = r.i_responsaveis) join
                        bethadba.pessoas_fisicas pf on (pf.i_pessoas = r.i_pessoas) join
                        bethadba.pessoas p on (pf.i_pessoas = p.i_pessoas) join
                        bethadba.funcoes_exerc fe on(r.i_funcoes_exerc = fe.i_funcoes_exerc) left outer join
                        bethadba.pessoas_enderecos pe on(pe.i_pessoas = p.i_pessoas) left outer join
                        bethadba.pessoas_emails em on(em.i_pessoas = p.i_pessoas)

                 union all

                 select 'responsaveis' as tipo_registro,
                        pf.cpf as chave_dsk1,
                        'Outros'  as chave_dsk2,
                        1 as sistema,
                        chave_dsk1 as cpf,
                        chave_dsk2 as tipo,
                        i_entidades,
                        0 as responsavel,
                        pf.i_pessoas as pessoa,
                        p.nome,
                        '' as cargo,
                        '' as registro,
                        pf.rg,
                        pf.dt_emis_rg as dataEmissao,
                        pf.orgao_emis_rg as orgaoEmissor,
                        (select sigla
                           from bethadba.estados
                          where i_estados = pf.uf_emis_rg) as ufEmissaoRG,
                        isnull(bethadba.dbf_get_id_gerado(sistema, 'tiposResponsaveis', chave_dsk2),'') as idTipoResponsaveis
                   from bethadba.pessoas_juridicas pj join
                        bethadba.pessoas p on (pj.responsavel = p.i_pessoas) join
                        bethadba.pessoas_fisicas pf on (pf.i_pessoas = p.i_pessoas)  join
                        sapo.credores c on (c.i_pessoas = pj.i_pessoas AND
                                            c.i_entidades = {{entidade}} and
                                            exists(select 1
                                                     from #tabTempMovCredores
                                                    where i_credores = c.i_credores))

                  union all

                 select 'responsaveis' as tipo_registro,
                        pf.cpf as chave_dsk1,
                        'Outros' as chave_dsk2,
                        1 as sistema,
                        chave_dsk1 as cpf,
                        chave_dsk2 as tipo,
                        0 as i_entidades,
                        0 as responsavel,
                        pf.i_pessoas as pessoa,
                        p.nome,
                        '' as cargo,
                        '' as registro,
                        pf.rg,
                        pf.dt_emis_rg as dataEmissao,
                        pf.orgao_emis_rg as orgaoEmissor,
                        (select sigla
                           from bethadba.estados
                          where i_estados = pf.uf_emis_rg) as ufEmissaoRG,
                        isnull(bethadba.dbf_get_id_gerado(sistema, 'tiposResponsaveis', chave_dsk2),'') as idTipoResponsaveis

                   from bethadba.equipes_planej join
                        bethadba.membros_equipes_planej on (equipes_planej.i_equipes_planej = membros_equipes_planej.i_equipes_planej) join
                        bethadba.pessoas_fisicas pf on (membros_equipes_planej.i_pessoas = pf.i_pessoas) join
                        bethadba.pessoas p on (pf.i_pessoas = p.i_pessoas)


                 ) as tab