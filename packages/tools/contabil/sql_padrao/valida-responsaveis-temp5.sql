select *
                        into #tabTempEmails
                        from (SELECT  r.email,
                                      r.i_responsaveis,
                                      r.cpf
                                 FROM sapo.responsaveis r
                                where r.tipo <> 2 and
                                      locate(r.email, '@') > 0 and
                                      locate(r.email, '@') = locate(r.email, '@',-1) and
                                      locate(substr(r.email,locate(r.email, '@')+1, length(r.email)-locate(r.email, '@')),'.') > 0 AND
                                      length(substr(r.email,locate(r.email, '.',-1)+1, length(r.email)-locate(r.email, '.',-1))) > 1

                              union all

                              SELECT  p.email,
                                      r.i_responsaveis,
                                      pf.cpf
                                 from bethadba.responsaveis r join
                                      bethadba.pessoas p on(p.i_pessoas = r.i_pessoas) join
                                      bethadba.pessoas_fisicas pf on(pf.i_pessoas = p.i_pessoas)
                                where locate(p.email, '@') > 0 and
                                      locate(p.email, '@') = locate(p.email, '@',-1) and
                                      locate(substr(p.email,locate(p.email, '@')+1, length(p.email)-locate(p.email, '@')),'.') > 0 AND
                                      length(substr(p.email,locate(p.email, '.',-1)+1, length(p.email)-locate(p.email, '.',-1))) > 1
                              ) as tab