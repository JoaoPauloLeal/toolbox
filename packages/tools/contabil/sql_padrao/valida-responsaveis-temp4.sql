select *
                       into #tabTempTelefones
                       from (SELECT  1 as principal,
                                     'CELULAR' as tipo,
                                     if length(sapo.dbf_retira_caracteres(r.telefone_cel)) in(8,9) then
                                         right('00' || sapo.dbf_retira_caracteres(r.telefone_cel),length(sapo.dbf_retira_caracteres(r.telefone_cel))+2)
                                     ELSE if left(sapo.dbf_retira_caracteres(r.telefone_cel),1) = 0 then
                                             right(sapo.dbf_retira_caracteres(r.telefone_cel),length(sapo.dbf_retira_caracteres(r.telefone_cel))-1)
                                          else
                                             right(sapo.dbf_retira_caracteres(r.telefone_cel),11)
                                          endif
                                     endif as numero,
                                     r.i_responsaveis,
                                     r.cpf
                                FROM sapo.responsaveis r
                               where r.tipo <> 2 and
                                     length(sapo.dbf_retira_caracteres(r.telefone_cel)) > 7

                             union all

                             SELECT  2 as principal,
                                     'FIXO' as tipo,
                                     if left(sapo.dbf_retira_caracteres(r.telefone_res),1) != 0 and length(sapo.dbf_retira_caracteres(r.telefone_res)) = 9 then
                                         substr(sapo.dbf_retira_caracteres(r.telefone_res),1,2) || '3' || substr(sapo.dbf_retira_caracteres(r.telefone_res),3,7)
                                     else if left(sapo.dbf_retira_caracteres(r.telefone_res),1) = 0 and length(sapo.dbf_retira_caracteres(r.telefone_res)) = 10 then
                                             substr(sapo.dbf_retira_caracteres(r.telefone_res),2,2) || '3' || substr(sapo.dbf_retira_caracteres(r.telefone_res),4,7)
                                          else if left(sapo.dbf_retira_caracteres(r.telefone_res),1) = 0 then
                                                 right(sapo.dbf_retira_caracteres(r.telefone_res),length(sapo.dbf_retira_caracteres(r.telefone_res))-1)
                                               else
                                                 right('00' || sapo.dbf_retira_caracteres(r.telefone_res),10)
                                               endif
                                          endif
                                     endif as numero,
                                     r.i_responsaveis,
                                     r.cpf
                                FROM sapo.responsaveis r
                               where r.tipo <> 2 and
                                     length(sapo.dbf_retira_caracteres(r.telefone_res)) > 6

                             union all

                             SELECT  3 as principal,
                                     'FAX' as tipo,
                                     if left(sapo.dbf_retira_caracteres(r.telefone_com),1) != 0 and length(sapo.dbf_retira_caracteres(r.telefone_com)) = 9 then
                                         substr(sapo.dbf_retira_caracteres(r.telefone_com),1,2) || '3' || substr(sapo.dbf_retira_caracteres(r.telefone_com),3,7)
                                     else if left(sapo.dbf_retira_caracteres(r.telefone_com),1) = 0 and length(sapo.dbf_retira_caracteres(r.telefone_com)) = 10 then
                                             substr(sapo.dbf_retira_caracteres(r.telefone_com),2,2) || '3' || substr(sapo.dbf_retira_caracteres(r.telefone_com),4,7)
                                          else if left(sapo.dbf_retira_caracteres(r.telefone_com),1) = 0 then
                                                 right(sapo.dbf_retira_caracteres(r.telefone_com),length(sapo.dbf_retira_caracteres(r.telefone_com))-1)
                                               else
                                                 right('00' || sapo.dbf_retira_caracteres(r.telefone_com),10)
                                               endif
                                          endif
                                     endif as numero,
                                     r.i_responsaveis,
                                     r.cpf
                                FROM sapo.responsaveis r
                               where r.tipo <> 2 and
                                     length(sapo.dbf_retira_caracteres(r.telefone_com)) > 6

                             union all

                             SELECT  1 as principal,
                                     'CELULAR' as tipo,
                                     if length(sapo.dbf_retira_caracteres(p.ddd_cel || p.celular)) in(8,9) then
                                         right('00' || sapo.dbf_retira_caracteres(p.celular),length(sapo.dbf_retira_caracteres(p.celular))+2)
                                     ELSE
                                         p.ddd_cel || sapo.dbf_retira_caracteres(p.celular)
                                     endif as numero,
                                     r.i_responsaveis,
                                     pf.cpf
                                from bethadba.responsaveis r join
                                             bethadba.pessoas p on(p.i_pessoas = r.i_pessoas) join
                                             bethadba.pessoas_fisicas pf on(pf.i_pessoas = p.i_pessoas)
                                       where length(sapo.dbf_retira_caracteres(p.celular)) > 7

                             union all

                             SELECT  2 as principal,
                                     'FIXO' as tipo,
                                     if length(sapo.dbf_retira_caracteres(p.telefone)) = 7 then
                                         right('00' || p.ddd || '3' || sapo.dbf_retira_caracteres(p.telefone),10)
                                     else
                                         right('00' || p.ddd || sapo.dbf_retira_caracteres(p.telefone),10)
                                     endif as numero,
                                     r.i_responsaveis,
                                     pf.cpf
                                from bethadba.responsaveis r join
                                             bethadba.pessoas p on(p.i_pessoas = r.i_pessoas) join
                                             bethadba.pessoas_fisicas pf on(pf.i_pessoas = p.i_pessoas)
                                       where length(sapo.dbf_retira_caracteres(p.telefone)) > 6

                             union all

                             SELECT  3 as principal,
                                     'FAX' as tipo,
                                     if length(sapo.dbf_retira_caracteres(p.fax)) = 7 then
                                         right('00' || p.ddd || '3' || sapo.dbf_retira_caracteres(p.fax),10)
                                     else
                                         right('00' || p.ddd || sapo.dbf_retira_caracteres(p.fax),10)
                                     endif as numero,
                                     r.i_responsaveis,
                                     pf.cpf
                                from bethadba.responsaveis r join
                                             bethadba.pessoas p on(p.i_pessoas = r.i_pessoas) join
                                             bethadba.pessoas_fisicas pf on(pf.i_pessoas = p.i_pessoas)
                                       where length(sapo.dbf_retira_caracteres(p.fax)) > 6) as tab