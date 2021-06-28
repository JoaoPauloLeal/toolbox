SELECT data_ativacao = string(data_movimento),
                             data_inativacao = isnull((select first string(mr2.data_movimento)
                                                         FROM sapo.movimentacao_responsavel mr2
                                                        where mr2.i_entidades = mr.i_entidades and
                                                              mr2.i_responsaveis = mr.i_responsaveis and
                                                              mr2.tipo_movimento = 2 and
                                                              mr2.data_movimento >= data_ativacao
                                                     order by mr2.data_movimento asc),''),
                             motivo_baixa = isnull((select descricao
                                                      FROM sapo.movimentacao_responsavel mr3
                                                     where mr3.i_entidades = mr.i_entidades and
                                                           mr3.i_responsaveis = mr.i_responsaveis and
                                                           mr3.tipo_movimento = 2 and
                                                           string(mr3.data_movimento) = data_inativacao),''),
                             mr.i_responsaveis,
                             r.cpf
                        into #tabTempPeriodosResponsabilidades
                        FROM sapo.movimentacao_responsavel mr key join
                             sapo.responsaveis r
                       where r.i_entidades = {{entidade}} and
                             r.tipo <> 2 and
                             mr.tipo_movimento = 1