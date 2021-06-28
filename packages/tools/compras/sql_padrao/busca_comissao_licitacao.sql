select distinct 'comissoes-licitacao' as tipo_registro,
                  responsaveis.i_entidades as chave_dsk1,
                  responsaveis.i_responsavel as chave_dsk2,
                  305 as sistema,
                  '' as numero,
                  if (descr_finalidade is null) or (trim(descr_finalidade) = '') then
                      'Sem informação'
                  else
                      left(descr_finalidade,250)
                  endif as finalidade,
                  (case tipo_comissao
                    when 'P' then 'PERMANENTE'
                    when 'G' then 'PERMANENTE'
                    when 'E' then 'ESPECIAL'
                    when 'S' then 'ESPECIAL'
                    when 'L' then 'ESPECIAL'
                    else 'PERMANENTE'
                   end) as tipoComissao,
                  isnull(dateformat(data_expira, 'yyyy-mm-dd'), '') as dataExpiracao,
                  isnull(dateformat(data_exonera, 'yyyy-mm-dd'), '') as dataExoneracao,
                  case comissao_port_decr
                    when 'P' then 3
                    when 'D' then 2
                    when 'C' then 8
                    when 'R' then 5
                    when 'L' then 6
                    when 'A' then 7
                    else 4
                  end as cod_tiposAto,
                  isnull(dateformat(responsaveis.data_publ,'yyyy-mm-dd'),dateformat(responsaveis.data_desig,'yyyy-mm-dd')) as dataCriacao,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'tipos-ato',cod_tiposAto),'') as tipoAto,
                  ISNULL((SELECT FIRST bethadba.dbf_get_id_gerado(sistema, 'atos', r.i_entidades, isnull(compras.dbf_retira_caracteres(r.portaria_comissao),''),cod_tiposAto, isnull(dateformat(r.data_publ,'yyyy-mm-dd'),dateformat(r.data_desig,'yyyy-mm-dd'))) as id
                           FROM compras.responsaveis r
                          WHERE r.portaria_comissao = responsaveis.portaria_comissao
                            and r.comissao_port_decr = responsaveis.comissao_port_decr
                            and r.data_publ = responsaveis.data_publ
                            AND id IS NOT NULL
                            AND bethadba.dbf_get_situacao_registro(sistema,'atos',r.i_entidades,isnull(compras.dbf_retira_caracteres(r.portaria_comissao),''),cod_tiposAto, isnull(dateformat(r.data_publ,'yyyy-mm-dd'),dateformat(r.data_desig,'yyyy-mm-dd'))) IN (2,4)
                       ORDER BY r.portaria_comissao),'') as ato,
                  if (tipo_comissao <> 'G') and (processos.modalidade in (13,14)) then
                      isnull(processos.i_processo,0)
                  else
                      0
                  endif as tipoIncompativel,
                  if (tipo_comissao <> 'G') and (processos.modalidade in (13,14)) then
                      isnull(processos.i_ano_proc,0)
                  else
                      0
                  endif as anoTipoIncompativel,
                  if (tipo_comissao in ('P', 'E', 'S')) and (processos.modalidade in (7))  then
                      isnull(processos.i_processo,0)
                  else
                      0
                  endif as procTipoIncompativelLeilao,
                  if (tipo_comissao in ('P', 'E', 'S')) and (processos.modalidade in (7)) then
                      isnull(processos.i_ano_proc,0)
                  else
                      0
                  endif as anoProcTipoIncompativelLeilao,
                  isnull(responsaveis.cpf_titular,'') as cpf_autoridade,
                  length(responsaveis.cpf_titular) as tamCpfAutoridade,
                  isnull(responsaveis.cpf_presid,'') as cpf_presid,
                  length(responsaveis.cpf_presid) as tamCpf,
                  cpf_membro1,
                  cpf_membro2,
                  cpf_membro3,
                  cpf_membro4,
                  cpf_membro5,
                  cpf_membro6,
                  cpf_membro7,
                  cpf_membro8,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_presid),'') as responsavelPresid,
                  case responsaveis.tipo_comissao
                    when 'P' then 'PRESIDENTE'
                    when 'E' then 'PRESIDENTE'
                    when 'S' then 'PRESIDENTE'
                    when 'L' then 'LEILOEIRO'
                    when 'G' then 'PREGOEIRO'
                  end as atribuicaoPresid,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro1),'') as responsavel1,
                  (if responsavel1 is not null and responsavel1 != '' then 'MEMBRO' else '' endif) as atribuicao1,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro2),'') as responsavel2,
                  (if responsavel2 is not null and responsavel2 != '' then 'MEMBRO' else '' endif) as atribuicao2,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro3),'') as responsavel3,
                  (if responsavel3 is not null and responsavel3 != '' then 'MEMBRO' else '' endif) as atribuicao3,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro4),'') as responsavel4,
                  (if responsavel4 is not null and responsavel4 != '' then 'MEMBRO' else '' endif) as atribuicao4,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro5),'') as responsavel5,
                  (if responsavel5 is not null and responsavel5 != '' then 'MEMBRO' else '' endif) as atribuicao5,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro6),'') as responsavel6,
                  (if responsavel6 is not null and responsavel6 != '' then 'MEMBRO' else '' endif) as atribuicao6,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro7),'') as responsavel7,
                  (if responsavel7 is not null and responsavel7 != '' then 'MEMBRO' else '' endif) as atribuicao7,
                  ISNULL(bethadba.dbf_get_id_gerado(sistema,'responsaveis', chave_dsk1, cpf_membro8),'') as responsavel8,
                  (if responsavel8 is not null and responsavel8 != '' then 'MEMBRO' else '' endif) as atribuicao8,
                   ISNULL((SELECT FIRST bethadba.dbf_get_id_gerado(sistema,tipo_registro,r.i_entidades,r.i_responsavel) as id
                           FROM compras.responsaveis r
                          WHERE r.tipo_comissao = responsaveis.tipo_comissao
                            AND r.data_expira = responsaveis.data_expira
                            and r.data_desig = responsaveis.data_desig
                            and r.comissao_port_decr = responsaveis.comissao_port_decr
                            and r.cpf_presid = responsaveis.cpf_presid
                            and r.portaria_comissao = responsaveis.portaria_comissao
                            AND id IS NOT NULL
                            AND bethadba.dbf_get_situacao_registro(sistema,tipo_registro,r.i_entidades,r.i_responsavel) IN (2,4)
                       ORDER BY r.i_responsavel),'') as id_gerado
             from compras.responsaveis, compras.processos
            where bethadba.dbf_get_situacao_registro(sistema,tipo_registro,chave_dsk1, chave_dsk2) in (5,3)
              and responsaveis.i_entidades = {{entidade}}
              and responsaveis.i_responsavel = processos.i_responsavel
              and responsaveis.i_entidades = processos.i_entidades
              and processos.i_entidades = {{entidade}}
                       and processos.i_forma_julg is not null
                       and (   (processos.i_ano_proc >= {{exercicio}})
                           or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is null
                               and not exists(select ap.i_processo
                                                from compras.anl_processos ap
                                               where ap.i_entidades = processos.i_entidades
                                                 and ap.i_processo = processos.i_processo
                                                 and ap.i_ano_proc = processos.i_ano_proc)
                               )
                           or (processos.i_ano_proc < {{exercicio}} and processos.data_homolog is not null
                               and (    (processos.data_contrato_transformers is null and processos.continuado_ano >={{exercicio}})
                                     or year(processos.data_contrato_transformers) >={{exercicio}})
                               )
                           )and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = {{entidade}} and parametros_anuais.ano_exerc = processos.i_ano_proc)
         order by 3