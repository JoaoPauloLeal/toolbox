select 'fornecedores' as tipo_registro,
                 305 as sistema,
                 cre.i_entidades as chave_dsk1, 
                 cre.i_credores as chave_dsk2, 
                 trim(cre.nome) as nome, 
                 chave_dsk1 as entidadeDesktop,
                 chave_dsk2 as identificadorPrimeiro,
                 if cre.tipo_credor = 'F' then isnull(cre.cpf,'') else '' endif as cpf,
                 if cre.tipo_credor = 'J' then isnull(cre.cgc,'') else '' endif as cnpj,
                 case cre.tipo_credor 
                    when 'J' then isnull(cre.cgc,'')
                    when 'F' then isnull(cre.cpf,'')
                    else ''
                 end as cpfCnpj, 
                 trim(if cre.tipo_credor = 'J' then 
                    'JURIDICA' 
                 else     
                    'FISICA'
                 endif) as tipo,
                 isnull(trim(cre.nome_fantasia),'') as nomeFantasia,
                 isnull((select cd.i_estados from compras.cidades cd where cd.i_cidades = cre.i_cidades and i_estados < 28),0) as estado,
                 isnull(cre.unidade_federacao,'') as unidadeFederacao,
                 isnull(cre.i_cidades,'') as cod_cidade,
                 ISNULL(bethadba.dbf_get_id_gerado(sistema,'estados', estado),'') as estadoInscricao,
                 isnull(trim(compras.dbf_retira_caracteres(cre.inscricao_estadual)),'') as inscricaoEstadual, 
                 if inscricaoEstadual > 0 then 1 else 0 endif as inscricaoEstadValida, 
                 ISNULL(bethadba.dbf_get_id_gerado(sistema,'municipios', isnull(cre.i_cidades,0)),'') as municipio,
                 isnull(trim(compras.dbf_retira_caracteres(cre.inscricao_municipal)),'') as inscricaoMunicipal,               
                 'ATIVO' as situacao,                  
                 dateformat(isnull(cre.data_inclusao_credor,isnull(cre.data_alt,'1900-01-01')),'yyyy-mm-dd') as dataInclusao, 
                 isnull((select ca.tipo_registro from compras.credores_adic ca where ca.i_credores = cre.i_credores and ca.i_entidades = cre.i_entidades), 0) as regEmpresa,  
                 case regEmpresa 
                   when 1 then 'CARTORIO'
                   when 2 then 'JUNTA_COMERCIAL' 
                   when 3 then 'OAB'
                   else ''
                 end as orgaoRegistroEmpresa,   
                 isnull(cre.optante_simples,'') as optanteSimples, 
                 isnull((select compras.dbf_retira_caracteres(ca.numero_registro) from compras.credores_adic ca where ca.i_credores = cre.i_credores and ca.i_entidades = cre.i_entidades),'') as numeroRegistro,  
                 
                 if data_reg_junta is null then
                    isnull((select dateformat(ca.dt_registro_cvm, 'yyyy-mm-dd') from compras.credores_adic ca where ca.i_credores = cre.i_credores and ca.i_entidades = cre.i_entidades),'') 
                 else
                    dateformat(cre.data_reg_junta,'yyyy-mm-dd') 
                 endif as dataregistro,  
                                 
                case cre.porte_empresa
                    when 0 then 'NAO_CLASSIFICADA'
                    when 1 then 'MICROEMPRESA'
                    when 2 then 'EMPRESA_PEQUENO_PORTE'
                    when 3 then 'EMPRESA_MEDIO_PORTE'
                    when 4 then 'EMPRESA_GRANDE_PORTE'
                    when 5 then 'MICROEMPREENDEDOR_INDIVIDUAL'
                    else 'NAO_CLASSIFICADA'
                 end as porteEmpresa,
                 if i_naturezas is null then '' else isnull(bethadba.dbf_get_id_gerado(sistema,'naturezas-juridicas', cre.i_naturezas),'') endif as naturezaJuridica, 
                 isnull(bethadba.dbf_get_id_gerado(sistema,'responsaveis',cre.i_entidades,cre.cpf_responsavel),'') as responsavel,
                 isnull(trim(cre.bairro),'') as bairro,
                 isnull(trim(cre.endereco),'') as logradouro,                 
                 if cep is null then '' else trim(compras.dbf_retira_caracteres(right(repeat('0',8)+string(cep),8))) endif as cep,
                 isnull(left(trim(compras.dbf_retira_caracteres(cre.telefone)),11),'') as telefone,
                 isnull(trim(compras.dbf_retira_acentos(cre.email_fornecedor)),'') as email,
                 ISNULL((SELECT FIRST bethadba.dbf_get_id_gerado(sistema,tipo_registro,c.i_entidades,c.i_credores) as id 
                           FROM compras.credores c
                          WHERE c.nome = cre.nome
                            AND isnull(c.cgc,'') = isnull(cre.cgc,'')
                            AND isnull(c.cpf,'') = isnull(cre.cpf,'')
                            and c.tipo_credor = cre.tipo_credor
                            AND id IS NOT NULL
                            AND bethadba.dbf_get_situacao_registro(sistema,tipo_registro,c.i_entidades,c.i_credores) IN (2,4)
                       ORDER BY c.i_credores),'') as id_gerado
            from compras.credores cre 
            where bethadba.dbf_get_situacao_registro(sistema,tipo_registro,chave_dsk1,chave_dsk2) in(5,3)
            and cre.i_entidades = {{entidade}}
            /*Verificar se o forncedor foi utilizado*/
             and ('Sim' = 'Não'
                  or ('Sim' = 'Sim' 
                      and 
                      /*Se o fornecedor foi utilizado nos processos*/
                      exists( select 1 from compras.participantes p, compras.processos
                                           where p.i_entidades = cre.i_entidades
                                             and p.i_credores  = cre.i_credores
                                             and p.i_entidades = processos.i_entidades 
                                             and p.i_processo  = processos.i_processo
                                             and p.i_ano_proc  = processos.i_ano_proc
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
                           )and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = {{entidade}} and parametros_anuais.ano_exerc = processos.i_ano_proc))
                      or 
                      exists( select 1 from compras.participantes_processos p, compras.processos
                                           where p.i_entidades = cre.i_entidades
                                             and p.i_credores  = cre.i_credores
                                             and p.i_entidades = processos.i_entidades 
                                             and p.i_processo  = processos.i_processo
                                             and p.i_ano_proc  = processos.i_ano_proc
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
                           )and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = {{entidade}} and parametros_anuais.ano_exerc = processos.i_ano_proc))
                      or 
                      /*Se o fornecedor foi utilizado nos processos*/
                      exists( select 1 from compras.convidados p, compras.processos
                                           where p.i_entidades = cre.i_entidades
                                             and p.i_credores  = cre.i_credores
                                             and p.i_entidades = processos.i_entidades 
                                             and p.i_processo  = processos.i_processo
                                             and p.i_ano_proc  = processos.i_ano_proc
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
                           )and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = {{entidade}} and parametros_anuais.ano_exerc = processos.i_ano_proc))
                      or   
                      /*Se o fornecedor foi utilizado na cotação*/
                      exists(select 1 from compras.itens_col_prec_forn icp
                              where icp.i_entidades = cre.i_entidades 
                                and icp.i_credores  = cre.i_credores
                                and icp.i_ano_coleta >= {{exercicio}})
        
                      or  
                      /*Se o fornecedor foi utilizado nos contratos*/
                      exists(select 1 from compras.contratos
                              where contratos.i_entidades = cre.i_entidades  
                                and contratos.i_credores = cre.i_credores
                                and year(contratos.data_vcto) >= {{exercicio}}
                                and contratos.i_processo is not null)
                      or
                      /*Se o fornecedor foi utilizado nas compras diretas*/
                      exists(select 1 from compras.simples
                              where simples.i_entidades = cre.i_entidades  
                                and simples.i_credores = cre.i_credores
                                and year(simples.data_sim) >= {{exercicio}})
                      or 
                       
                      /*Se o fornecedor foi utilizado nas impugnações*/
                      exists(select first 1 from compras.impugnacoes_proc, compras.processos
                              where impugnacoes_proc.i_entidades = cre.i_entidades
                                and impugnacoes_proc.i_credores  = cre.i_credores
                                and impugnacoes_proc.i_entidades = processos.i_entidades 
                                and impugnacoes_proc.i_processo  = processos.i_processo
                                and impugnacoes_proc.i_ano_proc  = processos.i_ano_proc
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
                           )and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = {{entidade}} and parametros_anuais.ano_exerc = processos.i_ano_proc))
                    )
                  )
            order by dataInclusao desc, chave_dsk2