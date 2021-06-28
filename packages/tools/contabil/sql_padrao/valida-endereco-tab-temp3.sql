select *
           into #tabTempCredores
           from (select distinct 'credores' as tipo_registro,
                           isnull(c.cpf,c.cgc,'99999999999') as chave_dsk1,
                           1 as sistema,
                           c.nome as nome,
                           chave_dsk1 as cpf_cnpj,
                         if length(chave_dsk1) = 11 then 'FISICA' else 'JURIDICA' endif as tipoCredor,
                         e.i_entidades,
                         c.i_credores,
                         p.i_pessoas,
                         //Dados especificos da pessoa física
                         c.identidade as rg,
                         pf.orgao_emis_rg as orgaoEmissor,
                         pf.dt_emis_rg as dataEmissao,
                         (select sigla
                            from bethadba.estados
                           where i_estados = pf.uf_emis_rg) as ufEmissaoRG,
                         c.pis_pasep,
                         if pf.nacionalidade = 'B' then 'BRASILEIRO' else
                            if pf.nacionalidade = 'N' then 'NATURALIZADO' else 'ESTRANGEIRO' endif endif as nacionalidade,
                         (select bethadba.dbf_get_id_gerado(1,'municipios',upper(trim(cidades.nome)), trim(estados.nome), trim(paises.nome)) as idMunicipio
                                                           from bethadba.cidades  join
                                                    bethadba.estados on (cidades.i_estados = estados.i_estados) join
                                                                bethadba.paises on (estados.i_paises = paises.i_paises)
                                                          where cidades.i_cidades = pf.i_cidades) as idMunicipioNascimento,
                         pf.dt_nascimento,
                         isnull(sapo.dbf_retira_caracteres(c.inscricao_municipal),'') as inscricao_municipal,
                         (select bethadba.dbf_get_id_gerado(1,'municipios',upper(trim(cidades.nome)), trim(estados.nome), trim(paises.nome)) as idMunicipio
                                                           from bethadba.cidades  join
                                                                bethadba.estados on (cidades.i_estados = estados.i_estados) join
                                                                bethadba.paises on (estados.i_paises = paises.i_paises)
                                                          where cidades.i_cidades = (select first pe.i_cidades
                                                                                       from bethadba.pessoas_enderecos pe
                                                                                      where pe.i_pessoas = p.i_pessoas and
                                                                                            pe.i_cidades is not null
                                                                                   order by if pe.tipo_endereco = 'P' then 1 else
                                                                                                if pe.tipo_endereco = 'C' then 2 else 3 endif endif)) as idMunicipioInscricao,
                         //Dados especificos da pessoa física estrangeira
                         pt.data_chegada,
                         if pt.tipo_visto_est = 1 then 'PERMANENTE' else
                            if pt.tipo_visto_est = 2 then 'TEMPORARIO' else '' endif endif as tipo_visto,
                         pt.ident_estrangeiro,
                         pt.cart_trab_est,
                         pt.serie_cart_est,
                         pt.dt_exp_cart_est,
                         pt.dt_val_cart_est,
                         bethadba.dbf_get_id_gerado(1,'paises',isnull((select paises.nome
                                                                         from bethadba.paises
                                                                        where paises.i_paises = pt.i_paises),'')) as idPaisNascimentoEstr,
                         //Dados especificos da pessoa juridicas
                         pj.i_naturezas,
                         bethadba.dbf_get_id_gerado(1,'responsaveis',(select isnull(pf.cpf,'')
                                                                        from bethadba.pessoas p key join
                                                                             bethadba.pessoas_fisicas pf
                                                                       where p.i_pessoas = pj.responsavel), 'Outros') as idResponsavel,
                         (select bethadba.dbf_get_id_gerado(1,'estados', estados.nome, paises.nome) as idEstado
                                                           from bethadba.estados join
                                                                                                     bethadba.paises on (estados.i_paises = paises.i_paises)
                                                          where estados.sigla = c.unidade_federacao) as idEstadoInscricao,
                         isnull(right(sapo.dbf_retira_caracteres(pj.inscricao_estadual),12),'') as inscricao_estadual,
                         if pj.porte_empresa = '1' then 'MICROEMPRESA' ELSE
                            if pj.porte_empresa = '2' then 'EMPRESA_PEQUENO_PORTE' ELSE
                                if pj.porte_empresa = '3' then 'EMPRESA_MEDIO_PORTE' ELSE
                                    if pj.porte_empresa = '4' then 'EMPRESA_GRANDE_PORTE' ELSE
                                        if pj.porte_empresa = '5' then 'MICROEMPREENDEDOR_INDIVIDUAL' ELSE '' endif endif endif endif endif as porteEmpresa,
                         if c.optante_simples = 'S' then 'true' else 'false' endif as optanteSimples,
                         c.data_inclusao_credor,
                         c.i_cbo as idCBO,
                         '1 - credores' as origem,
                         null as tipo_docto

                    from sapo.entidades e join
                         sapo.credores c on(c.i_entidades = e.i_entidades) join
                         bethadba.pessoas p on(c.i_pessoas = p.i_pessoas) left outer join
                         bethadba.pessoas_fisicas pf on(p.i_pessoas = pf.i_pessoas) left outer join
                         bethadba.pessoas_estrangeiras pt on(p.i_pessoas = pt.i_pessoas) left outer join
                         bethadba.pessoas_juridicas pj on(p.i_pessoas = pj.i_pessoas)
                     where e.i_entidades = {{entidade}} and
                           exists(select 1
                                    from #tabTempMovCredores
                                   where i_credores = c.i_credores)

                 union all

                 select distinct 'credores' as tipo_registro,
                         isnull(contratos.cgc_cpf,'99999999999') as chave_dsk1,
                         1 as sistema,
                         contratos.contratado as nome,
                         chave_dsk1 as cpf_cnpj,
                         if length(chave_dsk1) = 11 then 'FISICA' else 'JURIDICA' endif as tipoCredor,
                         contratos.i_entidades,
                         contratos.i_contratos as i_credores,
                         null as i_pessoas,
                         //Dados especificos da pessoa física
                         null as rg,
                         null as orgaoEmissor,
                         null as dataEmissao,
                         null as ufEmissaoRG,
                         null as pis_pasep,
                         null as nacionalidade,
                         null as idMunicipioNascimento,
                         null as dt_nascimento,
                         '' as inscricao_municipal,
                         null as idMunicipioInscricao,
                         //Dados especificos da pessoa física estrangeira
                         null as data_chegada,
                         null as tipo_visto,
                         null as ident_estrangeiro,
                         null as cart_trab_est,
                         null as serie_cart_est,
                         null as dt_exp_cart_est,
                         null as dt_val_cart_est,
                         null as idPaisNascimentoEstr,
                         //Dados especificos da pessoa juridicas
                         null as i_naturezas,
                         null as idResponsavel,
                         null as idEstadoInscricao,
                         '' as inscricao_estadual,
                         '' as porteEmpresa,
                         null as optanteSimples,
                         contratos.data_assinatura as data_inclusao_credor,
                         null as idCBO,
                         '2 - contratosDivida' as origem,
                         null as tipo_docto

                   from sapo.contratos left join
                        sapo.credores on(credores.i_entidades = contratos.i_entidades AND
                                         isnull(credores.cpf,credores.cgc) = contratos.cgc_cpf)
                  where contratos.i_entidades = {{entidade}} and
                        tipo_contrato <> 'N' and
                        contratos.i_contratos in (select i_contratos
                                                    from #tabTempMovDividas) and
                        not exists(select 1
                                     from sapo.credores
                                    where credores.i_entidades = contratos.i_entidades AND
                                          credores.cpf = contratos.cgc_cpf and
                                          length(contratos.cgc_cpf) = 11) and
                        not exists(select 1
                                     from sapo.credores
                                    where credores.i_entidades = contratos.i_entidades AND
                                          credores.cgc = contratos.cgc_cpf and
                                          length(contratos.cgc_cpf) = 14)
                 union ALL

                 select distinct 'credores' as tipo_registro,
                        isnull((if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc
                                else dctos_fiscais.cpf endif), '99999999999') as chave_dsk1,
                        1 as sistema,
                        dctos_fiscais.emitente as nome,
                        chave_dsk1 as cpf_cnpj,
                        if length(chave_dsk1) = 11 then 'FISICA' else 'JURIDICA' endif as tipoCredor,
                        dctos_fiscais.i_entidades,
                        dctos_fiscais.num_docto as i_credores,
                        null as i_pessoas,
                        //Dados especificos da pessoa física
                        null as rg,
                        null as orgaoEmissor,
                        null as dataEmissao,
                        null as ufEmissaoRG,
                        null as pis_pasep,
                        null as nacionalidade,
                        null as idMunicipioNascimento,
                        null as dt_nascimento,
                        '' as inscricao_municipal,
                        null as idMunicipioInscricao,
                        //Dados especificos da pessoa física estrangeira
                        null as data_chegada,
                        null as tipo_visto,
                        null as ident_estrangeiro,
                        null as cart_trab_est,
                        null as serie_cart_est,
                        null as dt_exp_cart_est,
                        null as dt_val_cart_est,
                        null as idPaisNascimentoEstr,
                        //Dados especificos da pessoa juridicas
                        null as i_naturezas,
                        null as idResponsavel,
                        null as idEstadoInscricao,
                        '' as inscricao_estadual,
                        '' as porteEmpresa,
                        null as optanteSimples,
                        dctos_fiscais.data_emissao as data_inclusao_credor,
                        null as idCBO,
                        '3 - documentosFiscais' as origem,
                        if dctos_fiscais.tipo_docto = 'O' then 'Ordens de Pagamento' else
                            if dctos_fiscais.tipo_docto = 'D' then 'Despesas Extras' else
                                if dctos_fiscais.tipo_docto = 'R' then 'Restos a Pagar' else
                                    if dctos_fiscais.tipo_docto = 'L' then 'Liquidação de Empenho' else
                                        if dctos_fiscais.tipo_docto = 'A' then 'Liquidação Anterior' else
                                            if dctos_fiscais.tipo_docto = 'C' then 'Cancelamento de Restos' else
                                                if dctos_fiscais.tipo_docto = 'E' then 'Empenho Anterior' else 'Ordem Anterior'
                                                endif endif endif endif endif endif endif as tipo_docto

                   from sapo.dctos_fiscais
                  where dctos_fiscais.i_entidades = {{entidade}} and
                        dctos_fiscais.ano_exerc >= {{exercicio}}-1 and
                        isnull(dctos_fiscais.cpf,dctos_fiscais.cgc) is not null and
                        not exists(select 1
                                     from sapo.credores
                                    where credores.i_entidades = dctos_fiscais.i_entidades AND
                                          dctos_fiscais.tipo_juridico <> 'J' and
                                          credores.cpf = dctos_fiscais.cpf)  and
                        not exists(select 1
                                     from sapo.credores
                                    where credores.i_entidades = dctos_fiscais.i_entidades AND
                                          dctos_fiscais.tipo_juridico = 'J' and
                                          credores.cgc = dctos_fiscais.cgc)
            order by i_credores
           ) as tab