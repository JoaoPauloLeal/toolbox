select *
                     into #tabTempEnderecos
                       from (
                             select isnull(bethadba.dbf_get_id_gerado(1,'municipios',upper(trim(nome_cidade)), trim(nome_estado), trim(nome_pais)),'') as idMunicipio,
                                    isnull(bethadba.dbf_get_id_gerado(1,'bairros',bethadba.dbf_retira_acentos(upper(trim(nome_bairro))), upper(trim(nome_cidade)), upper(trim(nome_estado)), upper(trim(nome_pais))),'') as idBairro,
                                    isnull(bethadba.dbf_get_id_gerado(1,'logradouros',bethadba.dbf_retira_acentos(upper(trim(nome_rua))),'RUA',upper(trim(nome_cidade)), upper(trim(nome_estado)), upper(trim(nome_pais))),'') as idLogradouro,
                                    '' as idLoteamento,
                                    '' as idDistrito,
                                    '' as idCondominio,
                                    null as cod_tipo_rua,
                                    null as tipo_rua,
                                    string(r.cep) as cep,
                                    isnull(string(r.numero),'0') as numero,
                                    0 as tipo_endereco,
                                    r.i_responsaveis as responsavel,
                                    null as pessoa,
                                    '' as nome_condominio,
                                    '' as nome_loteamento,
                                    '' as nome_distrito,
                                    isnull(r.endereco,ruas.nome) as nome_rua,
                                    isnull(r.bairro,bairros.nome) as nome_bairro,
                                    cidades.nome as nome_cidade,
                                    estados.nome as nome_estado,
                                    isnull(paises.nome,'') as nome_pais,
                                    'R' as ident,
                                    r.cpf as cpf
                               from sapo.responsaveis r join
                                    bethadba.entidades e on (e.i_entidades = r.i_entidades) left join
                                    bethadba.ruas on ((SELECT min(bairros_ruas.i_ruas)
                                                         from bethadba.bairros_ruas join
                                                              bethadba.ruas join
                                                              bethadba.cidades
                                                        where cidades.i_cidades = r.i_cidades) = ruas.i_ruas) left join
                                    bethadba.bairros on (bairros.i_bairros = (SELECT min(br.i_bairros)
                                                                                from bethadba.bairros as br join
                                                                                     bethadba.bairros_ruas join
                                                                                     bethadba.ruas join
                                                                                     bethadba.cidades
                                                                               where cidades.i_cidades = r.i_cidades)) left join
                                    bethadba.cidades on (isnull(r.i_cidades,ruas.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                                    bethadba.estados on (cidades.i_estados = estados.i_estados ) left join
                                    bethadba.paises on (estados.i_paises = paises.i_paises)
                              where r.i_entidades = {{entidade}} and
                                   r.tipo <> 2

                             union all

                             select isnull(bethadba.dbf_get_id_gerado(1,'municipios',upper(trim(nome_cidade)), trim(nome_estado), trim(nome_pais)),'') as idMunicipio,
                                    isnull(bethadba.dbf_get_id_gerado(1,'bairros',bethadba.dbf_retira_acentos(upper(trim(nome_bairro))), upper(trim(nome_cidade)), upper(trim(nome_estado)), upper(trim(nome_pais))),'') as idBairro,
                                    isnull(bethadba.dbf_get_id_gerado(1,'logradouros',bethadba.dbf_retira_acentos(upper(trim(nome_rua))), upper(trim(tipo_rua)), upper(trim(nome_cidade)), upper(trim(nome_estado)), upper(trim(nome_pais))),'') as idLogradouro,
                                    isnull(bethadba.dbf_get_id_gerado(1,'loteamentos',nome_loteamento,nome_bairro, nome_cidade, nome_estado, nome_pais),'') as idLoteamento,
                                    isnull(bethadba.dbf_get_id_gerado(1,'distrito',nome_distrito,nome_cidade, nome_estado, nome_pais),'') as idDistrito,
                                    isnull(bethadba.dbf_get_id_gerado(1,'condominios',nome_condominio,bethadba.dbf_retira_acentos(upper(trim(nome_rua))),nome_bairro, nome_cidade, nome_estado, nome_pais),'') as idCondominio,
                                    isnull(ruas.tipo,57) as cod_tipo_rua,
                                    (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                                    pe.cep,
                                    isnull(pe.numero,'0') as numero,
                                    if pe.tipo_endereco = 'P' then 1 else
                                       if pe.tipo_endereco = 'C' then 2 else 3 endif endif as tipo_endereco,
                                    null as responsavel,
                                    p.i_pessoas as pessoa,
                                    condominios.nome as nome_condominio,
                                    loteamentos.nome as nome_loteamento,
                                    isnull(distritos.nome,pe.nome_distrito) as nome_distrito,
                                    isnull(ruas.nome,pe.nome_rua) as nome_rua,
                                    isnull(bairros.nome,pe.nome_bairro) as nome_bairro,
                                    cidades.nome as nome_cidade,
                                    estados.nome as nome_estado,
                                    isnull(paises.nome,'') as nome_pais,
                                    'P' as ident,
                                    pf.cpf
                               from bethadba.entidades e join
                                    bethadba.pessoas p on (e.i_entidades = {{entidade}} AND
                                                           p.i_pessoas in (select distinct pessoa
                                                                             from #tabTempResponsaveis
                                                                            where pessoa is not null)) left join
                                    bethadba.pessoas_fisicas pf on (pf.i_pessoas = p.i_pessoas) left join
                                    bethadba.pessoas_enderecos pe on(pe.i_pessoas = p.i_pessoas) left join
                                    bethadba.condominios on(pe.i_condominios = condominios.i_condominios) left join
                                    bethadba.loteamentos on(pe.i_loteamentos = loteamentos.i_loteamentos) left join
                                    bethadba.distritos on (distritos.i_distritos = isnull(loteamentos.i_distritos,pe.i_distritos)) left join
                                    bethadba.ruas on (isnull(pe.i_ruas,condominios.i_ruas,(SELECT min(bairros_ruas.i_ruas)
                                                                                             from bethadba.bairros_ruas join
                                                                                                  bethadba.ruas join
                                                                                                  bethadba.cidades
                                                                                            where cidades.i_cidades = pe.i_cidades)) = ruas.i_ruas) left join
                                    bethadba.bairros on (bairros.i_bairros = isnull(pe.i_bairros,loteamentos.i_bairros,condominios.i_bairros,(SELECT min(br.i_bairros)
                                                                                                                                                from bethadba.bairros as br join
                                                                                                                                                     bethadba.bairros_ruas join
                                                                                                                                                     bethadba.ruas join
                                                                                                                                                     bethadba.cidades
                                                                                                                                               where cidades.i_cidades = pe.i_cidades))) left join
                                    bethadba.cidades on (isnull(ruas.i_cidades,pe.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                                    bethadba.estados on (cidades.i_estados = estados.i_estados ) left join
                                    bethadba.paises on (estados.i_paises = paises.i_paises)
                           order by tipo_endereco
                        ) as tab