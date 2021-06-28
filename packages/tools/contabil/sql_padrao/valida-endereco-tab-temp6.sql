select 'logradouros' as tipo_registro,
                 1 as sistema,
                 registro,
                 trim(nome_rua) as nome_rua,
                 bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                 trim(tipo_rua) as tipo_rua,
                 trim(nome_pais) as nome_pais,
                 trim(nome_estado) as nome_estado,
                 trim(nome_cidade) as nome_cidade,
                 null as nome_distrito,
                 trim(nome_bairro) as nome_bairro,
                 sapo.dbf_retira_caracteres_especiais(isnull((select first rr.cep
                                                                from bethadba.ruas rr
                                                               where isnull(rr.i_cidades,cod_cidades) = cod_cidades and
                                                                     rr.nome = nome_rua and
                                                                     rr.cep is not null),cep_entidade),'N','N') as cep_rua,
                 idMunicipio,
                 idTipo
            from (
                   select 'da Entidade '||e.i_entidades as registro,
                          REPLACE(r.nome,'''','') as nome_rua,
                          isnull(string(r.tipo),'57') as cod_tipo_rua,
                          (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                          cidades.nome as nome_cidade,
                          (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                          estados.nome as nome_estado,
                          paises.nome as nome_pais,
                          e.i_cidades as cod_cidades,
                          sapo.dbf_retira_caracteres_especiais(e.cep,'N','N') as cep_entidade,
                          (select first br.i_bairros from bethadba.bairros_ruas br where br.i_ruas = r.i_ruas) as cod_bairros,
                          bairros.nome as nome_bairro,
                          isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                 isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                          isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo

                     from bethadba.entidades e left join
                          bethadba.ruas r on (e.i_ruas = r.i_ruas) left join
                          bethadba.bairros on (bairros.i_bairros = cod_bairros) left join
                          bethadba.cidades on (e.i_cidades = cidades.i_cidades) left join
                          bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                          bethadba.paises on (estados.i_paises = paises.i_paises)
                    where e.i_entidades = {{entidade}} and
                          length(isnull(r.nome,'')) > 0
                 ) as Tab

           union all

          select 'logradouros' as tipo_registro,
                 1 as sistema,
                 registro,
                 trim(nome_rua) as nome_rua,
                 bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                 trim(tipo_rua) as tipo_rua,
                 trim(nome_pais) as nome_pais,
                 trim(nome_estado) as nome_estado,
                 trim(nome_cidade) as nome_cidade,
                 null as nome_distrito,
                 trim(nome_bairro) as nome_bairro,
                 (select first sapo.dbf_retira_caracteres_especiais(rr.cep,'N','N') from bethadba.ruas rr
                   where isnull(rr.i_cidades,cod_cidades) = cod_cidades and
                         rr.nome = nome_rua and
                         rr.cep is not null) as cep_rua,
                 idMunicipio,
                 idTipo
            from (
                   select 'do Organograma '||o.i_organogramas as registro,
                          REPLACE(r.nome,'''','') as nome_rua,
                          isnull(string(r.tipo),'57') as cod_tipo_rua,
                          (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                          cidades.nome as nome_cidade,
                          (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                          estados.nome as nome_estado,
                          paises.nome as nome_pais,
                          isnull(r.i_cidades,e.i_cidades) as cod_cidades,
                          bairros.i_bairros as cod_bairros,
                          bairros.nome as nome_bairro,
                          isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                 isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                          isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo
                     from bethadba.organogramas_sapo os join
                          bethadba.organogramas o on (os.i_config_organ = o.i_config_organ AND
                                                      os.i_organogramas = o.i_organogramas) join
                          bethadba.entidades e on (os.i_entidades = e.i_entidades) left join
                          bethadba.ruas r on (o.i_ruas = r.i_ruas) left join
                          bethadba.bairros on (bairros.i_bairros = o.i_bairros) left join
                          bethadba.cidades on (isnull(r.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                          bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                          bethadba.paises on (estados.i_paises = paises.i_paises)
                    where os.ano_exerc = {{exercicio}} and
                          os.i_entidades = {{entidade}} and
                          length(isnull(r.nome,'')) > 0
                  ) as Tab

            union all

           select 'logradouros' as tipo_registro,
                  1 as sistema,
                  registro,
                  trim(nome_rua) as nome_rua,
                  bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                  trim(tipo_rua) as tipo_rua,
                  trim(nome_pais) as nome_pais,
                  trim(nome_estado) as nome_estado,
                  trim(nome_cidade) as nome_cidade,
                  null as nome_distrito,
                  trim(nome_bairro) as nome_bairro,
                  (select first left(string(sapo.dbf_retira_caracteres_especiais(rr.cep,'N','N')),8) from sapo.responsaveis rr
                    where isnull(rr.i_cidades,cod_cidades) = cod_cidades and
                          rr.endereco = nome_rua and
                          rr.cep is not null) as cep_rua,
                  idMunicipio,
                  idTipo
             from (
                    select 'do Responsável '||r.i_responsaveis as registro,
                           REPLACE(r.endereco,'''','') as nome_rua,
                           '57' as cod_tipo_rua,
                           (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                           cidades.nome as nome_cidade,
                           (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                           estados.nome as nome_estado,
                           paises.nome as nome_pais,
                           isnull(r.i_cidades,e.i_cidades) as cod_cidades,
                           r.bairro as nome_bairro,
                           isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                  isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                           isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo
                      from sapo.responsaveis r join
                           bethadba.entidades e on (r.i_entidades = e.i_entidades) left join
                           bethadba.cidades on (isnull(r.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                           bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                           bethadba.paises on (estados.i_paises = paises.i_paises)
                     where r.i_entidades = {{entidade}} and
                     length(isnull(r.endereco,'')) > 0
                    ) as Tab

              union all

             select 'logradouros' as tipo_registro,
                    1 as sistema,
                    registro,
                    trim(nome_rua) as nome_rua,
                    bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                    trim(tipo_rua) as tipo_rua,
                    trim(nome_pais) as nome_pais,
                    trim(nome_estado) as nome_estado,
                    trim(nome_cidade) as nome_cidade,
                    null as nome_distrito,
                    null as nome_bairro,
                    null as cep_rua,
                    idMunicipio,
                    idTipo
               from (
                     select 'do Contrato de Dívida '||c.i_contratos as registro,
                            REPLACE(c.endereco,'''','') as nome_rua,
                            '57' as cod_tipo_rua,
                            (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                            cidades.nome as nome_cidade,
                            (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                            estados.nome as nome_estado,
                            paises.nome as nome_pais,
                            isnull(c.i_cidades,e.i_cidades) as cod_cidades,
                            isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                   isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                            isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo,
                            isnull(sapo.dbf_get_cta_contrato({{exercicio}},(select i_plano_contas from sapo.parametros where i_entidades = {{entidade}} and ano_exerc = {{exercicio}}),c.i_entidades,c.i_contratos,'P'),0) as contaP,
                            isnull(sapo.dbf_get_cta_contrato({{exercicio}},(select i_plano_contas from sapo.parametros where i_entidades = {{entidade}} and ano_exerc = {{exercicio}}),c.i_entidades,c.i_contratos,'F'),0) as contaF,
                            isnull(sapo.dbf_get_cta_contrato({{exercicio}},(select i_plano_contas from sapo.parametros where i_entidades = {{entidade}} and ano_exerc = {{exercicio}}),c.i_entidades,c.i_contratos,'5'),0) as conta5
                       from sapo.contratos c join
                            bethadba.entidades e on (c.i_entidades = e.i_entidades) left join
                            bethadba.cidades on (isnull(c.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                            bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                            bethadba.paises on (estados.i_paises = paises.i_paises) join
                            (select distinct l.i_contas as contaCC from sapo.lancamentos l where l.ano_exerc = {{exercicio}} and l.i_entidades = {{entidade}}) Tab on (Tab.contaCC = contaP or
                                                                                                                                                                         Tab.contaCC = contaF or
                                                                                                                                                                         Tab.contaCC = conta5)
                      where c.i_entidades = {{entidade}} and
                            c.tipo_contrato <> 'N' and
                            length(isnull(c.endereco,'')) > 0
                     ) as Tab

               union all

              select 'logradouros' as tipo_registro,
                     1 as sistema,
                     registro,
                     trim(nome_rua) as nome_rua,
                     bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                     trim(tipo_rua) as tipo_rua,
                     trim(nome_pais) as nome_pais,
                     trim(nome_estado) as nome_estado,
                     trim(nome_cidade) as nome_cidade,
                     trim(nome_distrito) as nome_distrito,
                     trim(nome_bairro) as nome_bairro,
                     (select first left(string(sapo.dbf_retira_caracteres_especiais(ppe.cep,'N','N')),8) from bethadba.pessoas_enderecos ppe
                       where isnull(ppe.i_cidades,Tab.cod_cidades) = Tab.cod_cidades and
                             ppe.nome_rua = Tab.nome_rua and
                             ppe.cep is not null) as cep_rua,
                     idMunicipio,
                     idTipo
                from (
                       select 'do Credor '||c.i_credores as registro,
                              REPLACE(isnull(r.nome,pe.nome_rua,''),'''','') as nome_rua,
                              isnull(string(r.tipo),'57') as cod_tipo_rua,
                              (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                              cidades.nome as nome_cidade,
                              (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                              estados.nome as nome_estado,
                              paises.nome as nome_pais,
                              isnull(distritos.nome,pe.nome_distrito) as nome_distrito,
                              isnull(pe.i_cidades,e.i_cidades) as cod_cidades,
                              bairros.i_bairros as cod_bairros,
                              isnull(bairros.nome,pe.nome_bairro) as nome_bairro,
                              isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                     isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                              isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo
                         from sapo.credores c join
                              bethadba.pessoas p on(p.i_pessoas = c.i_pessoas) join
                              bethadba.pessoas_enderecos pe on(pe.i_pessoas = p.i_pessoas) join
                              bethadba.entidades e on (c.i_entidades = e.i_entidades) left join
                              bethadba.ruas r on (pe.i_ruas = r.i_ruas) left join
                              bethadba.distritos on (distritos.i_distritos = pe.i_distritos) left join
                              bethadba.bairros on (bairros.i_bairros = pe.i_bairros) left join
                              bethadba.cidades on (isnull(pe.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                              bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                              bethadba.paises on (estados.i_paises = paises.i_paises)
                        where c.i_entidades = {{entidade}} and
                              length(isnull(r.nome,pe.nome_rua,'')) > 0 and
                              c.i_credores in (select Tab.i_credores
                                                 from #tabTempMovCredores Tab)
                      ) as Tab

                union all

               select 'logradouros' as tipo_registro,
                      1 as sistema,
                      registro,
                      trim(nome_rua) as nome_rua,
                      bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                      trim(tipo_rua) as tipo_rua,
                      trim(nome_pais) as nome_pais,
                      trim(nome_estado) as nome_estado,
                      trim(nome_cidade) as nome_cidade,
                      null as nome_distrito,
                      trim(nome_bairro) as nome_bairro,
                      (select first sapo.dbf_retira_caracteres_especiais(rr.cep,'N','N') from bethadba.ruas rr
                        where isnull(rr.i_cidades,cod_cidades) = cod_cidades and
                              rr.nome = nome_rua and
                              rr.cep is not null) as cep_rua,
                      idMunicipio,
                      idTipo
                 from (
                       select 'da Agência '||a.i_agencias as registro,
                              REPLACE(r.nome,'''','') as nome_rua,
                              isnull(string(r.tipo),'57') as cod_tipo_rua,
                              (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                              cidades.nome as nome_cidade,
                              (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                              estados.nome as nome_estado,
                              paises.nome as nome_pais,
                              isnull(a.i_cidades,r.i_cidades,e.i_cidades) as cod_cidades,
                              bairros.i_bairros as cod_bairros,
                              bairros.nome as nome_bairro,
                              isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                     isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                              isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo
                         from bethadba.agencias a join
                              bethadba.entidades e on ({{entidade}} = e.i_entidades) left join
                              bethadba.ruas r on (a.i_ruas = r.i_ruas) left join
                              bethadba.bairros on (bairros.i_bairros = a.i_bairros) left join
                              bethadba.cidades on (isnull(a.i_cidades,r.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                              bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                              bethadba.paises on (estados.i_paises = paises.i_paises)
                        where i_agencias in (select i_agencias from #tabTempContasBancarias) and
                              length(isnull(r.nome,'')) > 0
                       ) as Tab

                 union all

                 select 'logradouros' as tipo_registro,
                        1 as sistema,
                        registro,
                        trim(nome_rua) as nome_rua,
                        bethadba.dbf_retira_acentos(upper(trim(nome_rua))) as chave_nome_rua,
                        trim(tipo_rua) as tipo_rua,
                        trim(nome_pais) as nome_pais,
                        trim(nome_estado) as nome_estado,
                        trim(nome_cidade) as nome_cidade,
                        null as nome_distrito,
                        trim(nome_bairro) as nome_bairro,
                        (select first sapo.dbf_retira_caracteres_especiais(rr.cep,'N','N') from bethadba.ruas rr
                          where isnull(rr.i_cidades,cod_cidades) = cod_cidades and
                                rr.nome = nome_rua and
                                rr.cep is not null) as cep_rua,
                        idMunicipio,
                        idTipo
                 from (
                        select 'da Audiência '||a.i_audiencias||' de '||a.i_exercicios as registro,
                               REPLACE(r.nome,'''','') as nome_rua,
                               isnull(string(r.tipo),'57') as cod_tipo_rua,
                               (select descricao from #TempTiposLogradouros where chave_dsk1 = cod_tipo_rua) as tipo_rua,
                               cidades.nome as nome_cidade,
                               (select first controle_migracao_registro.i_chave_dsk1
                                 from bethadba.controle_migracao_registro
                                where SIMILAR(cidades.nome,controle_migracao_registro.i_chave_dsk1) > 79 and
                                      controle_migracao_registro.id_gerado is not null) as nome_cidade_aux,
                               estados.nome as nome_estado,
                               paises.nome as nome_pais,
                               isnull(r.i_cidades,e.i_cidades) as cod_cidades,
                               bairros.i_bairros as cod_bairros,
                               bairros.nome as nome_bairro,
                               isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(nome_cidade), trim(nome_estado), trim(nome_pais)),
                                      isnull(bethadba.dbf_get_id_gerado(1,'municipios',trim(isnull(nome_cidade_aux,'')), trim(nome_estado), trim(nome_pais)),'Município inválido')) as idMunicipio,
                               isnull(bethadba.dbf_get_id_gerado(1,'tiposLogradouros',cod_tipo_rua),bethadba.dbf_get_id_gerado(1,'tiposLogradouros','57')) as idTipo
                          from bethadba.audiencias a join
                               bethadba.entidades e on ({{entidade}} = e.i_entidades) left join
                               bethadba.ruas r on (a.i_ruas = r.i_ruas) left join
                               bethadba.bairros on (bairros.i_bairros = a.i_bairros) left join
                               bethadba.cidades on (isnull(r.i_cidades,e.i_cidades) = cidades.i_cidades) left join
                               bethadba.estados on (cidades.i_estados = estados.i_estados) left join
                               bethadba.paises on (estados.i_paises = paises.i_paises)
                         where a.i_exercicios >= {{exercicio}} and
                               length(isnull(r.nome,'')) > 0
                       ) as Tab

                 order by tipo_registro, sistema, registro, nome_rua, tipo_rua, nome_cidade, nome_bairro, cep_rua, idMunicipio, idTipo