select * from (
select
                 tipo_registro,
                 sistema,
                 chave_dsk1,
                 chave_dsk2,
                 chave_dsk3,
                 chave_dsk4,
                 chave_dsk5,
                 chave_dsk6,
                 chave_dsk7,    
                 exercicio,
                 credor,
                 tipo,
                 numero,
                 serie,
                 codigoValidacao,
                 data,
                 valor,
                 valorDesconto,
                 finalidade,
                 dt_vencimentos,
                 classificacao,
                 num_docto,
                 tipo_docto,
                 (if credor is null or tipo is null then
                     '9999999999' || chave_dsk1|| chave_dsk2|| chave_dsk3|| chave_dsk4|| chave_dsk5|| chave_dsk6|| chave_dsk7 
                 else
                     chave_dsk1|| chave_dsk2|| chave_dsk3|| chave_dsk4|| chave_dsk5|| chave_dsk6|| chave_dsk7
                 endif) as ordenacao
            from (
            select 
                   'comprovantes' as tipo_registro,
                            1 as sistema,
                            df.ano_exerc as chave_dsk1,
                            df.i_entidades as chave_dsk2,
                            i_tipo_dcto_fiscal as chave_dsk3,
                            i_numero_dcto_fiscal as chave_dsk4,
                            if df.tipo_juridico = 'J' then df.cgc else df.cpf endif as cpf_cnpj,                            
                            isnull(cpf_cnpj,'') as chave_dsk5,
                            num_docto as chave_dsk6,
                            tipo_docto as chave_dsk7,  
                                                      
                            df.ano_exerc as exercicio,
                            bethadba.dbf_get_id_gerado(sistema,'credores', isnull(cpf_cnpj,'')) as credor,
                            bethadba.dbf_get_id_gerado(1,'tiposDeComprovante', df.i_tipo_dcto_fiscal) as tipo, 
                            df.i_numero_dcto_fiscal as numero,
                            isnull(df.serie_nf,'') as serie,
                            isnull(numero_autenticacao,'') as codigoValidacao,
                            date(isNull(df.data_emissao, '1900-01-01')) as data,                                                        
                            valor_dcto_fiscal as valor,
                            0 as valorDesconto,                            
                            isNull(df.historico_dcto_fiscal, '') as finalidade,                            
                            isnull(data_vencimento, data_emissao) as dt_vencimentos, 
                            eh_prestacao = (if exists(select 1
                                                        from sapo.prestacoes p
                                                       where p.ano_exerc = df.ano_exerc
                                                         and p.i_entidades = df.i_entidades
                                                         and isnull(p.i_ordens, p.i_restos) = df.num_docto
                                                     ) then
                                                    1                                                 
                                            else 0 
                                            endif),                             
                            (if eh_prestacao = 0 then 'COMPROVANTE_COMPRAS' else 'COMPROVANTE_PRESTACOES' endif) as classificacao,
                            num_docto as num_docto,
                            tipo_docto                         
                       from sapo.dctos_fiscais df
                      where df.ano_exerc in ({{exercicio}} - 1,{{exercicio}}) //Busca os dctos fiscais do ano anterior, pois na prestação de contas de pgtos efetuados no ano anterios, o ano_exerc é o do documento (ex. ano do empenho)
                        and df.i_entidades = {{entidade}}
                        and df.i_tipo_dcto_fiscal <> 5 //descarta os comprovantes de diárias, pois essas são migrados pelo arqjoblet de diárias
                   ) as tab                               
              where bethadba.dbf_get_situacao_registro(sistema,tipo_registro,chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5, chave_dsk6, chave_dsk7) in(5,3)
              order by ordenacao  ) as tab where credor is null