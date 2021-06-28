SELECT * FROM (
SELECT 'proposta-participante' as tipo_registro,
                 participantes.i_entidades as chave_dsk1,
                 participantes.i_ano_proc as chave_dsk2,
                 participantes.i_processo as chave_dsk3,
                 participantes.i_credores as chave_dsk4,
                 participantes.i_item as chave_dsk5,
                 305 as sistema,
                 isnull(bethadba.dbf_get_id_gerado(sistema,'processos-administrativo',chave_dsk1,chave_dsk2,chave_dsk3),'') as processoAdministrativo,
                 isnull(bethadba.dbf_get_id_gerado(sistema,'participante-licitacao',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk4),'') as participante,
                 isnull(bethadba.dbf_get_id_gerado(sistema,'processos-administrativo-itens',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk5),'') as item,
                 if(formaEspecial = 4 and processos.controle_qtd_cred = 3) then
                     isnull(participantes.qtde_cotada,1)
                 else 
                     isnull((select qtde_item from compras.itens_processo i where i.i_entidades = participantes.i_entidades and i.i_processo = participantes.i_processo and i.i_ano_proc = participantes.i_ano_proc and i.i_item = participantes.i_item ),1)
                 endif as quantidade,
                 isnull((select f.tipo_julgam from compras.forma_julg f where f.i_forma_julg = processos.i_forma_julg and f.i_entidades = processos.i_entidades),1) as formaJulg, //Tipo de Julgamento (1-Por Item, 2-Por Lote, 3-Global)
                 isnull((select f.forma_especial from compras.forma_julg f where f.i_forma_julg = processos.i_forma_julg and f.i_entidades = processos.i_entidades),0) as formaEspecial, //Forma especial de julgamento (0-Nenhuma, 1-Maior desconto sobre o item, 2-Maior desconto sobre tabela/catálogo, 3-Menor Preço Por Lotes sem Itens, 4-Credenciamento, 5-Menor Adicional sobre Tabela, 6-Maior Lance)
                 if (formaEspecial = 1) or (formaEspecial = 2) or (formaEspecial = 5) then 'true' else 'false' endif as porPercentual, 
                 if formaJulg = 2 then 'true' else 'false' endif as porLote, 
                 if (formaEspecial = 1 or formaEspecial = 2 or formaEspecial = 5) then isnull(vlr_descto,0) else isnull(preco_unit_part,0) endif as valorUnitarioPercentual,
                 isnull(vlr_descto,0) as percentualProposto, // quando é por lote e percentual
                 ordem_clas as colocacao,
                 isnull(nome_marca,'') as marca,
                 case participantes.situacao
                    when 0 then 'NAO_ANALISADO'
                    when 1 then 'PERDEU'
                    when 2 then 'VENCEU'   
                    when 3 then 'DESCLASSIFICADO'
                    when 4 then 'NAO_COTOU'
                    when 5 then 'EMPATE_REAL'
                    when 6 then 'CANCELADO'
                    when 7 then 'CLASSIFICADO_LANCE'
                    when 8 then 'CANCELADO'
                    when 9 then 'INABILITADO'
                    when 10 then 'CREDENCIADO'
                    when 11 then 'DESCREDENCIADO'
                 else
                    'NAO_ANALISADO'
                 end as situacao,
                 dateformat(isnull(data_homolog,'1900-01-01'),'yyyy-mm-dd') as homologacao,
                 processos.modalidade AS modalidade,
                 isnull(bethadba.dbf_get_id_gerado(sistema, tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5),'') as id_gerado         
            from compras.participantes, compras.processos
           where bethadba.dbf_get_situacao_registro(sistema,tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5) in(5,3)
             and bethadba.dbf_get_situacao_registro(sistema,'processos-administrativo', chave_dsk1, chave_dsk2, chave_dsk3) in(4)
             and bethadba.dbf_get_situacao_registro(sistema,'participante-licitacao',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk4) in (4) 
             and participantes.i_entidades = {{entidade}}
             and participantes.i_entidades = processos.i_entidades
             and participantes.i_processo = processos.i_processo
             and participantes.i_ano_proc = processos.i_ano_proc
             
             UNION ALL 

           select distinct
               'proposta-participante' as tipo_registro,
               processos.i_entidades as chave_dsk1,
               processos.i_ano_proc as chave_dsk2,
               processos.i_processo as chave_dsk3,
               participantes_processos.i_credores as chave_dsk4,
               itens_processo.i_item as chave_dsk5,
               305 as sistema,
               isnull(bethadba.dbf_get_id_gerado(sistema,'processos-administrativo',chave_dsk1,chave_dsk2,chave_dsk3),'') as processoAdministrativo,
               isnull(bethadba.dbf_get_id_gerado(sistema,'participante-licitacao',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk4),'') as participante,
               isnull(bethadba.dbf_get_id_gerado(sistema,'processos-administrativo-itens',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk5),'') as item,
               0 as quantidade,
               isnull(forma_julg.tipo_julgam,1) as formaJulg, //Tipo de Julgamento (1-Por Item, 2-Por Lote, 3-Global)      
               4 as formaEspecial,
               if (formaEspecial = 1) or (formaEspecial = 2) or (formaEspecial = 5) then 'true' else 'false' endif as porPercentual, 
                   if formaJulg = 2 then 'true' else 'false' endif as porLote,
                   0 as valorUnitarioPercentual,
                   0 as percentualProposto, // quando é por lote e percentual
                   2 as colocacao,
                   '' as marca,
                   'DESCREDENCIADO' as situacao,
                   dateformat(isnull(processos.data_homolog,'1900-01-01'),'yyyy-mm-dd') as homologacao,
               processos.modalidade AS modalidade,
               isnull(bethadba.dbf_get_id_gerado(sistema, tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5),'') as id_gerado
              FROM compras.itens_processo JOIN
                   compras.processos ON (itens_processo.i_ano_proc = processos.i_ano_proc and
                                itens_processo.i_processo = processos.i_processo and
                                itens_processo.i_entidades = processos.i_entidades) JOIN
                   compras.participantes_processos on (participantes_processos.i_ano_proc = processos.i_ano_proc and
                                              participantes_processos.i_processo = processos.i_processo and
                                              participantes_processos.i_entidades = processos.i_entidades) JOIN 
                   compras.forma_julg ON (processos.i_forma_julg = forma_julg.i_forma_julg and
                                       processos.i_entidades = forma_julg.i_entidades)
             WHERE bethadba.dbf_get_situacao_registro(sistema,tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5) in(5,3)
               and bethadba.dbf_get_situacao_registro(sistema,'processos-administrativo', chave_dsk1, chave_dsk2, chave_dsk3) in(4)
               and bethadba.dbf_get_situacao_registro(sistema,'participante-licitacao',chave_dsk1,chave_dsk2, chave_dsk3, chave_dsk4) in (4)
               and processos.i_entidades = {{entidade}}
               and forma_julg.forma_especial = 4
               and itens_processo.i_item NOT IN (SELECT p.i_item
                               FROM compras.participantes p
                                      WHERE p.i_entidades = participantes_processos.i_entidades
                                and p.i_ano_proc = participantes_processos.i_ano_proc
                                and p.i_processo = participantes_processos.i_processo
                                and p.i_credores = participantes_processos.i_credores)
        ORDER BY chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5) as tab where situacao in ('NAO_ANALISADO', 'EMPATE_REAL', 'CLASSIFICADO_LANCE')