SELECT distinct *
  FROM (
        SELECT t.unicodigo as key1,
               t.unicodigo as id,
               t.uninomerazao as nome,
               case t.unitipopessoa
                when 2 then 'JURIDICA'
                else 'ESTRANGEIRA'
               end as tipo_pessoa,
               translate(t.unicpfcnpj, './-', '') as cpf_cnpj,
               case t.unisituacao
                    when 0 then false
                    when 5 then false
                    else true
               end as ativo,
               t.uninomefantasia as nome_fantasia,
               t.unirgie as inscricao_estadual

          FROM wun.tbunico t join wun.tbunicojuridica t2 on t.unicodigo = t2.unicodigo

        union all

            SELECT t.unicodigo as key1,
                   t.unicodigo as id,
                   t.uninomerazao as nome,
                   case t.unitipopessoa
                    when 1 then 'FISICA'
                    when 2 then 'JURIDICA'
                    else 'ESTRANGEIRA'
                   end as tipo_pessoa,
                   translate(t.unicpfcnpj, './-', '') as cpf_cnpj,
                   case t.unisituacao
                        when 0 then false
                        when 5 then false
                        else true
                   end as ativo,
                   t.uninomefantasia as nome_fantasia,
                   t.unirgie as inscricao_estadual
              FROM wun.tbunico t
             where not exists (select 1 from wun.tbunicojuridica t2
                                where t2.unicodigo = t.unicodigo )
               and not exists( select 1 from wun.tbunicofisica t3
                                where t3.unicodigo = t.unicodigo )
               and exists(select 1 from wpt.tbrequerente t4
                            where t4.unicodigo = t.unicodigo )
    ) AS tb2
    WHERE public.bth_get_situacao_registro('304', 'pessoaJuridica', cast(tb2.key1 as varchar)) in (0)
