SELECT * from compras.participantes p
    insert into compras.participantes
        select
            i_ano_proc,
            i_processo,
            {{credor}},		--i_credores
            i_item,
            0,			--preco_unit_part
            0,			--preco_total
            qtde_item,
            4,			--situacao
            null,		--atual_objeto
            '',			--nome_marca
            0,			--ordem_clas
            1,			--credenciado
            0,			--vlr_descto
            i_entidades,
            null,		--i_lotes
            'A',		--art_43_lcf_123_06
            'A',		--art_44_lcf_123_06
            null,		--dt_descredencia
            '',			--observacao
            null,		--percent_bdi_tce
            null		--percent_encargo_tce
        from compras.itens_processo
        where i_ano_proc = {{ano_processo}}
            and i_processo = {{processo}}
            and i_item not in (
                select i_item
                from compras.participantes
                where i_item = itens_processo.i_item
                and i_ano_proc = itens_processo.i_ano_proc
                and i_processo = itens_processo.i_processo
                and i_entidades = itens_processo.i_entidades
                and i_credores = {{credor}});