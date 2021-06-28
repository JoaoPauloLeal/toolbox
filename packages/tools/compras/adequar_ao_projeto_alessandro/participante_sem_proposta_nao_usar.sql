begin
    declare comando long varchar;
    declare ano integer;
	declare entidade integer;
	declare processo integer;

	set ano = 2017; -- alterar
	set entidade = 1; -- alterar
	set processo = 110; -- alterar

    call bethadba.pg_habilitartriggers('off');
    call bethadba.pg_setoption('wait_for_commit','on');

    for aa as lc_tables1 dynamic scroll cursor for

	select distinct i_credores as w_credor from compras.participantes cp
	where cp.i_ano_proc = ano and cp.i_processo = processo and cp.i_entidades = entidade
	and (select count(*) from compras.itens_processo where i_ano_proc = cp.i_ano_proc and i_processo = cp.i_processo and i_entidades = cp.i_entidades) >
	(select count(*) from compras.participantes cp1 where cp1.i_ano_proc = cp.i_ano_proc and cp1.i_processo = cp.i_processo and cp1.i_entidades = cp.i_entidades and cp1.i_credores = cp.i_credores)

    do
		for ee as lc_tables2 dynamic scroll cursor for

		select ip.i_item as w_item, ip.preco_max as w_preco from compras.itens_processo ip
		where ip.i_ano_proc = ano and ip.i_processo = processo and ip.i_entidades = entidade
		and ip.i_item not in (select cp.i_item from compras.participantes cp where cp.i_ano_proc = ip.i_ano_proc and cp.i_processo = ip.i_processo and cp.i_entidades = ip.i_entidades and cp.i_credores = w_credor)

		do
		   set comando = 'INSERT INTO compras.participantes (i_ano_proc,i_processo,i_credores,i_item,preco_unit_part,preco_total,qtde_cotada,situacao,atual_objeto,nome_marca,ordem_clas,credenciado,vlr_descto,i_entidades,i_lotes,art_43_lcf_123_06,art_44_lcf_123_06,dt_descredencia,observacao,percent_bdi_tce,percent_encargo_tce) VALUES('||ano||','||processo||','||w_credor||','||w_item||','||w_preco||',0.00,0.000,4,NULL,NULL,1,1,0.0000,'||entidade||',NULL,''A'',''A'',NULL,NULL,NULL,NULL);';
		   message comando to client;
		   execute immediate comando;
		end for;
    end for;
end;
