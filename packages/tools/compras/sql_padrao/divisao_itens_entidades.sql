SELECT DISTINCT * from (
	SELECT i_ano_proc ,
		i_processo ,
		i_item ,
		qtd_estimada as qtd_por_entidade,
		(SELECT sum(qtd_estimada)
		 from compras.processos_entidades_partic_itens pepi2
		 where i_ano_proc= pepi.i_ano_proc
		 	and i_processo = pepi.i_processo
		 	and i_item = pepi.i_item
		 	and i_entidades = pepi.i_entidades) as total_div ,
		(SELECT qtde_item
		 from compras.itens_processo ip
		 where i_ano_proc= pepi.i_ano_proc
		 	and i_processo = pepi.i_processo
		 	and i_item = pepi.i_item
		 	and i_entidades = pepi.i_entidades) as qtd_proc
from compras.processos_entidades_partic_itens pepi
where  i_entidades = 1) as tab
where total_div <> qtd_proc;
