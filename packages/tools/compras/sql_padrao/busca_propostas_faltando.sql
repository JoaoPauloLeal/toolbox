SELECT DISTINCT i_entidades entidade ,
	i_ano_proc ano ,
	i_processo processo,
	i_credores credor,
	COUNT(i_item) total_proposta,
	(SELECT count(i_item)
	 FROM compras.itens_processo ip
	 WHERE ip.i_ano_proc = p.i_ano_proc
	 	and ip.i_processo = p.i_processo
	 	and ip.i_entidades = p.i_entidades) total_itens,
	(total_itens - total_proposta) as total
FROM compras.participantes p
WHERE i_entidades = {{entidade}}
GROUP by p.i_ano_proc, p.i_processo, p.i_credores, p.i_entidades
HAVING total > 0
order by i_ano_proc, i_processo, i_credores 