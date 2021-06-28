--BUSCA TODOS OS PROCESSO QUE ESTÃO UTILIZANDO A COMISSÃO VENCIDA
SELECT (SELECT FIRST r2.i_responsavel as nova_comissao from compras.responsaveis r2
		WHERE here r2.data_desig <= p.data_processo
			and r2.data_expira >= p.data_processo
			and r2.tipo_comissao = r.tipo_comissao), r.i_responsavel as comissao_antiga, r.tipo_comissao, r.data_desig, r.data_expira
FROM rom compras.processos p join compras.responsaveis r
	on p.i_responsavel = r.i_responsavel
WHERE r.data_expira < p.data_processo ;

--BUSCA TODOS OS PROCESSOS ONDE A CRONOLOGIA DE TRAMITES DOS ENVELOPES ESTÁ INCORRETA
SELECT * from compras.processos p
where data_entrega > data_recebimento  or data_recebimento > data_abertura
	if data_entrega = data_recebimento  THEN hora_entrega > hora_recebimento end if;