select
		--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','lancamento-despesa',t2.veicodigo,upper(t2.veidescricao)))) as lancamentoDespesa,
		23001 as tipoDespesa,
		(select i_chave_dsk1 from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','materiais-servicos',upper(t2.prddescricao)))) as material,
		--(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','motoristas',upper(t4.uninomerazao),translate(t4.unicpfcnpj, './-', '')))) as materialEspecificacao,
		--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','lancamento-despesa',t2.veicodigo,upper(t2.veidescricao)))) as ordemAbastecimentoItem,
		--ROW_NUMBER() over() as numeroItem,
		t1.dspsequencia as numeroItem,
		false as tanqueCheio,
		(select sum(tx.idsquantidade) from wfm.tbitemdespesa tx where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata) as quantidadeItem,
		(select sum(tx.idsvalorunitario) from wfm.tbitemdespesa tx where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata) as valorUnitario,
		--'valorUnitario' as valorUnitario,
		--'valorTotal' as valorTotal,
		(select sum(tx.idsquantidade * tx.idsvalorunitario)
				from wfm.tbitemdespesa tx
			where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata) as valorTotal,
		--(select sum(tx.idsquantidade) from wfm.tbitemdespesa tx where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata),
		--(select sum(tx.idsquantidade * tx.idsvalorunitario)
			--	from wfm.tbitemdespesa tx
			--where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata) as valorTotalItensLancamentoDespesa,*/
		--sum(t7.idsquantidade) as quantidadeItensLancamentoDespesa--,
		--(sum(t7.idsquantidade) * sum(t7.idsvalorunitario)) as valorTotalItensLancamentoDespesa
		'2020' as exercicio
from wfm.tbitemdespesa t1
join wun.tbproduto t2 on t2.prdcodigo = t1.prdcodigo
where t1.dspdata >= '2020-01-01' and t1.dspdata < '2021-01-01';