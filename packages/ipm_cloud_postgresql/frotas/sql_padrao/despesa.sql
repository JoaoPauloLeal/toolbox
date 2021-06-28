
select distinct
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','veiculo-equipamento',t2.veicodigo,upper(t2.veidescricao)))) as veiculoEquipamento,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','motoristas',upper(t4.uninomerazao),translate(t4.unicpfcnpj, './-', '')))) as motorista,
		--(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','funcionario',upper(t3.uninomerazao),translate(t3.unicpfcnpj, './-', '')))) as funcionario,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','fornecedores',upper(t4.uninomerazao),translate(t4.unicpfcnpj, './-', '')))) as fornecedor,
		--(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','ordem-abastecimento',upper(t4.cncdescricao),left(translate(t4.cncclassif, '.', '') || '000000000000000',14)))) as ordemAbastecimento,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','organogramas',upper(t6.cncdescricao),left(translate(t6.cncclassif, '.', '') || '000000000000000',14),
					(select id_gerado from public.controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('306','configuracoes-organogramas','CONFIGURAÇÃO ORGANOGRAMA 2021', '2021')))))) as organograma,
		'ordemAbastecimento' as ordemAbastecimento,
		t1.dspdata as dataLancamentoDespesa,
		t1.dspnumeronf as numeroLancamentoDespesa,
		t1.dspnumeronf as numeroDocumento,
		t1.dspsequencia as sequencia,
		'ESTOQUE' as origem,
		/*case
			when t1.autestoqueproprio = 1
				then 'ESTOQUE'
			when t3.autestoqueproprio = 0
				then 'LICITACAO'
			else null end as origem,*/
		false as controlaKmHoraMilha,
		--'qtdUnidadeOperacional' as qtdUnidadeOperacional,
		--'qtdUnidadeOperacionalHist' as qtdUnidadeOperacionalHist,
		t1.dspobservacao as observacao,
		case
			when t1.dspsituacao = 1
				then 'OK'
			when t1.dspsituacao = 0
				then 'PENDENTE'
			else 'MINIMO' end as situacao,
		t1.veicodigo as veicodigo,
		(select sum(tx.idsquantidade) from wfm.tbitemdespesa tx where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata),
		(select sum(tx.idsquantidade * tx.idsvalorunitario)
				from wfm.tbitemdespesa tx
			where tx.veicodigo = t1.veicodigo and tx.dspsequencia = t1.dspsequencia and tx.dspdata = t1.dspdata) as valorTotalItensLancamentoDespesa,
			--sum(t7.idsquantidade) as quantidadeItensLancamentoDespesa--,
		--(sum(t7.idsquantidade) * sum(t7.idsvalorunitario)) as valorTotalItensLancamentoDespesa
		'2020' as exercicio
from wfm.tbdespesa t1
	join wfm.tbveiculo t2 on t2.veicodigo = t1.veicodigo
	left join wfm.tbautorizacao t3 on t3.autnumero = t1.autnumero --and t3.autano = t1.autano and t3.clicodigo = t1.clicodigo a
	left join wun.tbunico t4 on t4.unicodigo = t1.unicodigo
	left join wun.tbcencus t6 on t1.cnccodigo = t6.cnccodigo
	--join wfm.tbitemdespesa t7 on t7.veicodigo = t1.veicodigo and t7.dspsequencia = t1.dspsequencia and t7.dspdata = t1.dspdata
where t1.dspdata >= '2020-01-01' and t1.dspdata < '2021-01-01'; -- and t1.dspnumeronf = 944;