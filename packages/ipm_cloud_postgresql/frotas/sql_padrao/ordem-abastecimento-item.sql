
select *
from (
	select
		'123' as ordemAbastecimento, --(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','ordem-abastecimento',t2.veicodigo,upper(t2.veidescricao)))) as ordemAbastecimento,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','materiais-servicos',upper(t4.prddescricao)))) as material,
		' teste' as materialEspecificacao, --(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','funcionario',upper(t4.prddescricao), (select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','materiais-servicos',upper(t4.prddescricao))))))) as materialEspecificacao,
		ROW_NUMBER() over() as numeroItem,
		t1.autquantidade as quantidadeItem,
		false as completarTanque,
		case
			when t1.auttipo = 1
				then '23001'
			else '23002' end as tipo,
		t1.autano as exercicio
	from wfm.tbautorizacao t1
		--left join wun.tbunico t3 on t3.unicodigo = t1.unicodigorespaut
		join wun.tbunipro t5 on t5.prdcodigo = t1.prdcodigo and t1.cnicodigo = t5.cnicodigo
		join wun.tbproduto t4 on t4.prdcodigo = t5.prdcodigo
		/*left join wun.tbcencus t4 on t1.cnccodigo = t4.cnccodigo and t4.organo = 2021
		left join wco.tbcadfor t5 on t5.unicodigo = t1.unicodigoforn
		left join wun.tbunico t6 on t5.unicodigo = t6.unicodigo*/
	where t1.autano = 2020
) as tab; --limit 1;