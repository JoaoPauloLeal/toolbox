
select
	tab.veiculoEquipamento,
	coalesce(tab.motorista::text,tab.funcionario::text,'007') as motorista,
	--tab.motorista,
	coalesce(tab.funcionario::text,tab.motorista::text,'007') as funcionario,
	--tab.funcionario,
	tab.dataOrdem,
	ROW_NUMBER() over() as numeroOrdem,
	--tab.numeroOrdem,
	--tab.codigoDespesa,
	coalesce(tab.organograma::text,null) as organograma,
	tab.tipo,
	coalesce(tab.origem::text,'007') as origem,
	tab.situacao,
	tab.observacao,
	tab.numeroDocto,
	tab.situacaoCadastral,
	tab.exercicio
from (
	select
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','veiculo-equipamento',t2.veicodigo,upper(t2.veidescricao)))) as veiculoEquipamento,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','motoristas',upper(t3.uninomerazao),translate(t3.unicpfcnpj, './-', '')))) as motorista,
		(select id_gerado from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','funcionario',upper(t3.uninomerazao),translate(t3.unicpfcnpj, './-', '')))) as funcionario,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','fornecedores',upper(t6.uninomerazao),translate(t6.unicpfcnpj, './-', '')))) as fornecedor,
--		t1.autdata 	as dataOrdem,
		to_char(t1.autdata::timestamp at time zone 'UTC', 'YYYY-MM-DD') as dataOrdem,
		t1.autnumero as numeroOrdem,
		--t1.depcodigodep as codigoDespesa,
		t4.cncclassif as classifi,
		t4.cncdescricao as descricao,
		--(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','organogramas',upper(t4.cncdescricao),left(translate(t4.cncclassif, '.', '') || '000000000000000',14)))) as organograma,
		(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','organogramas',upper(t4.cncdescricao),left(translate(t4.cncclassif, '.', '') || '000000000000000',14),
					(select id_gerado from public.controle_migracao_auxiliar cma where hash_chave_dsk = md5(concat('306','configuracoes-organogramas','CONFIGURAÇÃO ORGANOGRAMA ano_extracao', 'ano_extracao')))))) as organograma,
		t1.cnccodigo as organograma_ipm,
		case
			when t1.auttipo = 1
				then 'ABASTECIMENTO'
			else 'S' end as tipo,
		case
			when t1.autestoqueproprio = 1
				then 'ESTOQUE'
			when t1.autestoqueproprio = 1
				then 'L'
			else null end as origem,
		case
			when t1.depcodigodep notnull
				then 'ATENDIDA'
			else 'PENDENTE' end as situacao,
		cast(t1.autobservacao as varchar(999)) as observacao,
		t1.autnumero as numeroDocto,
		'OK' as situacaoCadastral,
		t1.autano as exercicio
	from wfm.tbautorizacao t1
		join wfm.tbveiculo t2 on t1.veicodigo = t2.veicodigo
		left join wun.tbunico t3 on t3.unicodigo = t1.unicodigorespaut
		left join wun.tbcencus t4 on t1.cnccodigo = t4.cnccodigo --and t4.organo = t1.autano
		left join wco.tbcadfor t5 on t5.unicodigo = t1.unicodigoforn
		left join wun.tbunico t6 on t5.unicodigo = t6.unicodigo
	where t1.autano = ano_extracao
	--limit 1
) as tab; --limit 1;