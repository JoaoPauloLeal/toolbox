
select t1.veicodigo as id,
	t1.veicodigo as codigo,
	coalesce(t1.veiplaca,cast(ROW_NUMBER() over() as varchar)) as placa,
	t1.veidescricao as descricao,
	false as agregado,
	case
		when t1.veidataaquisicao is null
			then t1.veidataaquisicao
		else null
		end as dataAquisicao,
	'BOM' as estadoConservacao,
	t1.veinropassageiro as numeroPassageiros,
	coalesce((select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','tipos-veiculo-equipamento', upper((select t2.tipdescricao from wfm.tbtipo t2 where t2.tipcodigo = t1.tipcodigo))))),'7') as tipoVeiculoEquipamento,
	coalesce((select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','modelo-veiculo',upper(t5.moddescricao),right('0000000'||t5.modcodigofipe,7)))), '7231') as modeloVeiculo,
	t1.veirenavan as renavam,
	t1.veichassi as chassi,
	t1.veinumeroserie as numeroSerie,
	t1.veianofabricacao as anoFabricacao,
	t1.veianomodelo as anoModelo,
	'K' as marcacao,
	6 as digitosMarcador,
	(SELECT t3.cordescricao FROM wun.tbcor t3 WHERE t3.corcodigo = t1.corcodigo ) as cor,
	--t1.veipotenciamotor as potencia,
	case
		when t1.carcodigo = 1  -- PROPRIO, CEDIDO, LOCADO, OUTROS
			then 'TRACAO'
		when t1.carcodigo = 2
			then 'CARGA'
		when t1.carcodigo = 3
			then 'PASSAGEIROS'
		else 'OUTROS'
		end as classificacao,
	case
		when t1.veitipoaquisicao = 1  -- PROPRIO, CEDIDO, LOCADO, OUTROS
			then 'PROPRIO'
		when t1.veitipoaquisicao = 3
			then 'CEDIDO'
		when t1.veitipoaquisicao = 4
			then 'LOCADO'
		else 'OUTROS'
		end as vinculo,
	case
		when t1.veisituacao = 1
			then true
		when t1.veisituacao = 2
			then false
		end as ativo,
	(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','organogramas',upper(t4.cncdescricao),left(translate(t4.cncclassif, '.', '') || '000000000000000',14)))) as idorganograma
	from wfm.tbveiculo t1
		join wfm.tbtransferencia t3 on t1.veicodigo = t3.veicodigo and t3.clicodigo = t1.clicodigo and t3.tracodigo = t1.tracodigo
	    join wun.tbcencus t4 on t3.cnccodigo = t4.cnccodigo
	    left join wco.tbmodelomarca t5 on t1.modcodigo = t5.modcodigo
	    --join wfm.tbcarga t5 on t5.carcodigo = t1.carcodigo
	where --t1.veiplaca like 'MDJ-8736' and
	--t1.veiplaca not in('QJP-2788','214E','120B','MDJ-8736') and
	(select id_gerado from public.controle_migracao_auxiliar where hash_chave_dsk = md5(concat('306','organogramas',upper(t4.cncdescricao),left(translate(t4.cncclassif, '.', '') || '000000000000000',14)))) is not null
     and not exists (select 1 from public.controle_migracao_registro where hash_chave_dsk = md5(concat('306','veiculo-equipamento',t1.veicodigo,upper(t1.veidescricao))))

