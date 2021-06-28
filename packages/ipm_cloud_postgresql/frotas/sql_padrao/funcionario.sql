select codigo, nome, tipoPessoa, cpfCnpj, admissao, datanascimento  from (

select t.unicodigorespaut as codigo,
		t2.uninomerazao as nome,
		CASE WHEN LENGTH(t2.unicpfcnpj::text) = 14
			then 'FISICA'
			else 'JURIDICA' end as tipoPessoa,
		translate(t2.unicpfcnpj, './-', '') as cpfCnpj,
		to_char(t4.fundataadmissao ::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as admissao,
		to_char(t6.unfdatanascimento::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as datanascimento
from wfm.tbautorizacao t
	join wun.tbunico t2 on t2.unicodigo = t.unicodigorespaut
	join wfp.tbfuncionario t3 on t3.unicodigo = t2.unicodigo
	join wfp.tbfuncontrato t4 on t4.unicodigo = t2.unicodigo
	join wun.tbunicofisica t6 on t6.unicodigo = t4.unicodigo
where
--t.autdata > '01-01-2019' and t.autdata < '01-01-2020'
t.autano = 2019
and t4.fundataadmissao=(select min(t5.fundataadmissao) from wfp.tbfuncontrato t5 where t5.unicodigo = t2.unicodigo )
--and (select id_gerado from controle_migracao_auxiliar aux where aux.hash_chave_dsk = md5(concat(306,'pessoas',upper(t2.uninomerazao),translate(t2.unicpfcnpj, './-', '')))) is not null
group by t.unicodigorespaut, t2.uninomerazao,t2.unicpfcnpj, t4.fundataadmissao, t6.unfdatanascimento

) as tab