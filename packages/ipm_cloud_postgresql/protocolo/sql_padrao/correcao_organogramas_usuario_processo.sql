select distinct t1.i_chave_dsk3, t1.i_chave_dsk5,
		t1.i_chave_dsk6 as id_organograma_andamento,
		t3.i_chave_dsk3 as dsk3, t3.i_chave_dsk2,
		t3.i_chave_dsk4,
		t3.id_gerado,
		t1.i_chave_dsk1, t3.id_gerado as id,
		t3.i_chave_dsk8 as id_organograma_usuario
from controle_migracao_auxiliar_bkp t1
join controle_migracao_auxiliar_andamentos t2 on t2.id_processo = t1.i_chave_dsk2 and t1.i_chave_dsk3 = t2.numero_processo
	and t2.ultima_data_andamentos::timestamp = t1.i_chave_dsk1::timestamp
join controle_migracao_auxiliar t3 on t3.id_gerado = t1.i_chave_dsk2
	and t3.i_chave_dsk4 != t1.i_chave_dsk6 -- id organograma
join controle_migracao_incidente_processos t4
on t4.numero_processo = t1.i_chave_dsk3 --compara com cadastro disponibilizados na planilha
where t1.tipo_registro like 'andamentos-busca-fonte-dados' --and t1.i_chave_dsk3 = '000001788/2002'
and t1.i_chave_dsk5 = 'Confirmado'
and t3.tipo_registro like 'processos-busca-fonte-dados'
order by t1.i_chave_dsk1