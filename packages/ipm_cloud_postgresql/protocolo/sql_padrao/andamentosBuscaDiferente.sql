--select id_gerado as id_gerado from public.controle_migracao_auxiliar_bkp
--where tipo_registro like 'andamentos-fonte-dados-atual'

select i_chave_dsk3 as numero from public.controle_migracao_auxiliar_bkp
where tipo_registro like 'andamentos-fonte-dados-atual' group by i_chave_dsk3