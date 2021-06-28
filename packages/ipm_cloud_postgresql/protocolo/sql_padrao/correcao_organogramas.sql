select t1.id_gerado as id_gerado_processo, t2.id_gerado as id_gerado_organograma_usuario from public.controle_migracao_auxiliar t1
join public.controle_migracao_auxiliar_bkp t2 on t2.i_chave_dsk1 = t1.id_gerado 
where t1.tipo_registro like 'processos-extra' and t1.i_chave_dsk2 like 'ARQUIVADO'
and t2.tipo_registro like 'processo-organogramas-usuarios-extra' and t2.i_chave_dsk4 = upper('Gestão Documental')
