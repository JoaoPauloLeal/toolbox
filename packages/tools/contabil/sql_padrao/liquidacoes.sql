update bethadba.controle_migracao_registro set id_gerado = 9999
where tipo_registro like 'liquidacoes'
and i_chave_dsk1 = {{i_chave_dsk1}}
and i_chave_dsk2 = {{i_chave_dsk2}}
and i_chave_dsk3 = {{i_chave_dsk3}}
--and i_chave_dsk4 = {{i_chave_dsk4}}
and i_chave_dsk5 = {{i_chave_dsk5}}
and i_chave_dsk6 = '{{i_chave_dsk6}}'
and id_gerado is null;