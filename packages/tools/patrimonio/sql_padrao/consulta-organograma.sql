select * from bethadba.controle_migracao_registro cmr
where tipo_registro = 'organograma'
and i_chave_dsk1 = {{entidade}}
order by char_length(i_chave_dsk3) desc