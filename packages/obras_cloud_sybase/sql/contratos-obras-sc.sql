SELECT id = 1,
	   os.i_entidades,
	   cmr.id_gerado as i_obras ,
	   cast (os.mesano as varchar) as data_inclusao,
	   charindex('TAC',os.contrato) as ee,
	   (if charindex('TAC',os.contrato) > 0 then
	   SUBSTRING(os.contrato,charindex('TAC',os.contrato)+3)
	   else
	   SUBSTRING(os.contrato,charindex('TAC',os.contrato))
	   endif) as numero_contrato
FROM sapo.obras_sit os 
JOIN bethadba.controle_migracao_registro cmr on (cmr.i_chave_dsk2 = os.i_obras and i_chave_dsk1 = os.i_entidades and cmr.tipo_registro = 'obras')
where contrato is not null 
and contrato != '0' and i_entidades = {{entidade}}