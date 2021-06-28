SELECT t.unicodigo as key1,
	   t.unicodigo as id,
	   t.uninomerazao as nome,
   	   case t.unitipopessoa
	   	when 1 then 'FISICA'
	   	else 'ESTRANGEIRA'
	   end as tipo_pessoa,
	   translate(t.unicpfcnpj, './-', '') as cpf_cnpj,
	   case t.unisituacao
            when 0 then false
            when 5 then false
            else true
	   end as ativo,
	   t.uninomecivil as nome_social,
	   case t2.unfsexo
            when 2 then 'FEMININO'
            else 'MASCULINO'
	   end as sexo,
	   case t2.unfestadocivil
            when 1 then 'SOLTEIRO'
            when 2 then 'CASADO'
            when 3 then 'CONCUBINATO'
            when 4 then 'DIVORCIADO'
            when 5 then 'VIUVO'
            when 6 then 'UNIAO_ESTAVEL'
            when 7 then 'SEPARADO_JUDICIALMENTE'
            else 'SEPARADO_CONSENSUALMENTE'


--[ CASADO, CONCUBINATO, DIVORCIADO, SEPARADO_CONSENSUALMENTE, SEPARADO_JUDICIALMENTE, SOLTEIRO, UNIAO_ESTAVEL, VIUVO ]
	   end as estado_civil

  FROM wun.tbunico t join wun.tbunicofisica t2 on t.unicodigo = t2.unicodigo
