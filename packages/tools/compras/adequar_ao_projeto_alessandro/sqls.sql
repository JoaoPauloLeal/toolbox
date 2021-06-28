# equivalencias grupo 19
busca_equivalencia_grupo_19 = '''
SELECT mascara, descricao, i_bancos_febraban, i_agencias, conta_banco FROM "sapo"."contas" 
where mascara like '1111119%' 
	and i_entidades = '3' 
	and tipo_conta = 'A' 
	and data_inativacao is null
ORDER BY mascara 
'''

# equivalencias grupo 50
busca_equivalencia_grupo_50 = '''
SELECT mascara, descricao, i_bancos_febraban, i_agencias, conta_banco FROM "sapo"."contas" 
where mascara like '111115%' 
	and i_entidades = '2' 
	and tipo_conta = 'A' 
	and data_inativacao is null
ORDER BY mascara 
'''

# retencoes
busca_retencoes = '''
select distinct contas.i_entidades, mascara, contas.descricao
 from sapo.contas key join sapo.lancamentos where ano_exerc = 2020 and contas.i_entidades = 2 and left(mascara,4) = 2188
order by contas.i_entidades, mascara
'''

# consulta na ocor
consulta_ocor = '''
SELECT * FROM bethadba.controle_migracao_registro_ocor 
WHERE id_gerado IS NULL 
    and resolvido = 1 
ORDER BY tipo_registro, mensagem_erro  
'''
# consulta migracao
consulta_registro = '''
SELECT * from bethadba.controle_migracao_registro cmr 
where  tipo_registro like 'processos%' and id_gerado is not NULL 
order BY i_chave_dsk2 
'''

# Processos com modalidade 'outras modalidades' e tipo do objeto diferente de chamamento publico ou credenciamento
busca_proc_adm_modal_obj = '''
select i_processo, i_ano_proc, tipo_objeto, objeto_proc , modalidade from compras.processos p 
where i_entidades = 1 
	and (modalidade = 99 and tipo_objeto not in (5,9))
'''
update_proc_adm_modal_obj = '''
update compras.processos  set tipo_objeto = 5 
WHERE modalidade = 99 and objeto_proc like 'CHAMAMENTO PÚBLICO OBJETIVANDO%'

UPDATE compras.processos set modalidade = 5 where i_ano_proc = 2016 and i_processo = 115

UPDATE compras.processos  set data_processo = '2017-01-02' where i_entidades = 1 and i_ano_proc = 2017 and i_processo in (1,2,3,4,5,6,7,8,9)
UPDATE compras.processos  set data_publicacao = '2017-01-02' where i_entidades = 1 and i_ano_proc = 2017 and i_processo in (1,2,3,4,5,6,7,8,9)

select data_processo , data_publicacao ,* from compras.processos p where i_ano_proc = 2017 and i_processo in (1,2,3,4,5,6,7,8,9) and i_entidades  = 1

SELECT * from compras.publicacoes p2 where i_ano_proc = 2017 and i_processo in (1,2,3,4,5,6,7,8,9) and i_entidades  = 1
update compras.publicacoes set data_publ = '2017-01-02', data_ratificacao = '2017-01-02' where i_ano_proc = 2017 and i_processo in (1,2,3,4,5,6,7,8,9) and i_entidades  = 1
'''