select DISTINCT tab.advogado, tab.identificacao_advogado, tab.tipo_responsavel 
from 
(SELECT   sapo.convenios.i_entidades ,
			sapo.convenios.i_convenio ,
			sapo.convenios.convenio_superior ,
			sapo.convenios.valor_convenio ,
			sapo.convenios.moeda ,
			sapo.convenios.data_assinatura ,
			sapo.convenios.objeto_convenio ,
			sapo.convenios.data_vencimento ,
			upper(sapo.convenios.advogado) as advogado,
			(select first isNull(cpf, cgc) from sapo.credores c where SOUNDEX(UPPER(nome)) = SOUNDEX(UPPER(sapo.convenios.advogado)) and (cpf is not null or cgc is not null)) as identificacao_advogado,
			(select first id_gerado from bethadba.controle_migracao_registro cmr where tipo_registro in ('tipo-responsavel') and i_chave_dsk2 = 'Advogado') as tipo_responsavel,
			sapo.convenios.lei_convenio ,
			sapo.convenios.data_lei_convenio ,
			sapo.convenios.numero_diarioof ,
			sapo.convenios.data_publicacao ,
			sapo.convenios.mesano ,
			sapo.convenios.tipo_convenio ,
			sapo.convenios.tipo ,
			sapo.convenios.arquivo_texto ,
			i_contas = (select convenios_contas.i_contas
							  from sapo.convenios_contas
							 where convenios_contas.i_entidades = convenios.i_entidades and
									 convenios_contas.i_convenio  = convenios.i_convenio and
                             convenios_contas.i_plano_contas = {{i_plano_contas}} ),
			desc_conta = (select contas.descricao
				  			   from sapo.contas, sapo.convenios_contas
							  where contas.i_contas = convenios_contas.i_contas and
									  contas.i_entidades = convenios_contas.i_entidades and
									  contas.i_plano_contas = convenios_contas.i_plano_contas and
									  convenios.i_entidades =	convenios_contas.i_entidades and
									  convenios.i_convenio  = convenios_contas.i_convenio and
                             convenios_contas.i_plano_contas = {{i_plano_contas}}	),
			sapo.convenios.esfera ,
			sapo.convenios.valor_contrapartida ,
			sapo.convenios.orgao_concedente ,
			sapo.convenios.beneficiario ,
			sapo.convenios.tipo_prestacao ,
			sapo.convenios.data_situacao ,
			sapo.convenios.i_rubricas ,
			sapo.convenios.portaria_rubr ,
			i_contas_extra = (select convenios_contas.i_contas_extra
							  from sapo.convenios_contas
							 where convenios_contas.i_entidades = convenios.i_entidades and
									 convenios_contas.i_convenio  = convenios.i_convenio and
                            convenios_contas.i_plano_contas = {{i_plano_contas}})  ,
			desc_conta_extra = (select contas.descricao
						  			   from sapo.contas, sapo.convenios_contas
									  where contas.i_contas = convenios_contas.i_contas_extra and
											  contas.i_entidades = convenios_contas.i_entidades and
											  contas.i_plano_contas = convenios_contas.i_plano_contas and
											  convenios.i_entidades =	convenios_contas.i_entidades and
											  convenios.i_convenio  = convenios_contas.i_convenio	and
											  convenios_contas.i_plano_contas = {{i_plano_contas}}),
			desc_rubrica = (select rubricas.descricao
									from sapo.rubricas
								  where rubricas.i_entidades = convenios.i_entidades and
										  rubricas.portaria_rubr = convenios.portaria_rubr and
										  rubricas.i_rubricas = convenios.i_rubricas),
			sapo.convenios.situacao ,
			sapo.convenios.i_unid_medidas ,
			sapo.convenios.numero_parcelas ,
			desc_unidade = (select unid_medidas.descricao
									from  sapo.unid_medidas
								  where unid_medidas.i_unid_medidas = convenios.i_unid_medidas),
			sapo.convenios.numero_processo,
			sapo.convenios.data_inicio,
			sapo.convenios.tipo_aditivo,
			sapo.convenios.natureza,
			sapo.convenios.numero_empenho,
 			sapo.convenios.convenio_obra,
 			sapo.convenios.i_funcoes,
  			sapo.convenios.portaria_func,
			desc_funcao = (select funcoes.descricao
									from  sapo.funcoes
								  where funcoes.i_entidades = convenios.i_entidades and
										  funcoes.i_funcoes = convenios.i_funcoes and
										  funcoes.portaria_func = convenios.portaria_func),
			sapo.convenios.convenio_tce,
 			sapo.convenios.aditivo_tce,
			--ano_exerc = sapo.dbf_conexaogetano (),
			sapo.convenios.arquivo_anexo,
         sapo.convenios.desc_adit,
         sapo.convenios.clausulas,
         sapo.convenios.orgao_repas_tce,
         sapo.convenios.num_transf_tce,
			sapo.convenios.i_recursos,
			recursos_descricao = (SELECT recursos.descricao
											FROM sapo.recursos
                                WHERE recursos.i_entidades = convenios.i_entidades
											 AND recursos.i_recursos = convenios.i_recursos ),
			sapo.convenios.codigo_sit,
			sapo.convenios.i_credores,
			nome_credor = (SELECT credores.nome
								  FROM sapo.credores
                         WHERE credores.i_entidades = convenios.i_entidades
									AND credores.i_credores = convenios.i_credores ),
			sapo.convenios.data_canc_aditivo,
			sapo.convenios.motivo_canc_aditivo,
			sapo.convenios.valor_meta_fisica,
			sapo.convenios.data_inclusao,
			sapo.convenios.cnpj_beneficiario,
			protege_convenio = 0,
			regerar_lanctos = 'N',
			sapo.convenios.atividade_principal,
		   sapo.convenios.tipo_contrapartida,
			sapo.convenios.contrapartida,
			sapo.convenios.i_gestor,
			nome_gestor = (select nome
								  from sapo.responsaveis
								 where responsaveis.i_entidades = convenios.i_entidades
									and responsaveis.i_responsaveis = convenios.i_gestor),
			sapo.convenios.i_dirigente,
			nome_dirigente = (select nome
								  from sapo.responsaveis
								 where responsaveis.i_entidades = convenios.i_entidades
									and responsaveis.i_responsaveis = convenios.i_dirigente),
		   sapo.convenios.valor_contrapartida_economica

  FROM sapo.convenios
 WHERE ( sapo.convenios.i_entidades = {{id_entidade}} )
 		and sapo.convenios.advogado is not null
ORDER BY sapo.convenios.i_entidades          ASC,
			sapo.convenios.i_convenio          ASC) as tab
where tab.identificacao_advogado is not null and
				bethadba.dbf_get_situacao_registro({{sistema}},
				'responsavel',
				{{id_entidade}},
				advogado,
				identificacao_advogado) in(5, 3)