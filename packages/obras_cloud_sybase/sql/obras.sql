SELECT
       id = 1,
       i_entidades,
       i_obras,
	   B.id_gerado as categoria,
	   COALESCE(C.id_gerado, D.id_gerado) as tipo_obra,
	   D.id_gerado as teste,
	   classif_intervencao classificacao_id,
	   cast(data_cadastramento as varchar) datacadastro,
       descricao descricao,
	   dias_termino diastermino,
	   grau_latitude,
	   minutos_latitude,
	   segundos_latitude,
	   grau_longitude,
	   minutos_longitude,
	   segundos_longitude,
	   grau_latitude_f,
	   minutos_latitude_f,
	   segundos_latitude_f,
	   grau_longitude_f,
	   minutos_longitude_f,
	   segundos_longitude_f,
	   posicao_latitude_f,
	   posicao_latitude,
	   posicao_longitude_f,
	   posicao_longitude,
	   objeto,
	   CASE
	   	WHEN tipo_obra_empenho = 2 THEN 'PROJETO'
	   	WHEN tipo_obra_empenho = 1 THEN 'OBRA'
	   	ELSE 'NENHUM'
	   END obraprojeto,
	   ISNULL(quantidade, 0) as quantidade,
	   CASE
	   	WHEN tipo_execucao  = 1 THEN 'DIRETA'
	   	WHEN tipo_execucao  = 2 THEN 'INDIRETA'
	   	WHEN tipo_execucao  = 3 THEN 'DIRETA_INDIRETA'
	   	ELSE 'NENHUM'
	   END tipoexecucao_id,
	   i_unidades unidademedida_id,
	   data_conclusao
 FROM sapo.obras A
 LEFT JOIN bethadba.controle_migracao_registro B
    ON (A.categoria = B.i_chave_dsk2 and B.tipo_registro = 'categoria-obra')
 LEFT JOIN bethadba.controle_migracao_registro C
 	ON (A.i_tipo_obras = C.i_chave_dsk2 and C.tipo_registro = 'tipos-obra')
 INNER JOIN bethadba.controle_migracao_registro D
 	ON (D.i_chave_dsk3 = 'Outras' and D.tipo_registro = 'tipos-obra')
 --WHERE data_cadastramento >= '2019-01-01'
   --AND data_cadastramento <= '2019-12-31'
   WHERE i_entidades = {{entidade}}
 ORDER BY data_cadastramento ASC