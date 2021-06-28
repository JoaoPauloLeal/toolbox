 SELECT estcodigo as key1,
	   estcodigo as id,
	   37 as idpais,
	   estnome as nome,
	   estsigla as sigla,
	   public.bth_get_codigo_ibge(estsigla) as codigo_ibge
  FROM wun.tbestado
 WHERE paisiglaiso = 'BR'
