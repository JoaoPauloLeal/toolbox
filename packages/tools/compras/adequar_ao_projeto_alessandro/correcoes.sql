-- correção da data do processo ser menor que o exercicio
UPDATE compras.processos set data_processo = CAST(i_ano_proc as char )+'-01-02' WHERE DATEFORMAT(data_processo,'yyyy') < i_ano_proc  ;

--FAZ UM LOOP COM O SELECT
BEGIN
	--DECLARAÇÃO DE VARIAVEIS
	DECLARE ano int;
	DECLARE proce int;
	DECLARE data_proc char(10);
    DECLARE entidade int;

    FOR roda2 AS cotia2 DYNAMIC SCROLL CURSOR FOR
    --FAZ O SELECT NA BASE PRA BUSCAR OS RECULTADOS
    	SELECT * from compras.processos p where data_recebimento < data_processo
    DO
    --ATRIBUI OS VALORES AS VARIAVEIS QUE FORAM BUSCADAS NO SELECT
    SET ano = i_ano_proc ;
   	SET proce = i_processo ;
   	SET data_proc = data_processo;
   	SET entidade = i_entidades;
    --EXECUTA A FUNÇÃO QUE TIVER AQUI EM BAIXO
    --update compras.processos SET data_recebimento = DATEADD(dd, 1, data_proc) where i_ano_proc =  ano and i_processo =  proce and i_entidades = entidade
	--MESSAGE 'update compras.processos SET data_recebimento = ' , DATEADD(dd, 1, data_proc), ' where i_ano_proc = ', ano, ' and i_processo = ', proce, ' and i_entidades = ', entidade  to client

    END FOR;
END;