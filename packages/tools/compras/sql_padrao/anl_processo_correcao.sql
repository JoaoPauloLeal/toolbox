----------------------------ANULAÇÃO DE PROCESSOS SEM RESPONSAVEL-------------------------------------------------------
BEGIN
	DECLARE ano int;
	DECLARE proce int;

    FOR roda2 AS cotia2 DYNAMIC SCROLL CURSOR FOR

    	SELECT * from compras.anl_processos t1 where t1.i_responsaveis_atos is NULL
    DO

    SET ano = i_ano_proc ;
   	SET proce = i_processo ;

	update compras.anl_processos anl SET i_responsaveis_atos = (
		SELECT FIRST ra.i_responsaveis_atos
		FROM compras.responsaveis_atos ra
			JOIN compras.responsaveis r ON UPPER(ra.nome) = UPPER(r.presid_comissao) AND ra.i_entidades = r.i_entidades AND ra.cpf = r.cpf_presid
			JOIN compras.processos p ON r.i_responsavel = p.i_responsavel AND r.i_entidades = p.i_entidades
			JOIN compras.anl_processos ap ON p.i_ano_proc = ap.i_ano_proc AND p.i_processo = ap.i_processo AND p.i_entidades = ap.i_entidades
		WHERE ap.i_responsaveis_atos IS NULL AND ap.i_ano_proc = ano AND ap.i_processo = proce)

    END FOR;
END;
