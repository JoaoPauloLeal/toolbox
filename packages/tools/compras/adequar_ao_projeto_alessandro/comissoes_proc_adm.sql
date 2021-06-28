------------------------------------Comissão com mesmo membro em mais de um cargo------------------------------------
--CONSULTA
SELECT * from compras.responsaveis r
where nome_resp_setor in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_diretor, nome_secret)
	or nome_diretor in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_resp_setor, nome_secret)
	or nome_secret in (membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_resp_setor, nome_diretor);

--UPDATE
UPDATE compras.responsaveis
SET nome_resp_setor = NULL, matr_resp_compras = NULL
WHERE i_responsavel  in (
    SELECT i_responsavel FROM compras.responsaveis r
	WHERE nome_resp_setor in	(
	    membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_diretor, nome_secret
	)
);

UPDATE compras.responsaveis
SET nome_diretor = NULL, matr_diretor = NULL, descr_diretor = NULL
WHERE i_responsavel in (
	SELECT i_responsavel FROM compras.responsaveis r
	WHERE nome_diretor in (
	    membro_com1, membro_com2, membro_com3, membro_com4, membro_com5, membro_com6, membro_com7, membro_com8, nome_resp_setor, nome_secret
	)
);
---------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------Data de publicação da comissão invalida------------------------------------------------------------------------------------
--CONSULTA
SELECT * FROM compras.responsaveis
WHERE data_publ is null;

--UPDATE
UPDATE compras.responsaveis
SET data_publ = data_desig
WHERE data_publ is NULL;

