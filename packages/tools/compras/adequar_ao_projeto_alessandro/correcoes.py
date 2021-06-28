import lixo.comun.funcions_comuns as func


def materiais():
    banco = 'comprasSL'
    sql_consulta = '''
    SELECT count(i_material) FROM compras.material m 
    WHERE mat_serv = 'S' and estocavel = 'S' and tipo_mat = 'C'
    '''
    sql_correcao = '''
    UPDATE compras.material SET estocavel = 'N' 
    WHERE mat_serv = 'S' and estocavel = 'S' and tipo_mat = 'C'
    '''
    func.conecta_odbc(banco=banco)
    resp = func.sql(sql=sql_consulta)
    for x in resp:
        print('Existem {} materias que estão incorretos'.format(x[0]))
        if x[0] > 0:
            func.sql(sql=sql_correcao)
        return True

def processo_adm_sem_homolog_sem_anl(**kwargs):
    func.conecta_odbc(banco=kwargs.get('banco'))
    sql = '''
    SELECT i_processo , i_ano_proc from compras.processos p 
    where data_homolog is NULL 
	    and i_processo not in (SELECT i_processo from compras.anl_processos ap)
	order by i_ano_proc 
    '''
    resp = func.sql(sql=sql)
    for x in resp:
        print('{}/{} - Processo sem data de homologação e sem anulação'.format(x[0], x[1]))

def processo_adm_modalidade_objeto(**kwargs):
    func.conecta_odbc(banco=kwargs.get('banco'))
    '''          Modalidade                                        Tipo do Objeto
    1-Convite p/ compras e serviços,                    | 1-Mat. e Serv.,
    2-Convite p/ obras e serv. engenharia,              | 2-Obras e Serv. Eng.,
    3-Tomada de preço p/ compras e serviços,            | 3-Conc. e Perm.,
    4-Tomada de preço p/obras e serv.engenharia,        | 4-Alienação de Bens,
    5-Concorrência p/ compras e serviços,               | 5-Edit. de Chamamento/Cred.,
    6-Concorrência p/ obras e serv. engenharia,         | 6-Permissões,
    7-Leilão,                                           | 7-Locação de Imóveis,
    8-Dispensa de licitação p/ compras e serviços,      | 8-Cessão de Direitos,
    9-Inexigibilidade de licitação,                     | 9-Chamamento Público Parceria com OSS,
    10-Concurso,                                        | 10-Parceria com OSCIP,
    11-Dispensa de licitação p/ obras e serv.engenh.,   | 11-Procedimento de PMI ou MIP,
    12-Concorrência p/ alienação,                       | 12-Celebração de PPP
    13-Pregão presencial,
    14-Pregão eletrônico,
    15-Regime Diferenciado de Contratação - RDCação de PPP
    99-Outras modalidades,
    
    OBS.: Os tipos de objeto 6 e 7 só estarão visíveis para o 
 estado de MG. Também para o estado de MG o item 3-Concessão 
 e Permissão será alterado para 3-Concessão. O item 8 somente 
 estará visível para o estado do PR. Os itens 9, 10, 11 e 12 serão 
 visíveis somente para o estado de MT. 13-Compras p/ obras e/ou 
 serviços de engenharia
    '''
    sql = '''
    select i_processo, i_ano_proc, tipo_objeto, modalidade, objeto_proc  from compras.processos p 
    where i_entidades = 1 
	    and (modalidade = 99 and tipo_objeto not in (5,9))
    '''
    resp = func.sql(sql=sql)
    for x in resp:
        print('{}/{} modalidade "Outras Modalidades" porém o tipo de objeto está incorreto'.format(x[0], x[1]))

def comissao_licitacao(**kwargs):
    func.conecta_odbc(kwargs.get('banco'))
    sql = '''
    BEGIN
        DECLARE ano_proc varchar(100);
        DECLARE num_proc varchar (100);
   
        FOR roda2 AS cotia2 DYNAMIC SCROLL CURSOR FOR
    
		    SELECT p.i_ano_proc , p.i_processo , p.data_abertura , r.data_expira 
 		    from compras.processos p 
			    join compras.responsaveis r 
				    on p.i_responsavel = r.i_responsavel 
    	    where p.data_abertura > r.data_expira and i_ano_proc > 2015
    	
            DO
            SET ano_proc = i_ano_proc;
            SET num_proc = i_processo;
    	
   		    --MESSAGE '' to client
            update compras.processos set i_responsavel = (SELECT FIRST i_responsavel from compras.responsaveis r where data_expira >=  GETDATE() and tipo_comissao = 'G' order by data_desig) where i_ano_proc = ano_proc  and i_processo =  num_proc and i_entidades = 1 and modalidade = 13 
        END FOR;
    END;
    '''
    resp = func.sql(sql=sql)

def data_recebimento_envelope(**kwargs):
    msg = 'A data de início de recebimento dos envelopes não pode ser menor que a data do processo!'
    func.conecta_odbc(banco=kwargs.get('banco'))
    sql_ocor = """
                SELECT * FROM bethadba.controle_migracao_registro_ocor 
                WHERE id_gerado IS NULL 
                    and resolvido = 1 
                ORDER BY tipo_registro
        """
    result = func.sql(sql=sql_ocor, banco=kwargs.get('banco'))
    for x in result:
        if x[10] == msg:
            # print(x[11])
            processo = x[11]
            num = processo[41:processo.index('/')]
            ano = processo[processo.index('/')+1:processo.index('/')+5]
            # print('enviado para correção...{} / {}'.format(num, ano))
            sql = 'update compras.processos  set data_entrega = data_recebimento  where i_entidades = 1 and i_ano_proc  = {} and i_processo  = {}'.format(ano, num)
            print(sql)



        # else:
            # print('outra necessidade de correção')

