select descricao,
                chave_dsk1
           into #TempTiposLogradouros
           from (
                 select 'Acesso'               as descricao, '1' as chave_dsk1 from dummy union all
                 select 'Adro'                 as descricao, '2' as chave_dsk1 from dummy union all
                 select 'Alameda'              as descricao, '3' as chave_dsk1 from dummy union all
                 select 'Alto'                 as descricao, '4' as chave_dsk1 from dummy union all
                 select 'Atalho'               as descricao, '5' as chave_dsk1 from dummy union all
                 select 'Avenida'              as descricao, '6' as chave_dsk1 from dummy union all
                 select 'Balneário'            as descricao, '7' as chave_dsk1 from dummy union all
                 select 'Belvedere'            as descricao, '8' as chave_dsk1 from dummy union all
                 select 'Beco'                 as descricao, '9' as chave_dsk1 from dummy union all
                 select 'Bloco'                as descricao, '10' as chave_dsk1 from dummy union all
                 select 'Bosque'               as descricao, '11' as chave_dsk1 from dummy union all
                 select 'Boulevard'            as descricao, '12' as chave_dsk1 from dummy union all
                 select 'Baixa'                as descricao, '13' as chave_dsk1 from dummy union all
                 select 'Cais'                 as descricao, '14' as chave_dsk1 from dummy union all
                 select 'Caminho'              as descricao, '15' as chave_dsk1 from dummy union all
                 select 'Chapadão'             as descricao, '16' as chave_dsk1 from dummy union all
                 select 'Conjunto'             as descricao, '17' as chave_dsk1 from dummy union all
                 select 'Colônia'              as descricao, '18' as chave_dsk1 from dummy union all
                 select 'Corredor'             as descricao, '19' as chave_dsk1 from dummy union all
                 select 'Campo'                as descricao, '20' as chave_dsk1 from dummy union all
                 select 'Córrego'              as descricao, '21' as chave_dsk1 from dummy union all
                 select 'Desvio'               as descricao, '22' as chave_dsk1 from dummy union all
                 select 'Distrito'             as descricao, '23' as chave_dsk1 from dummy union all
                 select 'Escada'               as descricao, '24' as chave_dsk1 from dummy union all
                 select 'Estrada'              as descricao, '25' as chave_dsk1 from dummy union all
                 select 'Estação'              as descricao, '26' as chave_dsk1 from dummy union all
                 select 'Estádio'              as descricao, '27' as chave_dsk1 from dummy union all
                 select 'Favela'               as descricao, '28' as chave_dsk1 from dummy union all
                 select 'Fazenda'              as descricao, '29' as chave_dsk1 from dummy union all
                 select 'Ferrovia'             as descricao, '30' as chave_dsk1 from dummy union all
                 select 'Fonte'                as descricao, '31' as chave_dsk1 from dummy union all
                 select 'Feira'                as descricao, '32' as chave_dsk1 from dummy union all
                 select 'Forte'                as descricao, '33' as chave_dsk1 from dummy union all
                 select 'Galeria'              as descricao, '34' as chave_dsk1 from dummy union all
                 select 'Granja'               as descricao, '35' as chave_dsk1 from dummy union all
                 select 'Ilha'                 as descricao, '36' as chave_dsk1 from dummy union all
                 select 'Jardim'               as descricao, '37' as chave_dsk1 from dummy union all
                 select 'Ladeira'              as descricao, '38' as chave_dsk1 from dummy union all
                 select 'Largo'                as descricao, '39' as chave_dsk1 from dummy union all
                 select 'Lagoa'                as descricao, '40' as chave_dsk1 from dummy union all
                 select 'Loteamento'           as descricao, '41' as chave_dsk1 from dummy union all
                 select 'Morro'                as descricao, '42' as chave_dsk1 from dummy union all
                 select 'Monte'                as descricao, '43' as chave_dsk1 from dummy union all
                 select 'Paralela'             as descricao, '44' as chave_dsk1 from dummy union all
                 select 'Passeio'              as descricao, '45' as chave_dsk1 from dummy union all
                 select 'Pátio'                as descricao, '46' as chave_dsk1 from dummy union all
                 select 'Praça'                as descricao, '47' as chave_dsk1 from dummy union all
                 select 'Parada'               as descricao, '48' as chave_dsk1 from dummy union all
                 select 'Praia'                as descricao, '49' as chave_dsk1 from dummy union all
                 select 'Prolongamento'        as descricao, '50' as chave_dsk1 from dummy union all
                 select 'Parque'               as descricao, '51' as chave_dsk1 from dummy union all
                 select 'Passarela'            as descricao, '52' as chave_dsk1 from dummy union all
                 select 'Passagem'             as descricao, '53' as chave_dsk1 from dummy union all
                 select 'Ponte'                as descricao, '54' as chave_dsk1 from dummy union all
                 select 'Quadra'               as descricao, '55' as chave_dsk1 from dummy union all
                 select 'Quinta'               as descricao, '56' as chave_dsk1 from dummy union all
                 select 'Rua'                  as descricao, '57' as chave_dsk1 from dummy union all
                 select 'Ramal'                as descricao, '58' as chave_dsk1 from dummy union all
                 select 'Recanto'              as descricao, '59' as chave_dsk1 from dummy union all
                 select 'Retiro'               as descricao, '60' as chave_dsk1 from dummy union all
                 select 'Reta'                 as descricao, '61' as chave_dsk1 from dummy union all
                 select 'Rodovia'              as descricao, '62' as chave_dsk1 from dummy union all
                 select 'Retorno'              as descricao, '63' as chave_dsk1 from dummy union all
                 select 'Sítio'                as descricao, '64' as chave_dsk1 from dummy union all
                 select 'Servidão'             as descricao, '65' as chave_dsk1 from dummy union all
                 select 'Setor'                as descricao, '66' as chave_dsk1 from dummy union all
                 select 'Subida'               as descricao, '67' as chave_dsk1 from dummy union all
                 select 'Trincheira'           as descricao, '68' as chave_dsk1 from dummy union all
                 select 'Terminal'             as descricao, '69' as chave_dsk1 from dummy union all
                 select 'Trevo'                as descricao, '70' as chave_dsk1 from dummy union all
                 select 'Travessa'             as descricao, '71' as chave_dsk1 from dummy union all
                 select 'Via'                  as descricao, '72' as chave_dsk1 from dummy union all
                 select 'Viaduto'              as descricao, '73' as chave_dsk1 from dummy union all
                 select 'Vila'                 as descricao, '74' as chave_dsk1 from dummy union all
                 select 'Viela'                as descricao, '75' as chave_dsk1 from dummy union all
                 select 'Vale'                 as descricao, '76' as chave_dsk1 from dummy union all
                 select 'Zigue-Zague'          as descricao, '77' as chave_dsk1 from dummy union all
                 select 'Trecho'               as descricao, '78' as chave_dsk1 from dummy union all
                 select 'Vereda'               as descricao, '79' as chave_dsk1 from dummy union all
                 select 'Artéria'              as descricao, '80' as chave_dsk1 from dummy union all
                 select 'Elevada'              as descricao, '81' as chave_dsk1 from dummy union all
                 select 'Porto'                as descricao, '82' as chave_dsk1 from dummy union all
                 select 'Balão'                as descricao, '83' as chave_dsk1 from dummy union all
                 select 'Paradouro'            as descricao, '84' as chave_dsk1 from dummy union all
                 select 'Área'                 as descricao, '85' as chave_dsk1 from dummy union all
                 select 'Jardinete'            as descricao, '86' as chave_dsk1 from dummy union all
                 select 'Esplanada'            as descricao, '87' as chave_dsk1 from dummy union all
                 select 'Quintas'              as descricao, '88' as chave_dsk1 from dummy union all
                 select 'Rótula'               as descricao, '89' as chave_dsk1 from dummy union all
                 select 'Marina'               as descricao, '90' as chave_dsk1 from dummy union all
                 select 'Descida'              as descricao, '91' as chave_dsk1 from dummy union all
                 select 'Circular'             as descricao, '92' as chave_dsk1 from dummy union all
                 select 'Unidade'              as descricao, '93' as chave_dsk1 from dummy union all
                 select 'Chácara'              as descricao, '94' as chave_dsk1 from dummy union all
                 select 'Rampa'                as descricao, '95' as chave_dsk1 from dummy union all
                 select 'Ponta'                as descricao, '96' as chave_dsk1 from dummy union all
                 select 'Via de pedestre'      as descricao, '97' as chave_dsk1 from dummy union all
                 select 'Condomínio'           as descricao, '98' as chave_dsk1 from dummy union all
                 select 'Habitacional'         as descricao, '99' as chave_dsk1 from dummy union all
                 select 'Residencial'          as descricao, '100' as chave_dsk1 from dummy union all
                 select 'Canal'                as descricao, '101' as chave_dsk1 from dummy union all
                 select 'Buraco'               as descricao, '102' as chave_dsk1 from dummy union all
                 select 'Módulo'               as descricao, '103' as chave_dsk1 from dummy union all
                 select 'Estância'             as descricao, '104' as chave_dsk1 from dummy union all
                 select 'Lago'                 as descricao, '105' as chave_dsk1 from dummy union all
                 select 'Núcleo'               as descricao, '106' as chave_dsk1 from dummy union all
                 select 'Aeroporto'            as descricao, '107' as chave_dsk1 from dummy union all
                 select 'Passagem subterrânea' as descricao, '108' as chave_dsk1 from dummy union all
                 select 'Completo viário'      as descricao, '109' as chave_dsk1 from dummy union all
                 select 'Praça de esportes'    as descricao, '110' as chave_dsk1 from dummy union all
                 select 'Via elevado'          as descricao, '111' as chave_dsk1 from dummy union all
                 select 'Rotatória'            as descricao, '112' as chave_dsk1 from dummy union all
                 select 'Estacionamento'       as descricao, '113' as chave_dsk1 from dummy union all
                 select 'Vala'                 as descricao, '114' as chave_dsk1 from dummy union all
                 select 'Rua de pedestre'      as descricao, '115' as chave_dsk1 from dummy union all
                 select 'Túnel'                as descricao, '116' as chave_dsk1 from dummy union all
                 select 'Variante'             as descricao, '117' as chave_dsk1 from dummy union all
                 select 'Rodo anel'            as descricao, '118' as chave_dsk1 from dummy union all
                 select 'Travessa particular'  as descricao, '119' as chave_dsk1 from dummy union all
                 select 'Calçada'              as descricao, '120' as chave_dsk1 from dummy union all
                 select 'Via de acesso'        as descricao, '121' as chave_dsk1 from dummy union all
                 select 'Entrada particular'   as descricao, '122' as chave_dsk1 from dummy union all
                 select 'Acampamento'          as descricao, '123' as chave_dsk1 from dummy union all
                 select 'Via expressa'         as descricao, '124' as chave_dsk1 from dummy union all
                 select 'Localidade'           as descricao, '125' as chave_dsk1 from dummy union all
                 select 'Linha'                as descricao, '126' as chave_dsk1 from dummy union all
                 select 'Calçadão'             as descricao, '127' as chave_dsk1 from dummy union all
                 select 'Anel viário'          as descricao, '128' as chave_dsk1 from dummy
                 ) as Tab