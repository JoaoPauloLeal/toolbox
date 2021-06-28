select distinct
       wn.codtom as key1,
	   wn.codtom as id,
	   e.codigo as cpf_cnpj_tomador,
	   (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) as i_pessoas,
	   wn.cnpjpre::text as cpf_cnpj_prestador,
	   (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = wn.cnpjpre::text
	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = wn.cnpjpre)) as i_pessoas_declarados,
	   wn.ano as ano,
	   (select id_gerado from public.controle_migracao_registro cmr
	     where i_chave_dsk1 = (select t.cidcodigoibge::text from public.tbcidade t where t.cidcodigoreceita = wn2.cidcodigo)
	       and tipo_registro = 'cidades') as id_cidade,
--	   wn2.cidcodigo as id_cidade,
	   u2.nomerazao as nome_tomador_prestador,
	    coalesce(u2.nomefantasia,'') as nome_fantasia,
	   case u2.fisicajuridica
        when 'J' then 'J'
        when 'F' then 'F'
        else 'O'
       end as tipo_pessoa,
       wn.simples as optante_sn,
       (select t.cidnome from public.tbcidade t where t.cidcodigoreceita = wn2.cidcodigo) as nome_cidade,
        coalesce(u2.nomebairro,'') as nome_bairro,
        coalesce(u2.endereco,'') as endereco,
        coalesce(u2.nroresidencia,'') as numero,
        coalesce(u2.cep::text, '') as cep,
        coalesce(u2.complemento, '') as complemento,
        coalesce(u2.email, '') as email,
        coalesce(u2.fonecomercial::text, '') as telefone,
        coalesce(u2.fonecelular::text, '') as celular
  from "8045_isarq".wm_nfst wn join "8045_isarq".wm_nfstit wn2 on wn.codtom = wn2.codtom
  																and wn.cnpjpre = wn2.cnpjpre
  																and wn.compet = wn2.compet
  																and wn.numero = wn2.numero
  							   join "8045_spunico".unico u2 on wn.cnpjpre = u2.cpfcnpj
  							   join "8045_isarq".economico e on wn.codtom = e.cadastro
 where wn.compet = '12/2020'
   and (select id_gerado from controle_migracao_registro cmr where tipo_registro = 'pessoas' and i_chave_dsk2 = e.codigo::text
	   								and i_chave_dsk1 = (select u.nomerazao from "8045_spunico".unico u where u.cpfcnpj = e.codigo) ) is not null