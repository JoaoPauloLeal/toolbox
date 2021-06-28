select
isnull((select first i_credores from sapo.despexs c where
c.ano_exerc = {{exercicio}} and
c.i_entidades = {{entidade}} and
c.i_credores is not null and
c.i_contas = d.i_contas),'') as ee, *, i_entidades as entidades
from sapo.despexs d where
d.ano_exerc = {{exercicio}} and
d.i_entidades = {{entidade}} and
d.i_credores is null