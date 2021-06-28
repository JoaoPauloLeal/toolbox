import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import json
import logging
from datetime import datetime

tipo_registro = 'pessoas'
sistema = 306
limite_lote = 100
url = "https://e-gov.betha.com.br/glb/service-layer/v2/api/pessoas"


def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    print('- Iniciando busca dos dados de pessoas.')
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = 0
    req_res = interacao_cloud.busca_dados_cad_unico_cloud(params_exec,
                                                          url=url,
                                                          tipo_registro=tipo_registro,
                                                          tamanho_lote=limite_lote)
    # print(req_res)
    # dados_update = []
    for item in req_res:
        idGerado = item['idGerado']
        idGerado = idGerado['iPessoas']
        chave_dsk1 = item['nome'].upper()
        tipoPessoa = item['tipoPessoa']
        if tipoPessoa == 'F':
            if not item['pessoaFisica']:
                if not item['pessoaJuridica']:
                    print("ID GERADO COM ERRO:", idGerado)
                    print("CADASTRO DE PESSOA COM INCONSISTENCIA")
                else:
                    cpf_cnpj = item['pessoaJuridica']
                    cpf_cnpj = cpf_cnpj['cnpj']
            else:
                cpf_cnpj = item['pessoaFisica']
                cpf_cnpj = cpf_cnpj['cpf']
        if tipoPessoa == 'J':
            if not item['pessoaJuridica']:
                if not item['pessoaFisica']:
                    print("ID GERADO COM ERRO:", idGerado)
                    print("CADASTRO DE PESSOA COM INCONSISTENCIA")
                else:
                    cpf_cnpj = item['pessoaFisica']
                    cpf_cnpj = cpf_cnpj['cpf']
            else:
                cpf_cnpj = item['pessoaJuridica']
                cpf_cnpj = cpf_cnpj['cnpj']
        chave_dsk2 = cpf_cnpj
        # print(f'idCloud {idGerado} - {chave_dsk1} - {chave_dsk2}')
        # print(sistema, tipo_registro, chave_dsk1, chave_dsk2)
        #
        if not cpf_cnpj:
            cpf_cnpj = "ERRO"

        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, chave_dsk1, chave_dsk2)
        # print(hash_chaves)

        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Busca de Pessoas',
            'id_gerado': idGerado,
            'i_chave_dsk1': chave_dsk1,
            'i_chave_dsk2': chave_dsk2
        })
        contador += 1
    # print(lista_controle_migracao)
    model.insere_tabela_controle_migracao_auxiliar(params_exec, lista_req=lista_controle_migracao)
    # print(contador)
    print('- Busca de pessoas finalizado.')
