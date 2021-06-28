import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
import packages.ipm_cloud_postgresql.livro_eletronico.rotinas_envio.buscaTomadoresPrestadores as tomadoresPrestadoresBusca
import json
import logging
from datetime import datetime


tipo_registro = 'tomadoresprestadores'
sistema = 999
limite_lote = 50
url = "https://livro-eletronico.cloud.betha.com.br/service-layer-livro/api/tomadoresprestadores"


def iniciar_processo_envio(params_exec, *args, **kwargs):
    # tomadoresPrestadoresBusca.iniciar_processo_busca(params_exec)
    dados_assunto = coletar_dados(params_exec)
    dados_enviar = pre_validar(params_exec, dados_assunto)
    if not params_exec.get('somente_pre_validar'):
        iniciar_envio(params_exec, dados_enviar, 'POST')
    model.valida_lotes_enviados(params_exec, tipo_registro=tipo_registro)


def coletar_dados(params_exec):
    print('- Iniciando a consulta dos dados a enviar.')
    df = None
    try:
        query = model.get_consulta(params_exec, tipo_registro + '.sql')
        pgcnn = model.PostgreSQLConnection()
        df = pgcnn.exec_sql(query, index_col='id')
        print(f'- Consulta finalizada. {len(df.index)} registro(s) encontrado(s).')
    except Exception as error:
        print(f'Erro ao executar função {tipo_registro}. {error}')
    finally:
        return df


def pre_validar(params_exec, dados):
    print('- Iniciando pré-validação dos registros.')
    dados_validados = []
    registro_erros = []
    try:
        lista_dados = dados.to_dict('records')
        for linha in lista_dados:
            registro_valido = True
            if registro_valido:
                dados_validados.append(linha)
        print(f'- Pré-validação finalizada. Registros validados com sucesso: '
              f'{len(dados_validados)} | Registros com advertência: {len(registro_erros)}')
    except Exception as error:
        logging.error(f'Erro ao executar função "pre_validar". {error}')
    finally:
        return dados_validados


def iniciar_envio(params_exec, dados, metodo, *args, **kwargs):
    print('- Iniciando envio dos dados.')
    lista_dados_enviar = []
    lista_controle_migracao = []
    hoje = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    token = params_exec['token']
    contador = 0
    for item in dados:
        hash_chaves = model.gerar_hash_chaves(sistema, tipo_registro, item["key1"], item["cpf_cnpj_prestador"])
        dict_dados = {
            "idIntegracao": hash_chaves,
            "declarados": {
                "iPessoas": item["i_pessoas"],
                "tipoPessoa": item["tipo_pessoa"],
                "iPessoasDeclarados": None if "i_pessoas_declarados" not in item else item["i_pessoas_declarados"],
                "iMunicipios": item["id_cidade"],
                "cpfCnpj": item["cpf_cnpj_prestador"],
                # "numeroDocumento": item["numero_documento"],
                # "inscricaoMunicipal": item["inscricao_municipal"],
                # "inscricaoEstadual": item["inscricao_estadual"],
                "optanteSn": item["optante_sn"],
                # "porteEmpresa": item["porte_empresa"],
                "nome": item["nome_tomador_prestador"],
                "nomeFantasia": None if "nome_fantasia" not in item else item["nome_fantasia"],
                # "pais": item["pais"],
                "municipio": None if "nome_cidade" not in item else item["nome_cidade"],
                "bairro": None if "nome_bairro" not in item else item["nome_bairro"],
                "endereco": None if "endereco" not in item else item["endereco"],
                "numero": None if "numero" not in item else item["numero"],
                "cep": None if "cep" not in item else item["cep"],
                "complemento": None if "complemento" not in item else item["complemento"],
                "email": None if "email" not in item else item["email"],
                "telefone": None if "telefone" not in item else item["telefone"],
                # "site": item["site"],
                "celular": None if "celular" not in item else item["celular"]
            }
        }

        contador += 1
        # print(f'Dados gerados ({contador}): ', dict_dados)
        lista_dados_enviar.append(dict_dados)
        lista_controle_migracao.append({
            'sistema': sistema,
            'tipo_registro': tipo_registro,
            'hash_chave_dsk': hash_chaves,
            'descricao_tipo_registro': 'Cadastro de Tomadores/Prestadores',
            'id_gerado': None,
            'i_chave_dsk1': item["key1"],
            'i_chave_dsk2': item["cpf_cnpj_prestador"]
        })
    # print(lista_controle_migracao)
    # print(lista_dados_enviar)
    model.insere_tabela_controle_migracao_registro2(params_exec, lista_req=lista_controle_migracao)
    req_res = interacao_cloud.preparar_requisicao(lista_dados=lista_dados_enviar,
                                                  token=token,
                                                  url=url,
                                                  tipo_registro=tipo_registro,
                                                  tamanho_lote=limite_lote)
    model.insere_tabela_controle_lote(req_res)
    print('- Envio de dados finalizado.')
