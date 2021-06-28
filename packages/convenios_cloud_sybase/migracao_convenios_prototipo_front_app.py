import time
import settings
from flask import Flask, render_template
from PySimpleGUI import PySimpleGUI as sg
from datetime import datetime
from os import path
from json import (load as jsonload, dump as jsondump)

SETTINGS_FILE = path.join(path.dirname(__file__), r'settings_file.cfg')
DEFAULT_SETTINGS = {}


def iniciar():
    print('Iniciando migração do sistema convenios.')

    params_exec = {
        'id_entidade': 0,
        'i_plano_contas': 0,
        'sistema': 1,
        'nome_sistema': 'CONVÊNIOS',
        'nome_entidade': '',
        'token_tela': '',
        'user-access': '',
        'user_login': '',
        'user_pass': '',
        'db_name': 'Criciuma',
        'db_user': '',
        'db_pw': '',
        'db_host': 'localhost',
        'db_port': '9018',
    }

    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('main.html')

    @app.route('/estatisticas')
    def estatisticas():
        # tipo_registro, situacao, mensagem, codigo_mensagem, total_registro, total_migrados
        cad_1 = Cadastro('Concedente', 'Em andamento', 'Sem erro', 'BTH-000', '302', '15')
        cad_2 = Cadastro('Convedente', 'Em andamento', 'Sem erro', 'BTH-000', '15', '8')
        cad_3 = Cadastro('Modalidade', 'Em andamento', 'Sem erro', 'BTH-000', '58', '4')
        cad_4 = Cadastro('Tipo Repasse', 'Em andamento', 'Sem erro', 'BTH-000', '6', '2')
        cad_5 = Cadastro('Tipo Responsavel', 'Em andamento', 'Sem erro', 'BTH-000', '145', '16')
        cad_6 = Cadastro('Responsavel', 'Em andamento', 'Sem erro', 'BTH-000', '800', '500')
        cad_7 = Cadastro('Natureza Texto Juridico', 'Parado', 'Erro de cadastro dependente', 'BTH-002', '18', '3')
        cad_8 = Cadastro('Tipo Ato', 'Não iniciado', 'Sem erro', 'BTH-000', '58', '0')
        cad_9 = Cadastro('Ato', 'Não iniciado', 'Sem erro', 'BTH-000', '20', '0')
        cad_10 = Cadastro('Tipo Situacao', 'Não iniciado', 'Sem erro', 'BTH-000', '10', '0')
        cad_11 = Cadastro('Tipo Aditivo', 'Não iniciado', 'Sem erro', 'BTH-000', '16', '0')
        cad_12 = Cadastro('Convenio Recebido', 'Não iniciado', 'Sem erro', 'BTH-000', '566', '0')
        cad_13 = Cadastro('Convenio Recebido Aditivo', 'Não iniciado', 'Sem erro', 'BTH-000', '253', '0')
        cad_14 = Cadastro('Convenio Recebido Situacao', 'Não iniciado', 'Sem erro', 'BTH-000', '566', '0')
        cad_15 = Cadastro('Convenio Recebido Aditivo Situacao', 'Não iniciado', 'Sem erro', 'BTH-000', '253', '0')
        cad_16 = Cadastro('Convenio Repassado', 'Não iniciado', 'Sem erro', 'BTH-000', '6559', '0')
        cad_17 = Cadastro('Convenio Repassado Aditivo', 'Não iniciado', 'Sem erro', 'BTH-000', '8975', '0')
        cad_18 = Cadastro('Convenio Repassado Situacao', 'Não iniciado', 'Sem erro', 'BTH-000', '6559', '0')
        cad_19 = Cadastro('Convenio Repassado Aditivo Situacao', 'Não iniciado', 'Sem erro', 'BTH-000', '8975', '0')
        lista = [cad_1, cad_2, cad_3, cad_4, cad_5, cad_6, cad_7, cad_8, cad_9, cad_10, cad_11, cad_12, cad_13, cad_14,
                 cad_15, cad_16, cad_17, cad_18, cad_19]
        # return render_template('estatisticas.html')
        return render_template('estatisticas.html', lista=lista)


    # trecho da app
    app.run(debug=True, host='0.0.0.0', port=8080)


    # layout = load_layout()
    # parametros = {}
    # janela = sg.Window('Informações para Migração de Convênios', layout)
    # while True:
    #     eventos, valores = janela.read()
    #     if eventos == sg.WINDOW_CLOSED:
    #         break
    #     if eventos == 'Iniciar':
    #         params_exec['user_login'] = valores['user_login']
    #         params_exec['user_pass'] = valores['user_pass']
    #         params_exec['db_name'] = valores['db_name']
    #         params_exec['db_port'] = valores['db_port']
    #         params_exec['id_entidade'] = valores['id_entidade']
    #         params_exec['i_plano_contas'] = valores['i_plano_contas']
    #         params_exec['nome_entidade'] = valores['nome_entidade']
    #         save_settings(SETTINGS_FILE, parametros, valores, params_exec)
    #         janela.close()

    """ Carregamento do token de tela """
    # token = buscar(params_exec, 'get_token')
    # if token is not None:
    # params_exec['token_tela'] = token['authorization']
    # params_exec['user-access'] = token['user-access']
    """ Inicia envio Convenios """
    """ Dados cadastrais Convenios """
    # enviar(params_exec, 'concedente')
    # enviar(params_exec, 'convenente')
    # enviar(params_exec, 'modalidade')
    # enviar(params_exec, 'tipo_repasse')
    # enviar(params_exec, 'tipo_responsavel')
    # enviar(params_exec, 'responsavel')
    # enviar(params_exec, 'natureza_texto_juridico')
    # enviar(params_exec, 'tipo_ato')
    # enviar(params_exec, 'ato')
    # enviar(params_exec, 'tipo_situacao')
    # enviar(params_exec, 'tipo_aditivo')

    """ Inicia Cadastros principais """
    """ Convenio Recebido """
    # enviar(params_exec, 'convenio_recebido')
    # enviar(params_exec, 'convenio_recebido_aditivo')
    # enviar(params_exec, 'convenio_recebido_situacao')
    # enviar(params_exec, 'convenio_recebido_aditivo_situacao')
    """ Convenio Repassado """
    # enviar(params_exec, 'convenio_repassado')
    # enviar(params_exec, 'convenio_repassado_aditivo')
    # enviar(params_exec, 'convenio_repassado_situacao')
    # enviar(params_exec, 'convenio_repassado_aditivo_situacao')


class Cadastro:
    def __init__(self, tipo_registro, situacao, mensagem, codigo_mensagem, total_registro, total_migrados):
        self.tipo_registro = tipo_registro
        self.situacao = situacao
        self.mensagem = mensagem
        self.codigo_mensagem = codigo_mensagem
        self.total_registro = total_registro
        self.total_migrados = total_migrados


def enviar(params_exec, tipo_registro, *args, **kwargs):
    print(f'\n:: Iniciando execução do cadastro {tipo_registro}')
    tempo_inicio = datetime.now()
    path_padrao = f'packages.{settings.BASE_ORIGEM}.rotinas'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['iniciar_processo_envio'], 0)
    modulo.iniciar_processo_envio(params_exec)
    print(f'- Rotina de {tipo_registro} finalizada. '
          f'\nTempo total de execução: {(datetime.now() - tempo_inicio).total_seconds()} segundos.')


def buscar(params_exec, tipo_registro, *args, **kwargs):
    path_padrao = f'packages.{settings.BASE_ORIGEM}.rotinas'
    modulo = __import__(f'{path_padrao}.{tipo_registro}', globals(), locals(), ['get_token_access'], 0)
    return modulo.get_token_access(params_exec)


def valida_campo(dado):
    for item in dado:
        if item is None:
            return False
    return True


def load_layout():
    sg.theme('LightBlue3')
    retorno = load_settings(SETTINGS_FILE, DEFAULT_SETTINGS)
    if retorno:
        layout = [
            [sg.Text('Usuário do cloud', size=(40, 1)),
             sg.Input(retorno['user_login'], key='user_login', size=(40, 1))],
            [sg.Text('Senha do cloud', size=(40, 1)), sg.Input(key='user_pass', password_char='*', size=(40, 1))],
            [sg.Text('Nome do banco desktop', size=(40, 1)), sg.Input(retorno['db_name'], key='db_name', size=(40, 1))],
            [sg.Text('ID da entidade desktop', size=(40, 1)),
             sg.Input(retorno['id_entidade'], key='id_entidade', size=(40, 1))],
            [sg.Text('Numero do plano de contas desktop', size=(40, 1)),
             sg.Input(retorno['i_plano_contas'], key='i_plano_contas', size=(40, 1))],
            [sg.Text('Porta configurada do banco desktop', size=(40, 1)),
             sg.Input(retorno['db_port'], key='db_port', size=(40, 1))],
            [sg.Text('Nome da entidade (exatamente como esta no cloud)', size=(40, 1)),
             sg.Input(retorno['nome_entidade'], key='nome_entidade', size=(40, 1))],
            [sg.Button('Iniciar')]
        ]
    else:
        layout = [
            [sg.Text('Usuário do cloud', size=(40, 1)), sg.Input(key='user_login', size=(40, 1))],
            [sg.Text('Senha do cloud', size=(40, 1)), sg.Input(key='user_pass', password_char='*', size=(40, 1))],
            [sg.Text('Nome do banco desktop', size=(40, 1)), sg.Input(key='db_name', size=(40, 1))],
            [sg.Text('ID da entidade desktop', size=(40, 1)), sg.Input(key='id_entidade', size=(40, 1))],
            [sg.Text('Numero do plano de contas desktop', size=(40, 1)), sg.Input(key='i_plano_contas', size=(40, 1))],
            [sg.Text('Porta configurada do banco desktop', size=(40, 1)), sg.Input(key='db_port', size=(40, 1))],
            [sg.Text('Nome da entidade (exatamente como esta no cloud)', size=(40, 1)),
             sg.Input(key='nome_entidade', size=(40, 1))],
            [sg.Button('Iniciar')]
        ]
    return layout


def load_settings(settings_file, default_settings):
    try:
        with open(settings_file, 'r') as f:
            settings = jsonload(f)
    except Exception as e:
        sg.popup_quick_message(f'exception {e}', 'No settings file found... will create one for you', keep_on_top=True,
                               background_color='red', text_color='white')
        settings = default_settings
        save_settings(settings_file, settings, None)
    return settings


def save_settings(settings_file, parametros, values, params_exec):
    SETTINGS_KEYS_TO_ELEMENT_KEYS = {'id_entidade': params_exec['id_entidade'],
                                     'i_plano_contas': params_exec['i_plano_contas'],
                                     'sistema': params_exec['sistema'], 'nome_sistema': params_exec['nome_sistema'],
                                     'user_login': params_exec['user_login'], 'db_name': params_exec['db_name'],
                                     'db_user': params_exec['db_user'], 'nome_entidade': params_exec['nome_entidade'],
                                     'db_port': params_exec['db_port'], 'db_host': params_exec['db_host'],
                                     'token_tela': params_exec['token_tela']}
    if values:
        for key in SETTINGS_KEYS_TO_ELEMENT_KEYS:
            try:
                parametros[key] = SETTINGS_KEYS_TO_ELEMENT_KEYS[key]
            except Exception as e:
                print(f'Problem updating settings from window values. Key = {key}')

    with open(settings_file, 'w') as f:
        print(f)
        jsondump(parametros, f)
    # sg.popup('Configuração salva')
