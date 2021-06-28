import time
import settings
from flask import Flask, render_template
from PySimpleGUI import PySimpleGUI as sg
from datetime import datetime
from os import path
from json import (load as jsonload, dump as jsondump)


def iniciar():
    print('Iniciando front end da aplicação.')
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


class Cadastro:
    def __init__(self, tipo_registro, situacao, mensagem, codigo_mensagem, total_registro, total_migrados):
        self.tipo_registro = tipo_registro
        self.situacao = situacao
        self.mensagem = mensagem
        self.codigo_mensagem = codigo_mensagem
        self.total_registro = total_registro
        self.total_migrados = total_migrados