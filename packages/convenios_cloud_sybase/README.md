**Migrador de Obras**

Esse projeto faz a migração de obras dos sistemas desktop para sistemas Cloud Betha por API de tela.

**Como rodar**

Crie um venv (virtual enviroment) versão >= 3.9 do Python, depois execute ``pip install -r requirements.txt``.

No arquivo ``settings.py``, altere as chaves do banco:
```py
DB_HOST = 'localhost' # Host ODBC
DB_PORT = '3000' # Porta ODBC
DB_NAME = 'PROD_SapoAGUDOS' # Nome do serviço no ODBC
```

Também é necessário alterar as chaves do Cloud, retirando as informações da aba Network no sistema Cloud:
```py
APP_CONTEXT = 'eyJleGVyY2ljaW8iOnsidmFsdWUiOjIwMjEsImluc3VsYXRpb24iOmZhbHNlfX0='
USER_ACCESS = '_05RRp4R8zQ='
USER_TOKEN = '24085d1a-2bcf-4b67-8e1e-e60ad7e02eda'
```