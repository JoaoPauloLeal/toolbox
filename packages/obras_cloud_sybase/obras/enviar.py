import settings
# import packages.ipm_cloud_postgresql.model as model
import bth.interacao_cloud as interacao_cloud
from datetime import datetime

import packages.obras_cloud_sybase.migracao_obras as mg

def iniciar():
    print(':: Iniciando projeto do sistema Obras')
    mg.iniciar()