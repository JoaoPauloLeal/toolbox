"""
    Rotina de migração do sistema Obras

    Observações:
    * Deve-se configurar os dados de conexão e usuário no módulo 'settings.py' antes de executar essa rotina;
    * Foi definido o código 318 para ser o do sistema 'Obras' nas tabelas de controle;
"""
from migracao_convenios import iniciar as iniciar_migracao_convenios

if __name__ == '__main__':
    iniciar_migracao_convenios()
