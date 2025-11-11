import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os

def get_db_engine(db_type='SOURCE_DB'):
    """Crea y retorna un motor de SQLAlchemy para una base de datos"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)

    db_config = config[db_type]
    
    server = db_config['server']
    database = db_config['database']
    driver = db_config['driver']
    
    username = db_config.get('username')
    password = db_config.get('password')

    conn_str = None

    if username and password:
        # Autenticación
        connection_url = URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=server,
            database=database,
            query={"driver": driver},
        )
        conn_str = connection_url
    else:
        # Autenticación de Windows
        conn_str = f"mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes"

    try:
        engine = create_engine(conn_str)
        # Probar la conexión
        with engine.connect() as connection:
            print(f"Conectado exitosamente a la base de datos {db_type}: {database}")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos {db_type}: {e}")
        return None

if __name__ == '__main__':
    # Ejemplo de cómo usar la función
    print("Probando conexiones a la base de datos...")
    source_engine = get_db_engine('SOURCE_DB')
    dest_engine = get_db_engine('DESTINATION_DB')
    print("Prueba de conexión completada.")