import boto3
from sqlalchemy import create_engine

def mysql_connect(db_config_settings):

    engine = create_engine(
        f"mysql+mysqlconnector://{db_config_settings['user']}:{db_config_settings['password']}@{db_config_settings['host']}:{db_config_settings['port']}/{db_config_settings['database']}"
    )
    connection = engine.raw_connection()
    
    return connection


def s3_connect():

    s3_client = boto3.client('s3')
    return s3_client


def redshift_connect(db_config_settings):

    engine = create_engine(
        f"postgresql://{db_config_settings['user']}:{db_config_settings['password']}@{db_config_settings['host']}:{db_config_settings['port']}/{db_config_settings['database']}"
    )
    connection = engine.connect()

    return connection