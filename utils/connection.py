import snowflake.connector
import configparser
import sys
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

def getConnection(warehouse, log_obj,db='',sc=''):

    config_path = "privateKey\Config\config.ini"
    key_path = "privateKey\key_rsa.p8"

    log_obj.info("connection.py--getConnection| Establishing connection to database..")

    try:
        config_obj = configparser.ConfigParser()
        config_obj.read(config_path)

        conn_params = config_obj['CONNECTION_NAME']

        with open(key_path, 'rb') as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password = os.environ['PRIVATE_KEY_PASSPHRASE'].encode() # pwd stored as an environment variable for this account
                backend = default_backend()
            )

        pkb = p_key.private_bytes(
            encoding = serialization.Encoding.DER,
            format = serialization.PrivateFormat.PKCS8,
            encryption_algorithm = serialization.NoEncryption()
        )

        if pkb:
            connection = snowflake.connector.connect(
                user = conn_params['USER']
                account = conn_params['ACCOUNT']
                role = conn_params['ROLE']
                private_key = pkb,
                warehouse = warehouse,
                log_size = conn_params['LOG_SIZE'],
                database = db,
                schema = sc
            )

            log_obj.info("connection.py--getConnection()| Connection established completely")
            return connection

    except Exception as e:
        log_obj.error("connection.py--getConnection()| Connection failed, please check this error: "
                      +str(e))
        

def closeConnection(connection, log_obj):
    try:
        if connection:
            log_obj.info("connection.py--getConnection()| Connection will be closed now.")
            connection.close()
    except Exception as e:
        log_obj.info("connection.py--getConnection()| Please check this error while closing connection: "
                     +str(e))
        sys.exit()