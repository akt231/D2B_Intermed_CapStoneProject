from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import re
import os

#====================================================================
# load env. var. from .env file
#====================================================================
from dotenv import load_dotenv
load_dotenv() 

def sf_get_private_key_uncrypted():    
    sf_pem_pass = os.getenv('sf_pem_pass')
    with open("/opt/spark-app/rsa_key.p8", "rb") as key_file:
        p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=f"{sf_pem_pass}".encode(),
        backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
        )
    
    pkb = pkb.decode("UTF-8")
    pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",pkb).replace("\n","")
    print(pkb)
    return pkb