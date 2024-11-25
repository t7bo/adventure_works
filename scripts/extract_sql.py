# Dans la BDD, quelles seraient les données à extraire? --> infos clients et produits
    # Production.ProductDescription?
    # Production.ProductCategoryID?
    # Production.Product?
    # Production.ProductListPriceHistory?
    # Production.ProductCostHistory?
    # Production.ProductReview?
    # Production.ProductPhoto?

# Télécharger le ODBC Driver
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16

# Imports
import pyodbc # permet de se connecter à des bases de données en utilisant le protocole ODBC (Open Database Connectivity)
import pandas as pd
import logging

# Configuration de la journalisation : Activation de la journalisation dans un fichier spécifique avec des paramètres personnalisés.
logging.basicConfig(filename='../logs/sql_extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
                    # filename spécifie le fichier dans lequel les logs seront sauvegardés.
                    # level=logging.INFO définit le niveau minimal de logs, ici les messages avec un niveau INFO, WARNING, ERROR ou CRITICAL seront enregistrés.
                    # format définit le format des logs

def connect_to_sql_server(server, database, username, password):

    """
    Se connecter au server SQL.
    """

    server = "adventureworks-server-hdf"
    databse = "adventureworks"
    username = "jvcb"
    password = "cbjv592023!"

    try:
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};' # spécifie quel pilote ODBC doit être utilisé pour se connecter au serveur SQL.
            f'SERVER={server};' # nom/adresse du serveur SQL.
            f'DATABASE={database};' # nom/adresse de la BDD.
            f'UID={username};'
            f'PWD={password}'
        )
        logging.info("Connection réussie au serveur SQL.")
        return connection
    except Exception as e:
        logging.error("Erreur de connection au serveur SQL.")
        raise

