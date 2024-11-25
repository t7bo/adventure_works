# Télécharger le ODBC Driver
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16

# Imports
import pyodbc
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du répertoire de logs
log_directory = '../logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging.basicConfig(filename='../logs/sql_extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_sql_server(server, database, username, password):
    """
    Se connecter au serveur SQL.
    """
    try:
        # Log des informations de connexion pour débogage
        logging.info(f"Connexion au serveur : {server}, base de données : {database}")
        
        connection = pyodbc.connect(
            f'DRIVER={os.getenv("DRIVER")};'
            f'SERVER={os.getenv("SERVER")};'
            f'DATABASE={os.getenv("DATABASE")};'
            f'UID={os.getenv("USERNAME")};'
            f'PWD={os.getenv("PASSWORD")}'
        )
        logging.info("Connexion réussie au serveur SQL.")
        return connection
    except Exception as e:
        logging.error(f"Erreur de connexion au serveur SQL : {e}")
        raise

def extract_data(connection, query, output_file):
    """
    Extraire les données en utilisant une requête SQL.
    Enregistrer les données dans un fichier .CSV.
    """
    # Créer le dossier data si nécessaire
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Le répertoire {output_dir} a été créé.")

    try:
        # Exécution de la requête SQL
        df = pd.read_sql_query(query, connection)

        # Log des premières lignes des données extraites pour débogage
        logging.info(f"Premières lignes des données extraites : {df.head()}")

        # Sauvegarde dans le fichier CSV
        df.to_csv(output_file, index=False)
        logging.info(f"Données extraites et sauvegardées dans {output_file}")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données : {e}")

if __name__ == "__main__":
    # Variables d'environnement
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    # Vérification des variables d'environnement
    if not all([server, database, username, password]):
        logging.error("Une ou plusieurs variables d'environnement manquent.")
        raise ValueError("Une ou plusieurs variables d'environnement sont manquantes.")
    
    # Log pour vérifier les variables d'environnement
    logging.info(f"Connexion avec les paramètres : SERVER={server}, DATABASE={database}")

    # Connexion à la base de données
    conn = connect_to_sql_server(server, database, username, password)

    # Requête SQL pour extraire les données
    query = "SELECT * FROM Production.Product"

    # Extraction et sauvegarde des données
    extract_data(conn, query, output_file="./data/azure_data.csv")

    # Fermeture de la connexion
    conn.close()