import pyodbc
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du répertoire de logs, s'il n'existe pas il sera créé.
log_directory = './logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configuration de la journalisation des évennements dans un fichier logs
logging.basicConfig(filename='./logs/sql_extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_sql_server(server, database, username, password):
    """
    Se connecter à une base de données SQL Server en utilisant les informations de connexion passées en paramètres.

    Arguments:
    - server (str) : Le nom ou l'adresse du serveur SQL.
    - database (str) : Le nom de la base de données à utiliser.
    - username (str) : Le nom d'utilisateur pour se connecter.
    - password (str) : Le mot de passe associé à l'utilisateur.

    Retourne :
    - connection (pyodbc.Connection) : L'objet de connexion à la base de données.

    Cette fonction utilise pyodbc pour établir une connexion ODBC avec la base de données SQL Server.
    """
    try:
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

def get_tables_in_schema(connection, schema_name):
    """
    Récupère la liste des tables dans un schéma spécifique de la base de données.

    Arguments:
    - connection (pyodbc.Connection) : L'objet de connexion à la base de données.
    - schema_name (str) : Le nom du schéma dans lequel rechercher les tables.

    Retourne :
    - Une liste des noms des tables présentes dans le schéma.
    
    Cette fonction exécute une requête SQL pour obtenir les noms des tables de type 'BASE TABLE' dans le schéma spécifié.
    """

    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'BASE TABLE';
    """
    # Exécution de la requête SQL et récupération du résultat dans un DataFrame pandas
    try:
        tables = pd.read_sql_query(query, connection)
        logging.info(f"{len(tables)} tables trouvées dans le schéma '{schema_name}': {tables['TABLE_NAME'].tolist()}")
        return tables['TABLE_NAME'].tolist() # Retourne la liste des noms des tables
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des tables pour le schéma '{schema_name}' : {e}")
        raise

def get_compatible_columns(connection, schema_name, table_name):
    """
    Récupère les colonnes compatibles pour une table donnée (exclut les types de données non compatibles comme 'geometry').

    Arguments:
    - connection (pyodbc.Connection) : L'objet de connexion à la base de données.
    - schema_name (str) : Le nom du schéma.
    - table_name (str) : Le nom de la table pour laquelle obtenir les colonnes.

    Retourne :
    - Une liste des noms des colonnes compatibles.

    Cette fonction exclut les types de données comme 'geometry', 'geography', 'xml', etc., qui peuvent poser problème lors de l'extraction.
    """

    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    AND DATA_TYPE NOT IN ('geometry', 'geography', 'xml', 'hierarchyid', 'sql_variant');
    """
    # Exécution de la requête SQL et récupération du résultat
    try:
        columns = pd.read_sql_query(query, connection)
        if columns.empty:
            logging.warning(f"Aucune colonne compatible trouvée dans {schema_name}.{table_name}.")
            return [] # Retourne une liste vide si aucune colonne compatible n'est trouvée
        return columns['COLUMN_NAME'].tolist()
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des colonnes compatibles pour {schema_name}.{table_name} : {e}")
        raise

def extract_table_to_csv(connection, table_name, schema_name, output_dir):
    """
    Extrait les données d'une table et les sauvegarde dans un fichier CSV dans un répertoire donné.

    Arguments:
    - connection (pyodbc.Connection) : L'objet de connexion à la base de données.
    - table_name (str) : Le nom de la table à extraire.
    - schema_name (str) : Le nom du schéma contenant la table.
    - output_dir (str) : Le répertoire où enregistrer les fichiers CSV extraits.

    Si la table contient des colonnes compatibles, les données sont extraites et enregistrées dans un fichier CSV.
    Si la table est vide ou si aucune colonne compatible n'est trouvée, le processus est ignoré.
    """
    compatible_columns = get_compatible_columns(connection, schema_name, table_name)
    if not compatible_columns:
        logging.warning(f"Extraction ignorée pour {schema_name}.{table_name} (aucune colonne compatible).")
        return
    
    # Construction de la liste des colonnes compatibles pour la requête SQL
    column_list = ', '.join([f"[{col}]" for col in compatible_columns])
    query = f"SELECT {column_list} FROM {schema_name}.{table_name}"
    
    # Chemin du fichier de sortie
    output_file = os.path.join(output_dir, schema_name, f"{table_name}.csv")
    
    # Créer le répertoire pour le schéma si nécessaire
    schema_dir = os.path.join(output_dir, schema_name)
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)
        logging.info(f"Le répertoire {schema_dir} a été créé.")

    try:
        # Exécution de la requête SQL et récupération des résultats dans un DataFrame
        df = pd.read_sql_query(query, connection)
        if df.empty:
            logging.warning(f"La table {schema_name}.{table_name} est vide. Aucune donnée à sauvegarder.")
        else:
            # Sauvegarde des données dans un fichier CSV
            df.to_csv(output_file, index=False)
            logging.info(f"Données de {schema_name}.{table_name} sauvegardées dans {output_file}.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction de {schema_name}.{table_name} : {e}")
        raise

if __name__ == "__main__":

    # Chargement des variables d'environnement
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    schemas = ["Production", "Sales", "Person"]

    # Répertoire où les fichiers CSV seront sauvegardés
    output_dir = "./data/azure"

    # Vérification des variables d'environnement
    if not all([server, database, username, password]):
        logging.error("Une ou plusieurs variables d'environnement manquent.")
        raise ValueError("Une ou plusieurs variables d'environnement sont manquantes.")
    
    # Log pour vérifier les variables d'environnement
    logging.info(f"Connexion avec les paramètres : SERVER={server}, DATABASE={database}")

    # Connexion à la base de données
    conn = connect_to_sql_server(server, database, username, password)

    try:
        # Pour chaque schéma, récupérer les tables et extraire les données
        for schema in schemas:
            tables = get_tables_in_schema(conn, schema)
            for table in tables:
                extract_table_to_csv(conn, table, schema, output_dir)
    finally:
        # Fermeture de la connexion à la BDD
        conn.close()
        logging.info("Connexion fermée.")