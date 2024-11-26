import pyodbc
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du répertoire de logs
log_directory = './logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging.basicConfig(filename='./logs/sql_extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_sql_server(server, database, username, password):
    """
    Se connecter au serveur SQL.
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
    Obtenir la liste des tables dans un schéma spécifique.
    """
    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'BASE TABLE';
    """
    try:
        tables = pd.read_sql_query(query, connection)
        logging.info(f"{len(tables)} tables trouvées dans le schéma '{schema_name}': {tables['TABLE_NAME'].tolist()}")
        return tables['TABLE_NAME'].tolist()
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des tables pour le schéma '{schema_name}' : {e}")
        raise

def get_compatible_columns(connection, schema_name, table_name):
    """
    Obtenir les colonnes compatibles (types de données non problématiques) pour une table donnée.
    """
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    AND DATA_TYPE NOT IN ('geometry', 'geography', 'xml', 'hierarchyid', 'sql_variant');
    """
    try:
        columns = pd.read_sql_query(query, connection)
        if columns.empty:
            logging.warning(f"Aucune colonne compatible trouvée dans {schema_name}.{table_name}.")
            return []
        return columns['COLUMN_NAME'].tolist()
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des colonnes compatibles pour {schema_name}.{table_name} : {e}")
        raise

def extract_table_to_csv(connection, table_name, schema_name, output_dir):
    """
    Extraire les données d'une table et les sauvegarder dans un fichier CSV.
    """
    compatible_columns = get_compatible_columns(connection, schema_name, table_name)
    if not compatible_columns:
        logging.warning(f"Extraction ignorée pour {schema_name}.{table_name} (aucune colonne compatible).")
        return
    
    column_list = ', '.join([f"[{col}]" for col in compatible_columns])
    query = f"SELECT {column_list} FROM {schema_name}.{table_name}"
    
    output_file = os.path.join(output_dir, schema_name, f"{table_name}.csv")
    
    # Créer le répertoire pour le schéma si nécessaire
    schema_dir = os.path.join(output_dir, schema_name)
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)
        logging.info(f"Le répertoire {schema_dir} a été créé.")

    try:
        df = pd.read_sql_query(query, connection)
        if df.empty:
            logging.warning(f"La table {schema_name}.{table_name} est vide. Aucune donnée à sauvegarder.")
        else:
            df.to_csv(output_file, index=False)
            logging.info(f"Données de {schema_name}.{table_name} sauvegardées dans {output_file}.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction de {schema_name}.{table_name} : {e}")
        raise

if __name__ == "__main__":
    # Variables d'environnement
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    schemas = ["Production", "Sales", "HumanResources", "Person", "Purchasing"]

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
        for schema in schemas:
            tables = get_tables_in_schema(conn, schema)
            for table in tables:
                extract_table_to_csv(conn, table, schema, output_dir)
    finally:
        conn.close()
        logging.info("Connexion fermée.")