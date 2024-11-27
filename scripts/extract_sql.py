import pyodbc
import pandas as pd
from loguru import logger
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis un fichier `.env`
load_dotenv()

# Configuration du répertoire des logs
log_directory = './logs'
# Crée le répertoire pour les fichiers de logs si nécessaire
os.makedirs(log_directory, exist_ok=True)

# Configuration du logger avec loguru
logger.add(
    os.path.join(log_directory, 'sql_extraction.log'),  # Fichier log
    rotation="1 MB",  # Taille maximale d'un fichier log avant rotation
    retention="10 days",  # Durée de rétention des fichiers logs
    level="INFO",  # Niveau minimal pour journaliser
    format="{time} - {level} - {message}",  # Format des messages log
)

def connect_to_sql_server(server, database, username, password):
    """
    Établit une connexion à une base de données SQL Server.

    Arguments:
        server (str): Nom ou adresse du serveur SQL.
        database (str): Nom de la base de données cible.
        username (str): Nom d'utilisateur pour se connecter.
        password (str): Mot de passe pour l'utilisateur.

    Retourne:
        pyodbc.Connection: Objet de connexion à la base de données.

    Lève:
        Exception: Si la connexion échoue, une exception est levée et journalisée.
    """
    try:
        logger.info(f"Connexion au serveur : {server}, base de données : {database}")
        # Configuration de la connexion avec les variables d'environnement
        connection = pyodbc.connect(
            f'DRIVER={os.getenv("DRIVER")};'
            f'SERVER={os.getenv("SERVER")};'
            f'DATABASE={os.getenv("DATABASE")};'
            f'UID={os.getenv("USERNAME")};'
            f'PWD={os.getenv("PASSWORD")}'
        )
        logger.info("Connexion réussie au serveur SQL.")
        return connection
    except Exception as e:
        logger.error(f"Erreur de connexion au serveur SQL : {e}")
        raise

def get_tables_in_schema(connection, schema_name):
    """
    Récupère les noms des tables dans un schéma donné.

    Arguments:
        connection (pyodbc.Connection): Connexion active à la base de données.
        schema_name (str): Nom du schéma dont on veut lister les tables.

    Retourne:
        list: Une liste des noms des tables dans le schéma spécifié.

    Lève:
        Exception: Si la récupération échoue, une exception est levée et journalisée.
    """
    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'BASE TABLE';
    """
    try:
        # Exécute la requête SQL et retourne les résultats sous forme de DataFrame pandas
        tables = pd.read_sql_query(query, connection)
        logger.info(f"{len(tables)} tables trouvées dans le schéma '{schema_name}': {tables['TABLE_NAME'].tolist()}")
        return tables['TABLE_NAME'].tolist()
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des tables pour le schéma '{schema_name}' : {e}")
        raise

def get_compatible_columns(connection, schema_name, table_name):
    """
    Identifie les colonnes compatibles pour une extraction depuis une table donnée.

    Arguments:
        connection (pyodbc.Connection): Connexion active à la base de données.
        schema_name (str): Nom du schéma contenant la table.
        table_name (str): Nom de la table cible.

    Retourne:
        list: Une liste des noms des colonnes compatibles.

    Lève:
        Exception: Si une erreur survient lors de la récupération, elle est journalisée et levée.
    """
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    AND DATA_TYPE NOT IN ('geometry', 'geography', 'xml', 'hierarchyid', 'sql_variant');
    """
    try:
        # Exécute la requête pour récupérer les colonnes compatibles
        columns = pd.read_sql_query(query, connection)
        if columns.empty:
            logger.warning(f"Aucune colonne compatible trouvée dans {schema_name}.{table_name}.")
            return []
        return columns['COLUMN_NAME'].tolist()
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des colonnes compatibles pour {schema_name}.{table_name} : {e}")
        raise

def extract_table_to_csv(connection, table_name, schema_name, output_dir):
    """
    Extrait les données d'une table et les sauvegarde dans un fichier CSV.

    Arguments:
        connection (pyodbc.Connection): Connexion active à la base de données.
        table_name (str): Nom de la table à extraire.
        schema_name (str): Nom du schéma contenant la table.
        output_dir (str): Répertoire où enregistrer le fichier CSV.

    Actions:
        - Crée des sous-répertoires pour chaque schéma si nécessaire.
        - Sauvegarde les données sous forme de fichier CSV dans le répertoire spécifié.

    Lève:
        Exception: Si une erreur survient lors de l'extraction, elle est journalisée et levée.
    """
    compatible_columns = get_compatible_columns(connection, schema_name, table_name)
    if not compatible_columns:
        logger.warning(f"Extraction ignorée pour {schema_name}.{table_name} (aucune colonne compatible).")
        return

    # Prépare la liste des colonnes pour la requête SELECT
    column_list = ', '.join([f"[{col}]" for col in compatible_columns])
    query = f"SELECT {column_list} FROM {schema_name}.{table_name}"
    output_file = os.path.join(output_dir, schema_name, f"{table_name}.csv")

    # Crée le répertoire du schéma si nécessaire
    schema_dir = os.path.join(output_dir, schema_name)
    os.makedirs(schema_dir, exist_ok=True)

    try:
        # Exécute la requête et récupère les données dans un DataFrame pandas
        df = pd.read_sql_query(query, connection)
        if df.empty:
            logger.warning(f"La table {schema_name}.{table_name} est vide. Aucune donnée à sauvegarder.")
        else:
            # Sauvegarde les données dans un fichier CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Données de {schema_name}.{table_name} sauvegardées dans {output_file}.")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de {schema_name}.{table_name} : {e}")
        raise

if __name__ == "__main__":
    # Variables d'environnement
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    schemas = ["Production", "Sales", "Person"]  # Liste des schémas à traiter
    output_dir = "./data/azure"  # Répertoire de destination pour les CSV

    # Vérifie que toutes les variables nécessaires sont définies
    if not all([server, database, username, password]):
        logger.error("Une ou plusieurs variables d'environnement manquent.")
        raise ValueError("Une ou plusieurs variables d'environnement sont manquantes.")

    logger.info(f"Connexion avec les paramètres : SERVER={server}, DATABASE={database}")
    conn = connect_to_sql_server(server, database, username, password)

    try:
        # Pour chaque schéma, récupère les tables et extrait les données
        for schema in schemas:
            tables = get_tables_in_schema(conn, schema)
            for table in tables:
                extract_table_to_csv(conn, table, schema, output_dir)
    finally:
        # Ferme la connexion à la base de données après traitement
        conn.close()
        logger.info("Connexion fermée.")