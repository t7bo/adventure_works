import pyodbc
import pandas as pd
from loguru import logger
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du répertoire de logs
log_directory = './logs'
os.makedirs(log_directory, exist_ok=True)

# Configuration de loguru
logger.add(
    os.path.join(log_directory, 'sql_extraction.log'),
    rotation="1 MB",  # Log rotation based on size
    retention="10 days",  # Retain logs for 10 days
    level="INFO",
    format="{time} - {level} - {message}",
)

def connect_to_sql_server(server, database, username, password):
    """Connect to SQL Server database."""
    try:
        logger.info(f"Connexion au serveur : {server}, base de données : {database}")
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
    """Retrieve table names from a specific schema."""
    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'BASE TABLE';
    """
    try:
        tables = pd.read_sql_query(query, connection)
        logger.info(f"{len(tables)} tables trouvées dans le schéma '{schema_name}': {tables['TABLE_NAME'].tolist()}")
        return tables['TABLE_NAME'].tolist()
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des tables pour le schéma '{schema_name}' : {e}")
        raise

def get_compatible_columns(connection, schema_name, table_name):
    """Retrieve compatible columns for extraction."""
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    AND DATA_TYPE NOT IN ('geometry', 'geography', 'xml', 'hierarchyid', 'sql_variant');
    """
    try:
        columns = pd.read_sql_query(query, connection)
        if columns.empty:
            logger.warning(f"Aucune colonne compatible trouvée dans {schema_name}.{table_name}.")
            return []
        return columns['COLUMN_NAME'].tolist()
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des colonnes compatibles pour {schema_name}.{table_name} : {e}")
        raise

def extract_table_to_csv(connection, table_name, schema_name, output_dir):
    """Extract table data to a CSV file."""
    compatible_columns = get_compatible_columns(connection, schema_name, table_name)
    if not compatible_columns:
        logger.warning(f"Extraction ignorée pour {schema_name}.{table_name} (aucune colonne compatible).")
        return

    column_list = ', '.join([f"[{col}]" for col in compatible_columns])
    query = f"SELECT {column_list} FROM {schema_name}.{table_name}"
    output_file = os.path.join(output_dir, schema_name, f"{table_name}.csv")

    schema_dir = os.path.join(output_dir, schema_name)
    os.makedirs(schema_dir, exist_ok=True)

    try:
        df = pd.read_sql_query(query, connection)
        if df.empty:
            logger.warning(f"La table {schema_name}.{table_name} est vide. Aucune donnée à sauvegarder.")
        else:
            df.to_csv(output_file, index=False)
            logger.info(f"Données de {schema_name}.{table_name} sauvegardées dans {output_file}.")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de {schema_name}.{table_name} : {e}")
        raise

if __name__ == "__main__":
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    schemas = ["Production", "Sales", "Person"]
    output_dir = "./data/azure"

    if not all([server, database, username, password]):
        logger.error("Une ou plusieurs variables d'environnement manquent.")
        raise ValueError("Une ou plusieurs variables d'environnement sont manquantes.")

    logger.info(f"Connexion avec les paramètres : SERVER={server}, DATABASE={database}")
    conn = connect_to_sql_server(server, database, username, password)

    try:
        for schema in schemas:
            tables = get_tables_in_schema(conn, schema)
            for table in tables:
                extract_table_to_csv(conn, table, schema, output_dir)
    finally:
        conn.close()
        logger.info("Connexion fermée.")
