import os
import logging

def write_to_env_file(env_file_path, default_content=None):
    """
    Crée ou met à jour un fichier .env avec du contenu par défaut.
    """
    try:
        # Vérifie si le fichier existe déjà
        if not os.path.exists(env_file_path):
            os.makedirs(os.path.dirname(env_file_path), exist_ok=True)
            with open(env_file_path, "w") as file:
                if default_content:
                    file.write(default_content)
                    logging.info(f"Fichier {env_file_path} créé et pré-rempli avec les valeurs par défaut.")
                else:
                    logging.info(f"Fichier {env_file_path} créé.")
        else:
            logging.info(f"Fichier {env_file_path} existe déjà. Aucun contenu par défaut ajouté.")
        
        # Debug : lire le contenu pour vérification
        with open(env_file_path, "r") as file:
            logging.info(f"Contenu actuel du fichier {env_file_path} :\n{file.read()}")

    except Exception as e:
        logging.error(f"Erreur lors de la gestion du fichier {env_file_path} : {e}")
        raise


def append_to_env_file(env_file_path, key, value):
    """
    Ajoute ou met à jour une variable spécifique dans le fichier .env.
    """
    lines = []
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as file:
            lines = file.readlines()
    
    with open(env_file_path, "w") as file:
        key_found = False
        for line in lines:
            if line.startswith(f"{key}="):
                file.write(f"{key}={value}\n")
                key_found = True
            else:
                file.write(line)
        if not key_found:
            file.write(f"{key}={value}\n")
    
    logging.info(f"Variable '{key}' bien ajoutée ou mise à jour dans le fichier {env_file_path}.")

if __name__ == "__main__":
    # Configurez la journalisation
    logging.basicConfig(level=logging.INFO)

    # Chemin vers le fichier .env (dans le dossier parent)
    env_file_path = "./.env"

    # Contenu par défaut à insérer
    default_env_content = """\
    DRIVER="{ODBC Driver 18 for SQL Server};Server=tcp:adventureworks-server-hdf.database.windows.net,1433;Database=adventureworks;Uid=jvcb;Pwd={cbjv592023!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    SERVER="adventureworks-server-hdf"
    DATABASE="adventureworks"
    USERNAME="jvcb"
    PASSWORD="cbjv592023!"
    ACCOUNT_NAME="datalakedeviavals"
    ACCOUNT_KEY="Di7GZT/MHmRkRD6UTcIZmjnuWWhye/yvN2nFUTrgP380meAg4ggupT8RBGmL3IFnoI6aTqam3WFe+AStF56q9g=="
    CONTAINER_NAME="data"
    BLOB_URL="https://datalakedeviavals.blob.core.windows.net/data?sp=rl&st=2024-11-26T08:44:48Z&se=2024-11-26T16:44:48Z&spr=https&sv=2022-11-02&sr=c&sig=PkilXOWO2ExtIo6aOnjGBW3wN9dXvXqtd9mEtzwIXeU%3D"
    JETON_SAS="sp=r&st=2024-11-26T08:44:48Z&se=2024-11-26T16:44:48Z&spr=https&sv=2022-11-02&sr=c&sig=BZ0fret4ak3fuQs2heb%2BrlPjucJkAZ85xE%2BQ%2B2I%2B%2Bb4%3D"
    """

    # Création et pré-remplissage du fichier .env
    write_to_env_file(env_file_path, default_content=default_env_content)