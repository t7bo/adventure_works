import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient, generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta, timezone
import io
from tqdm import tqdm
import pandas as pd
from PIL import Image

# Chargement des variables d'environnement depuis un fichier .env
load_dotenv()

# Configuration du répertoire de logs
log_dir = './logs'
os.makedirs(log_dir, exist_ok=True)

# Configuration de la journalisation
logging.basicConfig(
    filename="./logs/parquet_extraction.log",  # Fichier où les logs seront enregistrés
    level=logging.INFO,  # Niveau minimal pour journaliser (INFO et supérieur)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format des messages de log
)

def generate_sas_token(account_name, account_key, container_name):
    """
    Génère un SAS (Shared Access Signature) pour accéder à un conteneur Azure Blob Storage.

    Arguments:
        account_name (str): Nom du compte de stockage Azure.
        account_key (str): Clé d'accès au compte.
        container_name (str): Nom du conteneur cible.

    Retourne:
        str: Un SAS token permettant l'accès au conteneur.

    Lève:
        Exception: En cas d'erreur lors de la génération du SAS token.
    """
    try:
        # Génère un SAS avec permissions de lecture et de liste, valable 1 heure
        sas_token = generate_container_sas(
            account_name=account_name,
            account_key=account_key,
            container_name=container_name,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        return sas_token
    except Exception as e:
        logging.error(f"Erreur lors de la génération du SAS : {e}")
        raise

def generate_sas_url():
    """
    Génère une URL complète pour accéder à un conteneur Azure avec un SAS.

    Retourne:
        str: URL complète du conteneur avec le SAS token.
    """
    account_name = os.getenv("ACCOUNT_NAME")
    account_key = os.getenv("ACCOUNT_KEY")
    container_name = os.getenv("CONTAINER_NAME")
    sas_token = generate_sas_token(account_name, account_key, container_name)
    return f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"

def list_blobs_with_extension(container_url, file_extension="parquet"):
    """
    Liste tous les blobs (fichiers) ayant une extension donnée dans un conteneur Azure.

    Arguments:
        container_url (str): URL du conteneur avec SAS.
        file_extension (str): Extension des fichiers à rechercher (par défaut : "parquet").

    Retourne:
        list: Liste des noms de blobs correspondant à l'extension.

    Lève:
        Exception: Si une erreur survient lors de la récupération des blobs.
    """
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blobs = container_client.list_blobs()
        # Filtre les blobs pour ne garder que ceux qui terminent par l'extension donnée
        blob_names = [blob.name for blob in blobs if blob.name.endswith(f".{file_extension}")]
        return blob_names
    except Exception as e:
        logging.error(f"Erreur lors de la liste des blobs : {e}")
        raise

def download_parquet_with_sas(container_url, blob_name, download_path):
    """
    Télécharge un fichier .parquet depuis Azure Blob Storage vers un répertoire local.

    Arguments:
        container_url (str): URL du conteneur avec SAS.
        blob_name (str): Nom du blob à télécharger.
        download_path (str): Chemin local pour enregistrer le fichier.

    Lève:
        Exception: En cas d'erreur durant le téléchargement.
    """
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Crée les répertoires nécessaires avant le téléchargement
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        with open(download_path, "wb") as file:
            # Télécharge le contenu du blob
            file.write(blob_client.download_blob().readall())
        
        logging.info(f"Blob téléchargé : {blob_name} -> {download_path}")
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement de {blob_name} : {e}")
        raise

if __name__ == "__main__":
    # Génère l'URL complète pour accéder au conteneur
    full_url = generate_sas_url()

    # Liste tous les blobs .parquet dans le conteneur
    list_blobs = list_blobs_with_extension(full_url)
    logging.info(f"Liste des fichiers Parquet : {list_blobs}")

    # Parcourt chaque blob et télécharge son contenu
    for blob in tqdm(list_blobs, desc="Téléchargement des fichiers", unit="fichier"):
        # Chemin local pour enregistrer le fichier téléchargé
        download_path = f"./data/parquet/downloads/{os.path.basename(blob)}"
        
        # Télécharge le fichier
        download_parquet_with_sas(full_url, blob, download_path)
        
        # Lecture du fichier .parquet téléchargé
        df = pd.read_parquet(download_path, engine="pyarrow")
        
        # Sauvegarde des images contenues dans le DataFrame, si présentes
        try:
            if "image" in df.columns:
                image_dir = './data/parquet/images'
                os.makedirs(image_dir, exist_ok=True)
                for index, row in df.iterrows():
                    # Extraction et sauvegarde des images
                    image_data = row['image']['bytes']
                    image = Image.open(io.BytesIO(image_data))
                    image_name = f"{image_dir}/{row['item_ID']}.png"
                    image.save(image_name, format="PNG")
                    logging.info(f"Image sauvegardée : {image_name}")
        except Exception as e:
            logging.error(f"Erreur lors du traitement des images : {e}")
            raise
        
        # Sauvegarde des données textuelles dans un fichier CSV
        try:
            data_dir = './data/parquet/data'
            os.makedirs(data_dir, exist_ok=True)
            csv_path = f"{data_dir}/{os.path.basename(blob)}.csv"
            df.drop(columns=['image'], errors='ignore').to_csv(csv_path, index=False)
            logging.info(f"Données textuelles sauvegardées : {csv_path}")
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde des données textuelles : {e}")
            raise