import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient, generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta, timezone
import io
from tqdm import tqdm
import pandas as pd
from PIL import Image

# Chargement des variables d'environnement
load_dotenv()

# Configuration du répertoire de logs
log_dir = './logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(filename="./logs/parquet_extraction.log", level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_sas_token(account_name, account_key, container_name):
    try:
        # Génération du SAS token
        sas_token = generate_container_sas(
            account_name=account_name,
            account_key=account_key,
            container_name=container_name,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        return sas_token
    except Exception as e:
        logging.error(f"Erreur SAS : {e}")
        raise

def generate_sas_url():
    account_name = os.getenv("ACCOUNT_NAME")
    account_key = os.getenv("ACCOUNT_KEY")
    container_name = os.getenv("CONTAINER_NAME")
    sas_token = generate_sas_token(account_name, account_key, container_name)
    return f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"

# Cherche uniquement les fichiers .parquet dans les différents dossiers du datalake
def list_blobs_with_extension(container_url, file_extension="parquet"):
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blobs = container_client.list_blobs()
        blob_names = [blob.name for blob in blobs if blob.name.endswith(f".{file_extension}")]
        return blob_names
    except Exception as e:
        logging.error(f"Erreur lors de la liste des blobs : {e}")
        raise

def download_parquet_with_sas(container_url, blob_name, download_path):
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blob_client = container_client.get_blob_client(blob_name)
        
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        with open(download_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
        
        logging.info(f"Blob téléchargé : {blob_name} -> {download_path}")
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement de {blob_name}: {e}")
        raise

# Main
if __name__ == "__main__":
    full_url = generate_sas_url()

    list_blobs = list_blobs_with_extension(full_url)
    logging.info(f"Liste des fichiers Parquet : {list_blobs}")

    for blob in tqdm(list_blobs, desc="Téléchargement des fichiers", unit="fichier"):
        # Modifier le chemin pour télécharger dans un seul dossier (downloads)
        download_path = f"./data/parquet/downloads/{os.path.basename(blob)}"  # Utiliser le nom de base du blob comme nom de fichier
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        
        download_parquet_with_sas(full_url, blob, download_path)
        
        # Lecture des données téléchargées
        df = pd.read_parquet(download_path, engine="pyarrow")
        
        # Images --> PNG
        try:
            if "image" in df.columns:
                # Créer le répertoire pour stocker les images téléchargées
                image_dir = './data/parquet/images'
                os.makedirs(image_dir, exist_ok=True)
                # Extraire les données binaires de l'image à partir du dictionnaire
                for index, row in df.iterrows():
                    image_data = row['image']['bytes']  # Extraire la valeur sous la clé 'bytes'
                    image = Image.open(io.BytesIO(image_data))  # Convertir en image avec PIL
                    image_name = f"{image_dir}/{row['item_ID']}.png"

                    image.save(image_name, format="PNG")  # Sauvegarder l'image au format PNG
                    print(f"Image sauvegardée : {image_name}")  # Ajout du print
                    logging.info(f"Image sauvegardée : {image_name}")
        except Exception as e:
            logging.error(f"Erreur lors du traitement des images : {e}")
            raise
   
        # Données textuelles --> CSV
        try:
            data_dir = './data/parquet/data'
            os.makedirs(data_dir, exist_ok=True)
            csv_path = f"{data_dir}/{os.path.basename(blob)}.csv"
            df.drop(columns=['image'], errors='ignore').to_csv(csv_path, index=False)
            logging.info(f"Données textuelles sauvegardées : {csv_path}")
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde des données textuelles : {e}")
            raise
