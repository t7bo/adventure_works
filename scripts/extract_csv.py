import os
import logging
from azure.storage.blob import ContainerClient, generate_container_sas, ContainerSasPermissions
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import zipfile
import io
import pandas as pd

# Charger les variables d'environnement
load_dotenv()

# Configuration des logs
logging.basicConfig(filename="logs/extract_csv.log", level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

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

    # Vérification des variables d'environnement
    if not all([account_name, account_key, container_name]):
        raise ValueError("Les variables d'environnement ACCOUNT_NAME, ACCOUNT_KEY ou CONTAINER_NAME ne sont pas définies.")

    sas_token = generate_sas_token(account_name, account_key, container_name)
    return f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"

def list_files_from_specific_folders(container_url, folders, extensions):
    """
    Liste les fichiers avec des extensions spécifiques dans des dossiers spécifiques d'un conteneur Azure Blob Storage.
    """
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blobs = container_client.list_blobs()
        files = {folder: [] for folder in folders}  # Dictionnaire pour stocker les fichiers par dossier

        for blob in blobs:
            for folder in folders:
                if blob.name.startswith(folder + "/") and any(blob.name.endswith(ext) for ext in extensions):
                    files[folder].append(blob.name)  # Ajoute au dossier correspondant
        return files
    except Exception as e:
        logging.error(f"Erreur lors de la liste des fichiers : {e}")
        raise

def download_file(container_url, blob_name, download_dir):
    """
    Télécharge un fichier depuis Azure Blob Storage vers un répertoire local.
    """
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blob_client = container_client.get_blob_client(blob_name)

        # Chemin complet pour sauvegarder le fichier
        file_path = os.path.join(download_dir, os.path.basename(blob_name))
        os.makedirs(download_dir, exist_ok=True)  # Crée le répertoire si nécessaire

        with open(file_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

        logging.info(f"Fichier téléchargé : {blob_name} -> {file_path}")
        print(f"Fichier téléchargé : {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement de {blob_name}: {e}")
        raise

def unzip_and_process_zip(zip_path, target_folder):
    """
    Décompresse un fichier .zip et traite les fichiers .csv à l'intérieur.
    """
    try:
        # Ouvrir le fichier zip
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(target_folder)  # Extraire tous les fichiers dans le dossier cible

            # Liste des fichiers extraits
            extracted_files = zip_ref.namelist()
            for file_name in extracted_files:
                if file_name.endswith('.csv'):
                    # Lire et traiter les fichiers CSV extraits
                    csv_file_path = os.path.join(target_folder, file_name)
                    df = pd.read_csv(csv_file_path)
                    csv_output_path = os.path.join(target_folder, file_name)

                    # Sauvegarder ou manipuler le fichier CSV comme vous le souhaitez
                    df.to_csv(csv_output_path, index=False)
                    logging.info(f"CSV extrait et sauvegardé : {csv_output_path}")
                    print(f"CSV extrait et sauvegardé : {csv_output_path}")
                else:
                    logging.info(f"Fichier ignoré (non CSV) : {file_name}")
    except Exception as e:
        logging.error(f"Erreur lors de la décompression du fichier ZIP : {e}")
        raise

if __name__ == "__main__":
    try:
        # URL du conteneur Azure Blob
        container_url = generate_sas_url()

        # Dossiers spécifiques à examiner
        target_folders = ["machine_learning", "nlp_data"]

        # Extensions des fichiers à rechercher
        extensions = [".csv", ".zip"]

        print(f"Recherche des fichiers {extensions} dans les dossiers : {target_folders}...")

        # Liste des fichiers par dossier et extension
        files_by_folder = list_files_from_specific_folders(container_url, target_folders, extensions)

        # Téléchargement des fichiers pour chaque dossier
        for folder, files in files_by_folder.items():
            print(f"Traitement des fichiers dans le dossier : {folder}")

            # Répertoire de destination pour le téléchargement
            download_dir = os.path.join("data", folder)
            
            for file in tqdm(files, desc=f"Téléchargement des fichiers {folder}"):
                downloaded_file_path = download_file(container_url, file, download_dir)

                # Si le fichier téléchargé est un .zip, le décompresser et traiter les fichiers CSV à l'intérieur
                if downloaded_file_path.endswith(".zip"):
                    unzip_and_process_zip(downloaded_file_path, download_dir)

        print("Tous les fichiers ont été téléchargés et traités.")
    except Exception as e:
        print(f"Erreur : {e}")
