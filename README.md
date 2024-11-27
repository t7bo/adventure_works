# ADVENTURE WORKS

## Brief

Vous pouvez consulter le brief du projet [ici](https://zippy-twig-11a.notion.site/Brief-Extraction-de-donn-es-multi-sources-annexe-E1-1451f9041c96805d9e06f5db6bf40fbf).

---

## Exécution des Scripts

Pour exécuter tous les scripts nécessaires à la configuration et au traitement des données, suivez les étapes ci-dessous :

1. **Rendre le script exécutable :**

   Avant d'exécuter le script principal, vous devez rendre le script `install_odbc_driver.sh` exécutable. Utilisez la commande suivante :

   ```bash
   chmod +x install_odbc_driver.sh

2. **Lancer les scripts :**

    ```bash
    bash ./main.sh

---

## Variables d'environnements et secrets

Pour le moment, les variables d'environnements, clés et secrets utiles à ce projet sont présents sur un des scripts afin d'être ajoutées au fichier .env qui sera créé par le même script.

Une gestion plus sécurisée du fichier .env et des variables qu'il contient devra être pensée. A l'heure actuelle, la création du fichier .env et des variables qu'il contient est automatisé via les script **scripts/generate_env_file.py** et **main.sh**.