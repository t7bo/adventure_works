# Se positionner dans le répertoire du script
cd "$(dirname "$0")"

# Vérifier si .env existe
# if [[ ! -f ".env" ]]; then
#     echo "Le fichier .env est manquant ! Merci de renseigner les variables d'environnement dans un fichier .env"
#     exit 1
# fi

# Installation du Driver ODBC
echo 'INSTALLATION DU DRIVER ODBC'

if ! [[ "18.04 20.04 22.04 23.04 24.04" == *"$(lsb_release -rs)"* ]];
then
    echo "Ubuntu $(lsb_release -rs) is not currently supported.";
    exit;
fi

# Add the signature to trust the Microsoft repo
# For Ubuntu versions < 24.04 
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
# For Ubuntu versions >= 24.04
curl https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Add repo to apt sources
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Install the driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
# optional: for unixODBC development headers
sudo apt-get install -y unixodbc-dev

# Création d'un VENV
echo "CREATION D'UN VIRTUAL ENVIRONMENT"
python3 -m venv venv
source venv/bin/activate

# Installation des librairies
echo 'INSTALLATION DES PACKAGES/LIBRAIRIES PYTHON'
python3 -m pip install --upgrade pip
pip install -r requirements.txt

# Création d'un fichier .env et des variables d'environnement
echo "CREATION DU FICHIER .env AVEC LES VARIABLES D'ENVIRONNEMENT ET GENERATION D'UN SAS TOKEN"
python3 scripts/generate_env_file.py

# Lancement des scripts d'extraction de données
echo "LANCEMENT DES SCRIPTS D'EXTRACTION DE DONNEES"
python3 scripts/extract_sql.py
python3 scripts/extract_parquet.py
python3 scripts/extract_sql.py