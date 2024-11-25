#!/bin/bash

# Script pour installer automatiquement le pilote ODBC Microsoft pour SQL Server

# Mise à jour des paquets
echo "Mise à jour des paquets..."
sudo apt-get update -y

# Ajoute la clé de Microsoft pour les paquets
echo "Ajout de la clé Microsoft..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

# Ajoute le dépôt Microsoft pour la version d'Ubuntu
echo "Ajout du dépôt Microsoft pour Ubuntu..."
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Met à jour les sources des paquets
echo "Mise à jour des sources des paquets..."
sudo apt-get update -y

# Accepte la licence et installe le pilote ODBC pour SQL Server
echo "Installation du pilote ODBC Microsoft pour SQL Server..."
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Vérifie que le pilote est installé
echo "Vérification de l'installation du pilote ODBC..."
odbcinst -q -d

# Installation réussie
echo "Le pilote ODBC Microsoft pour SQL Server a été installé avec succès."