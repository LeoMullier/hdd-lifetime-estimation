import pandas as pd
import matplotlib.pyplot as plt
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv

def parcourir_liens_recursif(url):
    fichiers = []

    # Envoyer une requête GET pour récupérer le contenu de la page HTML
    r = requests.get(url)

    # Vérifier que la requête a réussi
    if r.status_code == 200:
        # Extraire les liens vers les fichiers à partir du contenu HTML
        soup = BeautifulSoup(r.text, 'html.parser')
        for lien in soup.find_all('a'):
            href = lien.get('href')
            if href.endswith('.txt') or href.endswith('.csv'):
                url_absolu = urljoin(url, href)
                fichiers.append(url_absolu)
            elif len(href) != 3: #juste pour notre cas
                # Si le lien est relatif, calculer son URL absolue
                url_absolu = urljoin(url, href)
                fichiers.extend(parcourir_liens_recursif(url_absolu))

    return fichiers

def tracer_graph( x,  y, fichier ):
    # Charger le fichier CSV
    df = pd.read_csv("fichier", sep="\t")

    # Créer un graphique à partir des données
    plt.plot(df[x], df[y])

    # Définir les labels des axes x et y
    plt.xlabel(x)
    plt.ylabel(y)

    # Afficher le graphique
    plt.show()


def parcourir_repertoire(chemin):
    fichiers = []

    # Parcourir le répertoire et stocker tous les noms de fichiers dans le tableau
    for nom_fichier in os.listdir(chemin):
        chemin_fichier = os.path.join(chemin, nom_fichier)
        if os.path.isfile(chemin_fichier):
            fichiers.append(chemin_fichier)
        elif os.path.isdir(chemin_fichier):
            fichiers.extend(parcourir_repertoire(chemin_fichier))

    return fichiers



def ecrire_tableau_dans_csv(tableau, nom_fichier):
    with open(nom_fichier, mode='w', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        for ligne in tableau:
            writer.writerow(ligne)

'''
nom_fichier = "https://data.nicolasparmentier.fr/UTC/SR09/backblaze-per-failed-disk/"
fichier_chemin = parcourir_liens_recursif(nom_fichier)
ecrire_tableau_dans_csv(fichier_chemin, "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\donnee.csv")
'''