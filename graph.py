import pandas as pd
import matplotlib.pyplot as plt
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import io
import numpy as np

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


def tracer_les_graphs(y,fichier):
    dos = os.path.dirname(fichier[0])
    for x in fichier:
        print(dos)
        if dos != os.path.dirname(x):
            dos = os.path.dirname(x)
            plt.show(block=False)
            plt.figure()
            
         # Charger le fichier CSV
        df = pd.read_csv(x, sep="\t")
        df[y] = df[y].apply(lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x)
        # Créer un graphique à partir des données

        plt.plot(df['date'], df[y])
        plt.title(y)
        print(y)
    

nb_valeurs_par_date = {}

def remplir_dico_moyenne(fichier, liste):
    dico = {}
    nb_fichiers = len(fichier)
    
    
    '''
    # extraire la date maximale de tous les fichiers
    max_date = pd.Timestamp.min
    for f in fichier:
        df = pd.read_csv(f, sep="\t")
        max_date = max(max_date, pd.to_datetime(df['date'].max()))
    print(max_date)

    max_date = pd.to_datetime('2022-12-30 00:00:00')

    '''
    liste_valeurs ={}
    for f in fichier:
        df = pd.read_csv(f, sep="\t")
        df = df.fillna(value=0)
        
        for l in liste:
            df[l] = df[l].apply(lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x)
            if l not in dico:
                dico[l] = {}
                nb_valeurs_par_date[l] = {}

            if (l not in liste_valeurs): 
                liste_valeurs[l] = {}
            
            '''            
            # ajouter les dates manquantes avec une valeur de 0
            df = df.set_index('date')
            df = df.reindex(pd.date_range(start=df.index.min(), end=max_date, freq='D'), fill_value=0)
            df = df.reset_index().rename(columns={'index': 'date'})'''

            for i in range(len(df)):
                date = df.loc[i, 'trace']
                valeur = df.loc[i, l]

                if(date not in liste_valeurs[l]):
                    liste_valeurs[l][date] = []


                if isinstance(valeur, float) and valeur!=0 :
                    liste_valeurs[l][date].append(valeur)
                    if date not in dico[l]:
                        dico[l][date] = valeur
                        nb_valeurs_par_date[l][date] = 1
                    else:
                        dico[l][date] += valeur
                        nb_valeurs_par_date[l][date] += 1
#print(df)               
        #print(dico)

    for l in dico:

        for date in dico[l]:
            moyenne = dico[l][date]/ nb_valeurs_par_date[l][date]
            erreur = np.nanstd(liste_valeurs[l][date])/np.sqrt(nb_valeurs_par_date[l][date])
            dico[l][date] = (moyenne, erreur)


    return dico


def ajouter_colonne_trace(fichier):
    for f in fichier:
        print(f)
        df = pd.read_csv(f, sep="\t")
        trace = pd.Series(range(0, -1, -1))
        df['trace'] = trace[:len(df)]
        df.to_csv(f, sep="\t", index=False, columns=list(df.columns) + ['trace'])



def tracer_graph( x,  y, fichier ):
    
    # Charger le fichier CSV
    df = pd.read_csv(fichier, sep="\t")

    # Créer un graphique à partir des données
    plt.plot(df[x], df[y])

    print(df[y])

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

def tableau_csv(chemin_csv):
    with open(chemin_csv, newline='') as f:
        lecteur_csv = csv.reader(f)
        tableau = [element for ligne in lecteur_csv for element in ligne]
    return tableau

def tracer_dico(dico):
    for col, valeurs in dico.items():
        x = []
        y = []
        yerr=[]
        test=[]
        for date, (valeur,erreur) in valeurs.items():
            x.append(date)
            y.append(valeur)
            yerr.append(erreur)
        plt.figure()
        plt.errorbar(x, y, yerr=yerr, fmt='-', label=col, ecolor='r', capsize=3)
        plt.xlabel('Date')
        plt.ylabel('Valeur')
        plt.legend()
        plt.show(block=False)
        
    


#om_fichier = "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\results"
nom_fichier = "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\Fichier_csvV.3"

fichier_chemin = parcourir_repertoire(nom_fichier)


#print(fichier_chemin)


'''
ecrire_tableau_dans_csv(fichier_chemin, "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\donnee.csv")
tab = tableau_csv("C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\donnee.csv")


for x in fichier_chemin:
    tracer_graph('date','smart_222_raw', "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\results\\2021-12-29\\Z9D0A001FVKG_90.csv")



for x in fichier_chemin:
    df = pd.read_csv(x, sep="\t")
    for y in df.columns:
        if(y not in liste):
            liste.append(y)

'''
liste=[]
autre =['Unnamed: 0', 'serial_number', 'model', 'capacity_bytes','date']
df = pd.read_csv(fichier_chemin[0], sep="\t")
for y in df.columns:
    if y not in liste and not y.endswith('normalized') and y not in autre:
        liste.append(y)

#iste=['smart_1_raw','smart_5_raw','smart_188_raw','smart_10_raw','smart_187_raw','smart_190_raw','smart_196_raw','smart_197_raw','smart_198_raw','smart_201_raw','smart_220_raw']


#liste=['smart_5_raw']
'''
for col in liste:
        tracer_les_graphs(col,fichier_chemi
''' 


ajouter_colonne_trace(fichier_chemin)
dictio = remplir_dico_moyenne(fichier_chemin,liste)

tracer_dico(dictio)
